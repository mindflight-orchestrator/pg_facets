const std = @import("std");
const utils = @import("../utils.zig");
const c = utils.c;
const PgAllocator = utils.PgAllocator;
const tokenizer_native = @import("tokenizer_native.zig");
const scoring = @import("scoring.zig");
const roaring_index = @import("roaring_index.zig");

/// Search options
pub const SearchOptions = struct {
    prefix_match: bool = false,
    fuzzy_match: bool = false,
    fuzzy_threshold: f64 = 0.3,
    k1: f64 = 1.2,
    b: f64 = 0.75,
};

/// Search result
pub const SearchResult = struct {
    doc_id: i64,
    score: f64,
};

/// Term frequency entry for batch loading
const TermFreqEntry = struct {
    term_hash: i64,
    doc_id: i64,
    freq: i32,
};

/// Document length entry for batch loading
const DocLengthEntry = struct {
    doc_id: i64,
    length: i32,
};

/// Native BM25 search with batch queries - much faster than per-document queries
/// Uses single SPI connection for all queries to avoid nesting issues
pub fn searchNative(
    table_id: c.Oid,
    query_text: []const u8,
    config_name: []const u8,
    options: SearchOptions,
    limit: i32,
    allocator: std.mem.Allocator,
) !std.ArrayList(SearchResult) {
    utils.elogFmt(c.NOTICE, "[TRACE] searchNative: Starting for table_id={d}, query='{s}'", .{table_id, query_text});
    
    // All database operations in single SPI connection
    // Connect FIRST before tokenization to avoid nested SPI connections
    const conn_result = c.SPI_connect();
    const need_finish = (conn_result == c.SPI_OK_CONNECT);
    if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
        utils.elogWithContext(c.ERROR, "searchNative", "SPI_connect failed");
        return error.SPIConnectFailed;
    }
    defer if (need_finish) {
        _ = c.SPI_finish();
    };
    
    // Phase 1: Tokenize query using native tokenizer
    // Use helper that assumes SPI is already connected to avoid nested connections
    const query_tokens = try tokenizer_native.tokenizeNativeWithExistingConnection(query_text, config_name, allocator);
    utils.elogFmt(c.NOTICE, "[TRACE] searchNative: Tokenized into {d} tokens", .{query_tokens.items.len});

    if (query_tokens.items.len == 0) {
        utils.elogFmt(c.NOTICE, "[TRACE] searchNative: No tokens, returning empty", .{});
        return std.ArrayList(SearchResult).empty;
    }

    // Convert tokens to hashes
    var query_hashes = std.ArrayList(i64).empty;
    for (query_tokens.items) |token| {
        const hash = tokenizer_native.hashLexeme(token.lexeme);
        utils.elogFmt(c.NOTICE, "[TRACE] searchNative: Token '{s}' -> hash {d}", .{token.lexeme, hash});
        try query_hashes.append(allocator, hash);
    }

    if (query_hashes.items.len == 0) {
        return std.ArrayList(SearchResult).empty;
    }

    // Phase 2: Get statistics
    var stats_query_buf: [256]u8 = undefined;
    const stats_query = std.fmt.bufPrintZ(&stats_query_buf, 
        "SELECT total_documents, avg_document_length FROM facets.bm25_statistics WHERE table_id = {d}",
        .{table_id}) catch return error.BufferTooSmall;
    
    var total_docs: i64 = 0;
    var avgdl: f64 = 1.0;
    
    var ret = c.SPI_execute(stats_query.ptr, true, 1);
    if (ret == c.SPI_OK_SELECT and c.SPI_processed > 0) {
        var isnull1: bool = false;
        var isnull2: bool = false;
        const tuple = c.SPI_tuptable.*.vals[0];
        const tupdesc = c.SPI_tuptable.*.tupdesc;
        const td_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull1);
        const ad_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull2);
        if (!isnull1) total_docs = c.DatumGetInt64(td_datum);
        if (!isnull2) avgdl = c.DatumGetFloat8(ad_datum);
    }

    utils.elogFmt(c.NOTICE, "[TRACE] searchNative: total_docs={d}, avgdl={d}", .{total_docs, @as(i64, @intFromFloat(avgdl))});

    if (total_docs == 0) {
        utils.elogFmt(c.NOTICE, "[TRACE] searchNative: No docs, returning empty", .{});
        return std.ArrayList(SearchResult).empty;
    }

    // Phase 3: Get doc frequencies for IDF calculation
    var doc_freqs = std.AutoHashMap(i64, i64).init(allocator);
    defer doc_freqs.deinit();

    // Build term_hash array string
    var hash_arr = std.ArrayList(u8).empty;
    defer hash_arr.deinit(allocator);
    try hash_arr.appendSlice(allocator, "ARRAY[");
    for (query_hashes.items, 0..) |hash, i| {
        if (i > 0) try hash_arr.appendSlice(allocator, ",");
        const hash_str = try std.fmt.allocPrint(allocator, "{d}", .{hash});
        defer allocator.free(hash_str);
        try hash_arr.appendSlice(allocator, hash_str);
    }
    try hash_arr.appendSlice(allocator, "]::bigint[]");

    const df_query = try std.fmt.allocPrintSentinel(allocator,
        "SELECT term_hash, rb_cardinality(doc_ids)::bigint FROM facets.bm25_index WHERE table_id = {d} AND term_hash = ANY({s})",
        .{table_id, hash_arr.items}, 0);
    defer allocator.free(df_query);

    ret = c.SPI_execute(df_query.ptr, true, 0);
    if (ret == c.SPI_OK_SELECT and c.SPI_processed > 0) {
        var i: u64 = 0;
        while (i < c.SPI_processed) : (i += 1) {
            const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
            const tupdesc = c.SPI_tuptable.*.tupdesc;
            var isnull1: bool = false;
            var isnull2: bool = false;
            const h_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull1);
            const df_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull2);
            if (!isnull1 and !isnull2) {
                const term_hash: i64 = c.DatumGetInt64(h_datum);
                const df: i64 = c.DatumGetInt64(df_datum);
                try doc_freqs.put(term_hash, df);
            }
        }
    }

    // Phase 4: Get all term frequencies and doc lengths in one query
    const tf_query = try std.fmt.allocPrintSentinel(allocator,
        \\SELECT i.term_hash, d.doc_id, d.doc_length, (i.term_freqs->>d.doc_id::text)::int as freq
        \\FROM facets.bm25_index i
        \\CROSS JOIN facets.bm25_documents d
        \\WHERE i.table_id = {d} AND d.table_id = {d}
        \\  AND i.term_hash = ANY({s})
        \\  AND i.term_freqs ? d.doc_id::text
        , .{table_id, table_id, hash_arr.items}, 0);
    defer allocator.free(tf_query);

    var results = std.ArrayList(SearchResult).empty;
    
    // Map to accumulate scores per doc
    var doc_scores = std.AutoHashMap(i64, f64).init(allocator);
    defer doc_scores.deinit();
    var doc_lengths_map = std.AutoHashMap(i64, i32).init(allocator);
    defer doc_lengths_map.deinit();

    utils.elogFmt(c.NOTICE, "[TRACE] searchNative: Executing tf_query", .{});
    ret = c.SPI_execute(tf_query.ptr, true, 0);
    utils.elogFmt(c.NOTICE, "[TRACE] searchNative: tf_query ret={d}, processed={d}", .{ret, c.SPI_processed});
    if (ret == c.SPI_OK_SELECT and c.SPI_processed > 0) {
        var i: u64 = 0;
        while (i < c.SPI_processed) : (i += 1) {
            const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
            const tupdesc = c.SPI_tuptable.*.tupdesc;
            var isnull1: bool = false;
            var isnull2: bool = false;
            var isnull3: bool = false;
            var isnull4: bool = false;

            const h_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull1);
            const d_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull2);
            const l_datum = c.SPI_getbinval(tuple, tupdesc, 3, &isnull3);
            const f_datum = c.SPI_getbinval(tuple, tupdesc, 4, &isnull4);

            if (isnull1 or isnull2 or isnull3 or isnull4) continue;

            const term_hash: i64 = c.DatumGetInt64(h_datum);
            const doc_id: i64 = c.DatumGetInt64(d_datum);
            const doc_length: i32 = c.DatumGetInt32(l_datum);
            const tf: i32 = c.DatumGetInt32(f_datum);

            if (tf <= 0) continue;

            // Store doc length
            try doc_lengths_map.put(doc_id, doc_length);

            utils.elogFmt(c.NOTICE, "[TRACE] searchNative: tf row - hash={d}, doc_id={d}, tf={d}", .{term_hash, doc_id, tf});

            // Calculate IDF
            const n_qi = doc_freqs.get(term_hash) orelse 0;
            if (n_qi == 0) {
                utils.elogFmt(c.NOTICE, "[TRACE] searchNative: SKIPPING - no doc_freq for hash={d}", .{term_hash});
                continue;
            }
            utils.elogFmt(c.NOTICE, "[TRACE] searchNative: doc_freq n_qi={d}", .{n_qi});
            
            const N = @as(f64, @floatFromInt(total_docs));
            const n = @as(f64, @floatFromInt(n_qi));
            const idf = @log((N + 1.0) / (n + 0.5));

            // Calculate BM25 component
            const tf_f = @as(f64, @floatFromInt(tf));
            const doc_len_f = @as(f64, @floatFromInt(doc_length));
            const numerator = tf_f * (options.k1 + 1.0);
            const denominator = tf_f + options.k1 * (1.0 - options.b + options.b * (doc_len_f / avgdl));
            const score_component = idf * (numerator / denominator);

            // Accumulate score
            const prev_score = doc_scores.get(doc_id) orelse 0.0;
            try doc_scores.put(doc_id, prev_score + score_component);
        }
    }

    // Convert to results array
    var doc_iter = doc_scores.iterator();
    while (doc_iter.next()) |entry| {
        if (entry.value_ptr.* > 0.0) {
            try results.append(allocator, SearchResult{
                .doc_id = entry.key_ptr.*,
                .score = entry.value_ptr.*,
            });
        }
    }

    // Sort by score descending
    std.mem.sort(SearchResult, results.items, {}, struct {
        fn lessThan(_: void, a: SearchResult, b: SearchResult) bool {
            return a.score > b.score;
        }
    }.lessThan);

    // Limit results
    utils.elogFmt(c.NOTICE, "[TRACE] searchNative: query='{s}', Final results count={d}", .{query_text, results.items.len});
    if (results.items.len > @as(usize, @intCast(limit))) {
        results.shrinkRetainingCapacity(@as(usize, @intCast(limit)));
    }

    return results;
}

/// Get document set bitmap for a term hash
fn getDocumentSetByHash(
    table_id: c.Oid,
    term_hash: i64,
    allocator: std.mem.Allocator,
) !?*c.roaring_bitmap_t {
    const query = try std.fmt.allocPrintSentinel(
        allocator,
        "SELECT doc_ids FROM facets.bm25_index WHERE table_id = {d} AND term_hash = {d}",
        .{ table_id, term_hash },
    );
    defer allocator.free(query);

    const ret = c.SPI_execute(query.ptr, true, 1);
    if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0) {
        return null;
    }

    const tuple = c.SPI_tuptable.*.vals[0];
    const tupdesc = c.SPI_tuptable.*.tupdesc;

    var isnull: bool = false;
    const doc_ids_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);

    if (isnull) {
        return null;
    }

    return try roaring_index.datumToRoaringBitmap(doc_ids_datum);
}

/// SQL-callable native search function
pub fn bm25_search_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = PgAllocator.allocator();

    // Parse arguments
    // p_table_id oid
    if (utils.is_arg_null(fcinfo, 0)) {
        utils.elogWithContext(c.ERROR, "bm25_search_native", "p_table_id cannot be null");
        return c.PointerGetDatum(null);
    }
    const table_id: c.Oid = @intCast(utils.get_arg_datum(fcinfo, 0));

    // p_query text
    if (utils.is_arg_null(fcinfo, 1)) {
        utils.elogWithContext(c.ERROR, "bm25_search_native", "p_query cannot be null");
        return c.PointerGetDatum(null);
    }
    const query_datum = utils.get_arg_datum(fcinfo, 1);
    const query_cstr = utils.textToCstring(query_datum);
    if (query_cstr == null) {
        utils.elogWithContext(c.ERROR, "bm25_search_native", "Failed to extract query");
        return c.PointerGetDatum(null);
    }
    defer c.pfree(@ptrCast(query_cstr));
    // Use bounded strlen with max check to prevent reading corrupted memory
    var query_len: usize = 0;
    const max_query_len: usize = 1048576; // Max 1MB
    while (query_len < max_query_len and query_cstr[query_len] != 0) {
        query_len += 1;
    }
    if (query_len >= max_query_len) {
        utils.elogFmt(c.ERROR,
            "bm25_search_native: Query text too long or not null-terminated (length >= {d} bytes, max {d} bytes). Please reduce query size or split into multiple queries.",
            .{ query_len, max_query_len });
        return c.PointerGetDatum(null);
    }
    const query_text = query_cstr[0..query_len];

    // p_language text (default 'english')
    var language: []const u8 = "english";
    var language_alloc: ?[]u8 = null;
    defer if (language_alloc) |la| allocator.free(la);
    if (!utils.is_arg_null(fcinfo, 2)) {
        const lang_datum = utils.get_arg_datum(fcinfo, 2);
        const lang_cstr = utils.textToCstring(lang_datum);
        if (lang_cstr != null) {
            defer c.pfree(@ptrCast(lang_cstr));
            // Use bounded strlen with max check (language names should be < 64 bytes)
            var lang_len: usize = 0;
            const max_lang_len: usize = 64;
            while (lang_len < max_lang_len and lang_cstr[lang_len] != 0) {
                lang_len += 1;
            }
            if (lang_len >= max_lang_len) {
                utils.elogFmt(c.WARNING, 
                    "bm25_search_native: Language string too long or not null-terminated (length >= {d} bytes, max {d} bytes). Using default 'english'. Language names should be short (e.g., 'english', 'spanish').", 
                    .{ lang_len, max_lang_len });
                // Use default 'english' instead of returning error
                language = "english";
            } else {
                const lang_copy = allocator.alloc(u8, lang_len) catch {
                    utils.elogWithContext(c.ERROR, "bm25_search_native", "Failed to allocate language buffer");
                    return c.PointerGetDatum(null);
                };
                @memcpy(lang_copy, lang_cstr[0..lang_len]);
                language = lang_copy;
                language_alloc = lang_copy;
            }
        }
    }

    // p_prefix_match boolean (default false) - arg 3
    // Currently not used in searchNative, but we need to skip it
    _ = utils.is_arg_null(fcinfo, 3); // prefix_match
    
    // p_fuzzy_match boolean (default false) - arg 4
    // Currently not used in searchNative, but we need to skip it
    _ = utils.is_arg_null(fcinfo, 4); // fuzzy_match
    
    // p_fuzzy_threshold float (default 0.3) - arg 5
    // Currently not used in searchNative, but we need to skip it
    _ = utils.is_arg_null(fcinfo, 5); // fuzzy_threshold

    // p_k1 float (default 1.2) - arg 6
    var k1: f64 = 1.2;
    if (!utils.is_arg_null(fcinfo, 6)) {
        k1 = c.DatumGetFloat8(utils.get_arg_datum(fcinfo, 6));
    }

    // p_b float (default 0.75) - arg 7
    var b: f64 = 0.75;
    if (!utils.is_arg_null(fcinfo, 7)) {
        b = c.DatumGetFloat8(utils.get_arg_datum(fcinfo, 7));
    }

    // p_limit int (default 10) - arg 8
    var limit: i32 = 10;
    if (!utils.is_arg_null(fcinfo, 8)) {
        limit = @intCast(c.DatumGetInt32(utils.get_arg_datum(fcinfo, 8)));
    }

    const options = SearchOptions{
        .k1 = k1,
        .b = b,
    };

    // Perform search
    utils.elogFmt(c.NOTICE, "[TRACE] bm25_search_native: calling searchNative for query='{s}'", .{query_text});
    const results = searchNative(table_id, query_text, language, options, limit, allocator) catch {
        utils.elogWithContext(c.ERROR, "bm25_search_native", "Native search failed");
        return c.PointerGetDatum(null);
    };
    utils.elogFmt(c.NOTICE, "[TRACE] bm25_search_native: searchNative returned {d} results for query='{s}'", .{results.items.len, query_text});

    // Set up ReturnSetInfo
    const rsi_ptr = @as(?*c.ReturnSetInfo, @alignCast(@ptrCast(fcinfo.*.resultinfo)));
    if (rsi_ptr == null) {
        utils.elogWithContext(c.ERROR, "bm25_search_native", "set-valued function called in context that cannot accept a set");
        return 0;
    }
    const rsi = rsi_ptr.?;

    if ((rsi.allowedModes & c.SFRM_Materialize) == 0) {
        utils.elogWithContext(c.ERROR, "bm25_search_native", "SRF materialize mode not allowed");
        return 0;
    }
    rsi.returnMode = c.SFRM_Materialize;

    const oldcontext = c.MemoryContextSwitchTo(rsi.econtext.?.*.ecxt_per_query_memory);
    rsi.setResult = c.tuplestore_begin_heap(true, false, utils.workMem());

    // Create tuple descriptor: (doc_id bigint, score float8)
    const tupdesc = c.CreateTemplateTupleDesc(2);
    _ = c.TupleDescInitEntry(tupdesc, 1, "doc_id", c.INT8OID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 2, "score", c.FLOAT8OID, -1, 0);
    rsi.setDesc = tupdesc;

    // Store results
    utils.elogFmt(c.NOTICE, "[TRACE] bm25_search_native: Storing {d} results in tuplestore", .{results.items.len});
    for (results.items) |result| {
        utils.elogFmt(c.NOTICE, "[TRACE] bm25_search_native: Storing doc_id={d}, score={d}", .{result.doc_id, @as(i64, @intFromFloat(result.score * 1000))});
        var values = [_]c.Datum{
            c.Int64GetDatum(result.doc_id),
            c.Float8GetDatum(result.score),
        };
        var nulls = [_]bool{ false, false };
        const tuple = c.heap_form_tuple(tupdesc, &values, &nulls);
        c.tuplestore_puttuple(rsi.setResult, tuple);
    }

    _ = c.MemoryContextSwitchTo(oldcontext);
    utils.elogFmt(c.NOTICE, "[TRACE] bm25_search_native: Returning", .{});

    return 0;
}

