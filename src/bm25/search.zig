const std = @import("std");
const utils = @import("../utils.zig");
const c = utils.c;
const tokenizer = @import("tokenizer.zig");
const tokenizer_pure = @import("tokenizer_pure.zig");
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


/// Tokenize query text into individual terms
fn tokenizeQuery(
    query_text: []const u8,
    config_name: []const u8,
    allocator: std.mem.Allocator
) !std.ArrayList([]const u8) {
    const query_tokens = try tokenizer.tokenize(query_text, config_name, allocator);
    if (query_tokens.items.len == 0) {
        query_tokens.deinit(allocator);
        return std.ArrayList([]const u8).empty;
    }
    return query_tokens;
}

/// Expand query terms to term hashes and combine document sets
fn expandQueryTerms(
    table_id: c.Oid,
    query_tokens: std.ArrayList([]const u8),
    options: SearchOptions,
    allocator: std.mem.Allocator
) !?*c.roaring_bitmap_t {
    const MAX_QUERY_HASHES = 64;
    var expanded_hashes_arr: [MAX_QUERY_HASHES]i64 = undefined;
    var expanded_hashes_count: usize = 0;

    var combined_bitmap: ?*c.roaring_bitmap_t = null;

    // Try to connect - may already be connected from caller
    const conn_result = c.SPI_connect();
    const need_finish = (conn_result == c.SPI_OK_CONNECT);
    if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed unexpectedly");
        return error.SPIConnectFailed;
    }
    defer if (need_finish) {
        _ = c.SPI_finish();
    };

    // Expand query terms to hashes (prefix/fuzzy matching)
    for (query_tokens.items) |query_term| {
        var matching_hashes = try findMatchingTermHashesInternal(table_id, query_term, options, allocator);
        defer matching_hashes.deinit(allocator);

        for (matching_hashes.items) |hash| {
            if (expanded_hashes_count < MAX_QUERY_HASHES) {
                expanded_hashes_arr[expanded_hashes_count] = hash;
                expanded_hashes_count += 1;
            }
        }
    }

    if (expanded_hashes_count == 0) {
        return null;
    }

    // Get document sets for all term hashes and combine (OR operation)
    for (expanded_hashes_arr[0..expanded_hashes_count]) |term_hash| {
        const doc_set = try getDocumentSetByHashInternal(table_id, term_hash, allocator);
        defer if (doc_set) |ds| roaring_index.free(ds);

        if (doc_set) |ds| {
            if (combined_bitmap) |combined| {
                roaring_index.orInPlace(combined, ds);
            } else {
                combined_bitmap = roaring_index.copy(ds);
            }
        }
    }

    return combined_bitmap;
}

/// Calculate BM25 scores for documents matching the query
fn calculateScores(
    table_id: c.Oid,
    combined_bitmap: *c.roaring_bitmap_t,
    expanded_hashes: []i64,
    stats: *const scoring.CollectionStats,
    options: SearchOptions,
    allocator: std.mem.Allocator
) !std.ArrayList(SearchResult) {
    var results = std.ArrayList(SearchResult).empty;

    // Collect doc_ids first
    var doc_ids = std.ArrayList(i64).empty;
    defer doc_ids.deinit(allocator);

    var iter = roaring_index.Iterator.init(combined_bitmap);
    while (iter.hasValue()) {
        try doc_ids.append(allocator, @intCast(iter.currentValue()));
        iter.advance();
    }

    // Try to connect - may already be connected from caller
    const conn_result = c.SPI_connect();
    const need_finish = (conn_result == c.SPI_OK_CONNECT);
    if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed unexpectedly");
        return error.SPIConnectFailed;
    }
    defer if (need_finish) {
        _ = c.SPI_finish();
    };

    for (doc_ids.items) |doc_id| {
        const score = try calculateDocumentScore(table_id, doc_id, expanded_hashes, stats, options);
        if (score > 0.0) {
            try results.append(allocator, SearchResult{
                .doc_id = doc_id,
                .score = score,
            });
        }
    }

    return results;
}

/// Calculate BM25 score for a single document
fn calculateDocumentScore(
    table_id: c.Oid,
    doc_id: i64,
    expanded_hashes: []i64,
    stats: *const scoring.CollectionStats,
    options: SearchOptions
) !f64 {
    // Use stack-allocated arrays for term frequencies
    const MAX_TERMS = 64;
    var term_freq_hashes: [MAX_TERMS]i64 = undefined;
    var term_freq_values: [MAX_TERMS]i32 = undefined;
    var term_freq_count: usize = 0;

    for (expanded_hashes) |term_hash| {
        if (term_freq_count >= MAX_TERMS) break;

        // Query term frequency using stack-allocated buffer
        var query_buf: [256]u8 = undefined;
        const query = std.fmt.bufPrintZ(&query_buf, "SELECT (term_freqs->>'{d}')::int FROM facets.bm25_index WHERE table_id = {d} AND term_hash = {d}", .{ doc_id, table_id, term_hash }) catch continue;

        const ret = c.SPI_execute(query.ptr, true, 1);

        if (ret == c.SPI_OK_SELECT and c.SPI_processed > 0 and c.SPI_tuptable != null) {
            const tuple = c.SPI_tuptable.*.vals[0];
            const tupdesc = c.SPI_tuptable.*.tupdesc;
            var isnull: bool = false;
            const freq_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);

            if (!isnull) {
                const freq: i32 = @intCast(c.DatumGetInt32(freq_datum));
                if (freq > 0) {
                    term_freq_hashes[term_freq_count] = term_hash;
                    term_freq_values[term_freq_count] = freq;
                    term_freq_count += 1;
                }
            }
        }
    }

    if (term_freq_count == 0) {
        return 0.0;
    }

    // Get document length using stack buffer
    var doc_len_query_buf: [256]u8 = undefined;
    const doc_len_query = std.fmt.bufPrintZ(&doc_len_query_buf, "SELECT doc_length FROM facets.bm25_documents WHERE table_id = {d} AND doc_id = {d}", .{ table_id, doc_id }) catch return 0.0;

    var doc_length: i32 = 0;
    const doc_ret = c.SPI_execute(doc_len_query.ptr, true, 1);
    if (doc_ret == c.SPI_OK_SELECT and c.SPI_processed > 0 and c.SPI_tuptable != null) {
        var doc_isnull: bool = false;
        const doc_len_datum = c.SPI_getbinval(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 1, &doc_isnull);
        if (!doc_isnull) {
            doc_length = @intCast(c.DatumGetInt32(doc_len_datum));
        }
    }

    // Calculate BM25 score manually using stack arrays
    var score: f64 = 0.0;
    const avgdl = stats.avg_document_length;
    const doc_len_f = @as(f64, @floatFromInt(doc_length));

    // For each query term hash, calculate BM25 component
    for (expanded_hashes) |query_hash| {
        // Find frequency for this hash in our stack array
        var tf: i32 = 0;
        for (0..term_freq_count) |i| {
            if (term_freq_hashes[i] == query_hash) {
                tf = term_freq_values[i];
                break;
            }
        }

        if (tf == 0) continue;

        // Get IDF from stats
        const idf = scoring.calculateIDFByHash(query_hash, stats);
        if (idf == 0.0) continue;

        // Calculate BM25 component
        const tf_f = @as(f64, @floatFromInt(tf));
        const numerator = tf_f * (options.k1 + 1.0);
        const denominator = tf_f + options.k1 * (1.0 - options.b + options.b * (doc_len_f / avgdl));

        score += idf * (numerator / denominator);
    }

    return score;
}

/// Rank and limit search results
fn rankResults(results: *std.ArrayList(SearchResult), limit: i32) void {
    // Sort by score descending
    std.mem.sort(SearchResult, results.items, {}, struct {
        fn lessThan(_: void, a: SearchResult, b: SearchResult) bool {
            return a.score > b.score; // Descending
        }
    }.lessThan);

    // Limit results
    if (results.items.len > @as(usize, @intCast(limit))) {
        results.shrinkRetainingCapacity(@as(usize, @intCast(limit)));
    }
}

/// Search documents using BM25
/// This function avoids nested SPI by doing operations in separate phases
pub fn search(
    table_id: c.Oid,
    query_text: []const u8,
    config_name: []const u8,
    options: SearchOptions,
    limit: i32,
    allocator: std.mem.Allocator
) !std.ArrayList(SearchResult) {
    // Phase 1: Tokenize query
    var query_tokens = try tokenizeQuery(query_text, config_name, allocator);
    defer {
        for (query_tokens.items) |token| {
            allocator.free(token);
        }
        query_tokens.deinit(allocator);
    }

    if (query_tokens.items.len == 0) {
        return std.ArrayList(SearchResult).empty;
    }

    // Phase 2: Expand query terms and get document sets
    const combined_bitmap = try expandQueryTerms(table_id, query_tokens, options, allocator);
    defer if (combined_bitmap) |bm| roaring_index.free(bm);

    if (combined_bitmap == null or roaring_index.isEmpty(combined_bitmap.?)) {
        return std.ArrayList(SearchResult).empty;
    }

    // Get the expanded hashes slice for use in scoring
    const MAX_QUERY_HASHES = 64;
    var expanded_hashes_arr: [MAX_QUERY_HASHES]i64 = undefined;
    var expanded_hashes_count: usize = 0;

    // Re-expand to get hashes (we need this for scoring)
    {
        const conn_result = c.SPI_connect();
        const need_finish = (conn_result == c.SPI_OK_CONNECT);
        if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
            return error.SPIConnectFailed;
        }
        defer if (need_finish) {
            _ = c.SPI_finish();
        };

        for (query_tokens.items) |query_term| {
            var matching_hashes = try findMatchingTermHashesInternal(table_id, query_term, options, allocator);
            defer matching_hashes.deinit(allocator);

            for (matching_hashes.items) |hash| {
                if (expanded_hashes_count < MAX_QUERY_HASHES) {
                    expanded_hashes_arr[expanded_hashes_count] = hash;
                    expanded_hashes_count += 1;
                }
            }
        }
    }

    const expanded_hashes = expanded_hashes_arr[0..expanded_hashes_count];

    // Phase 3: Load statistics
    var stats = try scoring.loadStatistics(table_id, allocator);
    defer stats.deinit();

    // Phase 4: Calculate BM25 scores
    var results = try calculateScores(table_id, combined_bitmap.?, expanded_hashes, &stats, options, allocator);

    // Phase 5: Rank and limit results
    rankResults(&results, limit);

    return results;
}

/// Get roaring bitmap of documents matching query
/// If SPI is already connected, use getMatchesBitmapWithExistingConnection instead
pub fn getMatchesBitmap(
    table_id: c.Oid,
    query_text: []const u8,
    config_name: []const u8,
    options: SearchOptions,
    allocator: std.mem.Allocator
) !?*c.roaring_bitmap_t {
    utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Starting, table_id={d}, query_len={d}, config={s}", .{ table_id, query_text.len, config_name });
    
    // Connect to SPI first, then tokenize to avoid nested connections
    utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Connecting to SPI", .{});
    const conn_result = c.SPI_connect();
    const need_finish = (conn_result == c.SPI_OK_CONNECT);
    utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: SPI_connect result={d}, need_finish={}", .{ conn_result, need_finish });
    if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
        utils.elog(c.ERROR, "[TRACE] getMatchesBitmap: SPI_connect failed unexpectedly");
        return error.SPIConnectFailed;
    }
    defer if (need_finish) {
        utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: About to call SPI_finish (need_finish={})", .{need_finish});
        _ = c.SPI_finish();
        utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: SPI_finish completed", .{});
    };
    
    // Phase 1: Tokenize query (assumes SPI is already connected)
    utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Phase 1 - Tokenizing query", .{});
    var query_tokens = try tokenizer.tokenizeWithExistingConnection(query_text, config_name, allocator);
    defer {
        for (query_tokens.items) |token| {
            allocator.free(token);
        }
        query_tokens.deinit(allocator);
    }
    utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Phase 1 - Tokenized, got {d} tokens", .{query_tokens.items.len});
    
    if (query_tokens.items.len == 0) {
        utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: No tokens, returning null", .{});
        return null;
    }
    
    // Phase 2: Expand query terms and get document sets
    const MAX_QUERY_HASHES = 64;
    var expanded_hashes_arr: [MAX_QUERY_HASHES]i64 = undefined;
    var expanded_hashes_count: usize = 0;
    
    var combined_bitmap: ?*c.roaring_bitmap_t = null;
    
    // Expand query terms to hashes
    utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Phase 2 - Expanding query terms to hashes", .{});
    for (query_tokens.items) |query_term| {
        utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Processing token: {s}", .{query_term});
        var matching_hashes = try findMatchingTermHashesInternal(table_id, query_term, options, allocator);
        defer matching_hashes.deinit(allocator);
        
        for (matching_hashes.items) |hash| {
            if (expanded_hashes_count < MAX_QUERY_HASHES) {
                expanded_hashes_arr[expanded_hashes_count] = hash;
                expanded_hashes_count += 1;
            }
        }
    }
    utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Phase 2 - Expanded to {d} hashes", .{expanded_hashes_count});
    
    if (expanded_hashes_count == 0) {
        utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: No matching hashes, returning null", .{});
        return null;
    }
    
    // Get document sets for all term hashes and combine (OR operation)
    // IMPORTANT: Do this while SPI is still connected, then copy the result
    utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Phase 3 - Getting document sets for {d} term hashes", .{expanded_hashes_count});
    for (expanded_hashes_arr[0..expanded_hashes_count], 0..) |term_hash, idx| {
        const i = idx + 1;
        utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Phase 3.{d} - Getting doc_set for hash={d}", .{ i, term_hash });
        const doc_set = try getDocumentSetByHashInternal(table_id, term_hash, allocator);
        
        if (doc_set) |ds| {
            defer {
                utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Phase 3.{d} - Freeing doc_set={*}", .{ i, ds });
                roaring_index.free(ds);
            }
            utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Phase 3.{d} - Got doc_set={*}, cardinality={d}", .{ i, ds, roaring_index.cardinality(ds) });
            if (combined_bitmap) |combined| {
                utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Phase 3.{d} - ORing with existing bitmap", .{i});
                roaring_index.orInPlace(combined, ds);
            } else {
                utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Phase 3.{d} - Copying first bitmap", .{i});
                combined_bitmap = roaring_index.copy(ds);
                utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Phase 3.{d} - Copied bitmap={*}, cardinality={d}", .{ i, combined_bitmap.?, roaring_index.cardinality(combined_bitmap.?) });
            }
        } else {
            utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Phase 3.{d} - No doc_set for hash={d}", .{ i, term_hash });
        }
    }
    
    // SPI_finish will be called here via defer, but combined_bitmap is already copied
    // and allocated with malloc, so it will persist after SPI_finish
    if (combined_bitmap) |bm| {
        utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Returning bitmap={*}, cardinality={d}", .{ bm, roaring_index.cardinality(bm) });
    } else {
        utils.elogFmt(c.NOTICE, "[TRACE] getMatchesBitmap: Returning null (no bitmap)", .{});
    }
    return combined_bitmap;
}

/// Internal version of findMatchingTermHashes (assumes SPI is connected)
fn findMatchingTermHashesInternal(
    table_id: c.Oid,
    query_term: []const u8,
    options: SearchOptions,
    allocator: std.mem.Allocator
) !std.ArrayList(i64) {
    var matching_hashes = std.ArrayList(i64).empty;
    
    if (options.fuzzy_match) {
        const query = try std.fmt.allocPrintSentinel(
            allocator,
            "SELECT term_hash FROM facets.bm25_index WHERE table_id = {d} AND term_text % $1 AND similarity(term_text, $1) >= {d}",
            .{ table_id, options.fuzzy_threshold }, 0);
        defer allocator.free(query);
        
        var argtypes = [_]c.Oid{c.TEXTOID};
        const term_datum = c.PointerGetDatum(c.cstring_to_text_with_len(query_term.ptr, @intCast(query_term.len)));
        var argvalues = [_]c.Datum{term_datum};
        var argnulls = [_]u8{' '};
        
        const ret = c.SPI_execute_with_args(query.ptr, 1, &argtypes, &argvalues, &argnulls, true, 0);
        if (ret == c.SPI_OK_SELECT) {
            var i: u64 = 0;
            while (i < c.SPI_processed) : (i += 1) {
                const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
                const tupdesc = c.SPI_tuptable.*.tupdesc;
                var isnull: bool = false;
                const hash_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);
                if (!isnull) {
                    try matching_hashes.append(allocator, c.DatumGetInt64(hash_datum));
                }
            }
        }
    } else if (options.prefix_match) {
        const query = try std.fmt.allocPrintSentinel(
            allocator,
            "SELECT term_hash FROM facets.bm25_index WHERE table_id = {d} AND term_text LIKE $1 || '%'",
            .{table_id}, 0);
        defer allocator.free(query);
        
        var argtypes = [_]c.Oid{c.TEXTOID};
        const term_datum = c.PointerGetDatum(c.cstring_to_text_with_len(query_term.ptr, @intCast(query_term.len)));
        var argvalues = [_]c.Datum{term_datum};
        var argnulls = [_]u8{' '};
        
        const ret = c.SPI_execute_with_args(query.ptr, 1, &argtypes, &argvalues, &argnulls, true, 0);
        if (ret == c.SPI_OK_SELECT) {
            var i: u64 = 0;
            while (i < c.SPI_processed) : (i += 1) {
                const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
                const tupdesc = c.SPI_tuptable.*.tupdesc;
                var isnull: bool = false;
                const hash_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);
                if (!isnull) {
                    try matching_hashes.append(allocator, c.DatumGetInt64(hash_datum));
                }
            }
        }
    } else {
        // Exact match: hash the query term directly
        try matching_hashes.append(allocator, tokenizer.hashLexeme(query_term));
    }
    
    return matching_hashes;
}

/// Internal version of getDocumentSetByHash (assumes SPI is connected)
fn getDocumentSetByHashInternal(
    table_id: c.Oid,
    term_hash: i64,
    allocator: std.mem.Allocator
) !?*c.roaring_bitmap_t {
    utils.elogFmt(c.NOTICE, "[TRACE] getDocumentSetByHashInternal: Starting, table_id={d}, term_hash={d}", .{ table_id, term_hash });
    
    const query = try std.fmt.allocPrintSentinel(
        allocator,
        "SELECT doc_ids FROM facets.bm25_index WHERE table_id = {d} AND term_hash = {d}",
        .{ table_id, term_hash }, 0);
    defer allocator.free(query);
    
    utils.elogFmt(c.NOTICE, "[TRACE] getDocumentSetByHashInternal: Executing query: {s}", .{query});
    const ret = c.SPI_execute(query.ptr, true, 1);
    utils.elogFmt(c.NOTICE, "[TRACE] getDocumentSetByHashInternal: SPI_execute result={d}, processed={d}", .{ ret, c.SPI_processed });
    if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0) {
        utils.elogFmt(c.NOTICE, "[TRACE] getDocumentSetByHashInternal: No results, returning null", .{});
        return null;
    }
    
    utils.elogFmt(c.NOTICE, "[TRACE] getDocumentSetByHashInternal: Got {d} rows, getting tuple", .{c.SPI_processed});
    const tuple = c.SPI_tuptable.*.vals[0];
    const tupdesc = c.SPI_tuptable.*.tupdesc;
    
    var isnull: bool = false;
    utils.elogFmt(c.NOTICE, "[TRACE] getDocumentSetByHashInternal: Getting doc_ids_datum from tuple", .{});
    const doc_ids_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);
    utils.elogFmt(c.NOTICE, "[TRACE] getDocumentSetByHashInternal: Got doc_ids_datum, isnull={}", .{isnull});
    
    if (isnull) {
        utils.elogFmt(c.NOTICE, "[TRACE] getDocumentSetByHashInternal: doc_ids_datum is null, returning null", .{});
        return null;
    }
    
    utils.elogFmt(c.NOTICE, "[TRACE] getDocumentSetByHashInternal: Calling datumToRoaringBitmap", .{});
    const bitmap = try roaring_index.datumToRoaringBitmap(doc_ids_datum);
    utils.elogFmt(c.NOTICE, "[TRACE] getDocumentSetByHashInternal: datumToRoaringBitmap returned bitmap={*}, cardinality={d}", .{ bitmap, roaring_index.cardinality(bitmap) });
    
    return bitmap;
}

/// Calculate BM25 score for a single document
/// IMPORTANT: This function uses a PURE ZIG tokenizer to avoid SPI issues when called
/// from within PL/pgSQL EXECUTE statements. The pure tokenizer does simple word splitting
/// without PostgreSQL's stemming, so results may differ slightly from to_tsvector.
pub fn calculateScore(
    table_id: c.Oid,
    query_text: []const u8,
    doc_id: i64,
    config_name: []const u8,
    options: SearchOptions,
    allocator: std.mem.Allocator
) !f64 {
    _ = config_name; // Not used with pure tokenizer
    
    utils.elogFmt(c.NOTICE, "[TRACE] calculateScore: Starting for doc_id={d}", .{doc_id});
    
    // Phase 1: Tokenize query using PURE ZIG tokenizer (NO SPI!)
    // This is critical to avoid crashes when called from within EXECUTE statements
    var query_tokens = try tokenizer_pure.tokenizePure(query_text, allocator);
    defer {
        for (query_tokens.items) |token| {
            allocator.free(token);
        }
        query_tokens.deinit(allocator);
    }
    
    utils.elogFmt(c.NOTICE, "[TRACE] calculateScore: Tokenized query into {d} tokens", .{query_tokens.items.len});
    
    if (query_tokens.items.len == 0) {
        return 0.0;
    }
    
    // Convert query tokens to hashes (no SPI needed)
    var query_hashes = std.ArrayList(i64).empty;
    defer query_hashes.deinit(allocator);
    
    for (query_tokens.items) |term| {
        try query_hashes.append(allocator, tokenizer_pure.hashLexeme(term));
    }
    
    utils.elogFmt(c.NOTICE, "[TRACE] calculateScore: Generated {d} hashes", .{query_hashes.items.len});
    
    // Phase 2: Load statistics (uses SPI but handles connection safely)
    utils.elogFmt(c.NOTICE, "[TRACE] calculateScore: Loading statistics", .{});
    var stats = try scoring.loadStatistics(table_id, allocator);
    defer stats.deinit();
    utils.elogFmt(c.NOTICE, "[TRACE] calculateScore: Statistics loaded, total_docs={d}", .{stats.total_documents});
    
    // Phase 3: Get term frequencies and document length (single SPI connection)
    var term_freqs = std.AutoHashMap(i64, i32).init(allocator);
    defer term_freqs.deinit();
    var doc_length: i32 = 0;
    
    // Pre-allocate term_freqs
    try term_freqs.ensureTotalCapacity(@intCast(query_hashes.items.len));
    
    {
        utils.elogFmt(c.NOTICE, "[TRACE] calculateScore: Phase 3 - SPI_connect", .{});
        // Try to connect - may already be connected from caller
        const conn_result3 = c.SPI_connect();
        const need_finish3 = (conn_result3 == c.SPI_OK_CONNECT);
        utils.elogFmt(c.NOTICE, "[TRACE] calculateScore: SPI_connect result={d}, need_finish={}", .{conn_result3, need_finish3});
        if (conn_result3 != c.SPI_OK_CONNECT and conn_result3 != c.SPI_ERROR_CONNECT) {
            utils.elog(c.ERROR, "SPI_connect failed");
            return error.SPIConnectFailed;
        }
        defer if (need_finish3) {
            utils.elogFmt(c.NOTICE, "[TRACE] calculateScore: Calling SPI_finish", .{});
            _ = c.SPI_finish();
        };
        
        // Get term frequencies for this document
        utils.elogFmt(c.NOTICE, "[TRACE] calculateScore: Getting term frequencies", .{});
        for (query_hashes.items) |term_hash| {
            const freq = try getTermFrequencyByHashInternal(table_id, term_hash, doc_id, allocator);
            if (freq > 0) {
                term_freqs.putAssumeCapacity(term_hash, freq);
            }
        }
        
        // Get document length
        utils.elogFmt(c.NOTICE, "[TRACE] calculateScore: Getting document length", .{});
        doc_length = try getDocumentLengthInternal(table_id, doc_id, allocator);
        utils.elogFmt(c.NOTICE, "[TRACE] calculateScore: doc_length={d}", .{doc_length});
    }
    
    if (term_freqs.count() == 0) {
        utils.elogFmt(c.NOTICE, "[TRACE] calculateScore: No matching terms, returning 0", .{});
        return 0.0;
    }
    
    // Phase 4: Calculate BM25 score (no SPI needed)
    utils.elogFmt(c.NOTICE, "[TRACE] calculateScore: Calculating BM25 score", .{});
    return scoring.calculateBM25ByHash(
        query_hashes.items,
        term_freqs,
        doc_length,
        &stats,
        options.k1,
        options.b
    );
}

/// Internal version of getTermFrequencyByHash (assumes SPI is connected)
fn getTermFrequencyByHashInternal(
    table_id: c.Oid,
    term_hash: i64,
    doc_id: i64,
    allocator: std.mem.Allocator
) !i32 {
    _ = allocator; // Not needed - use stack buffer
    
    // Use stack-allocated buffer to avoid palloc issues during SPI
    var query_buf: [256]u8 = undefined;
    const query = std.fmt.bufPrintZ(&query_buf, "SELECT (term_freqs->>'{d}')::int FROM facets.bm25_index WHERE table_id = {d} AND term_hash = {d}", .{ doc_id, table_id, term_hash }) catch {
        return error.BufferTooSmall;
    };

    const ret = c.SPI_execute(query.ptr, true, 1);
    if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0) {
        return 0;
    }

    const tuple = c.SPI_tuptable.*.vals[0];
    const tupdesc = c.SPI_tuptable.*.tupdesc;

    var isnull: bool = false;
    const freq_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);

    if (isnull) {
        return 0;
    }

    return @intCast(c.DatumGetInt32(freq_datum));
}

/// Internal version of getDocumentLength (assumes SPI is connected)
fn getDocumentLengthInternal(
    table_id: c.Oid,
    doc_id: i64,
    allocator: std.mem.Allocator
) !i32 {
    _ = allocator; // Not needed - use stack buffer
    
    // Use stack-allocated buffer to avoid palloc issues during SPI
    var query_buf: [256]u8 = undefined;
    const query = std.fmt.bufPrintZ(&query_buf, "SELECT doc_length FROM facets.bm25_documents WHERE table_id = {d} AND doc_id = {d}", .{ table_id, doc_id }) catch {
        utils.elog(c.ERROR, "Query buffer too small");
        return 0;
    };
    
    const ret = c.SPI_execute(query.ptr, true, 1);
    if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0) {
        return 0;
    }
    
    const tuple = c.SPI_tuptable.*.vals[0];
    const tupdesc = c.SPI_tuptable.*.tupdesc;
    
    var isnull: bool = false;
    const length_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);
    
    if (isnull) {
        return 0;
    }
    
    return @intCast(c.DatumGetInt32(length_datum));
}
