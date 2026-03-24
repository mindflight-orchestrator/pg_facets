const std = @import("std");
const utils = @import("utils.zig");
const deltas = @import("deltas.zig");
const filters = @import("filters.zig");
const facets = @import("facets.zig");
const search = @import("search.zig");
const bm25_index = @import("bm25/index.zig");
const bm25_search = @import("bm25/search.zig");
const roaring_index = @import("bm25/roaring_index.zig");
const worker_native = @import("bm25/worker_native.zig");
const tokenizer_test = @import("bm25/tokenizer_test.zig");
const search_native = @import("bm25/search_native.zig");
const stats_native = @import("bm25/stats_native.zig");

// Import C headers
const c = utils.c;

// PostgreSQL module magic - must be a function that returns pointer to Pg_magic_struct
// version field is PG_VERSION_NUM / 100, so for PG 17.0.7 (170007) it's 1700
const pg_magic_data = c.Pg_magic_struct{
    .len = @sizeOf(c.Pg_magic_struct),
    .version = 1700, // PG_VERSION_NUM / 100 for PostgreSQL 17.x
    .funcmaxargs = 100,
    .indexmaxkeys = 32,
    .namedatalen = 64,
    .float8byval = 1, // FLOAT8PASSBYVAL = true
    .abi_extra = "PostgreSQL".* ++ [_]u8{0} ** 22, // "PostgreSQL" + padding to 32 bytes
};

export fn Pg_magic_func() callconv(.c) *const c.Pg_magic_struct {
    return &pg_magic_data;
}

// Export the function
export fn merge_deltas_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return deltas.merge_deltas_native(fcinfo);
}

// Function info for V1 calling convention
const PgFinfoRecord = extern struct {
    api_version: c_int,
};

export fn pg_finfo_merge_deltas_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export build_filter_bitmap_native
export fn build_filter_bitmap_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return filters.build_filter_bitmap_native(fcinfo);
}

export fn pg_finfo_build_filter_bitmap_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export get_facet_counts_native
export fn get_facet_counts_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return facets.get_facet_counts_native(fcinfo);
}

export fn pg_finfo_get_facet_counts_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export search_documents_native
export fn search_documents_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return search.search_documents_native(fcinfo);
}

export fn pg_finfo_search_documents_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export filter_documents_by_facets_bitmap_jsonb_native
export fn filter_documents_by_facets_bitmap_jsonb_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return filters.filter_documents_by_facets_bitmap_jsonb_native(fcinfo);
}

export fn pg_finfo_filter_documents_by_facets_bitmap_jsonb_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// ============================================================================
// BM25 Function Exports
// ============================================================================

// Export bm25_index_document_native
export fn bm25_index_document_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = utils.PgAllocator.allocator();
    
    // args[0]: table_id (oid)
    if (utils.is_arg_null(fcinfo, 0)) {
        return c.PointerGetDatum(null);
    }
    const table_id_datum = utils.get_arg_datum(fcinfo, 0);
    const table_id: c.Oid = @intCast(table_id_datum);
    
    // args[1]: doc_id (bigint)
    if (utils.is_arg_null(fcinfo, 1)) {
        return c.PointerGetDatum(null);
    }
    const doc_id_datum = utils.get_arg_datum(fcinfo, 1);
    const doc_id: i64 = @intCast(c.DatumGetInt64(doc_id_datum));
    
    // args[2]: content (text) - copy to stable memory
    if (utils.is_arg_null(fcinfo, 2)) {
        return c.PointerGetDatum(null);
    }
    const content_datum = utils.get_arg_datum(fcinfo, 2);
    const content_cstr = utils.textToCstring(content_datum);
    if (content_cstr == null) {
        utils.elogWithContext(c.ERROR, "bm25_index_document_native", "Failed to extract content text");
        return c.PointerGetDatum(null);
    }
    // Use bounded strlen with max check and UTF-8 safe truncation
    // Measure length safely (stop at max_content_len or null terminator)
    var content_len: usize = 0;
    const max_content_len: usize = 10485760; // Max 10MB
    while (content_len < max_content_len) {
        if (content_cstr[content_len] == 0) {
            break; // Found null terminator
        }
        content_len += 1;
    }
    var was_truncated = false;
    if (content_len >= max_content_len) {
        // Document exceeds limit - truncate to max_content_len with UTF-8 safety
        // First, get the raw bytes up to max_content_len
        const raw_slice = content_cstr[0..max_content_len];
        // Find safe UTF-8 truncation point
        content_len = utils.truncateUtf8Safe(raw_slice, max_content_len);
        was_truncated = true;
        utils.elogFmt(c.WARNING, 
            "bm25_index_document_native: Document content exceeds maximum size. Truncating to {d} bytes (UTF-8 safe). Consider splitting large documents.", 
            .{content_len});
    }
    if (content_len == 0) {
        c.pfree(@ptrCast(content_cstr));
        return c.PointerGetDatum(null);
    }
    const content = allocator.alloc(u8, content_len) catch {
        c.pfree(@ptrCast(content_cstr));
        utils.elogWithContext(c.ERROR, "bm25_index_document_native", "Failed to allocate content buffer");
        return c.PointerGetDatum(null);
    };
    // Safe copy: content_len is guaranteed to be <= max_content_len and within bounds, UTF-8 safe
    @memcpy(content, content_cstr[0..content_len]);
    c.pfree(@ptrCast(content_cstr));
    defer allocator.free(content);
    
    // args[3]: language (text, optional, default 'english') - copy to stable memory
    var language: []const u8 = "english";
    var language_alloc: ?[]u8 = null;
    defer if (language_alloc) |la| allocator.free(la);
    
    if (!utils.is_arg_null(fcinfo, 3)) {
        const lang_datum = utils.get_arg_datum(fcinfo, 3);
        const lang_cstr = utils.textToCstring(lang_datum);
        if (lang_cstr == null) {
            utils.elogWithContext(c.ERROR, "bm25_index_document_native", "Failed to extract language text");
            return c.PointerGetDatum(null);
        }
        // Use bounded strlen with max check
        var lang_len: usize = 0;
        const max_lang_len: usize = 64;
        while (lang_len < max_lang_len and lang_cstr[lang_len] != 0) {
            lang_len += 1;
        }
        if (lang_len >= max_lang_len) {
            c.pfree(@ptrCast(lang_cstr));
            utils.elogFmt(c.WARNING, 
                "bm25_index_document_native: Language text too long or not null-terminated (length >= {d} bytes, max {d} bytes). Using default 'english'. Language names should be short (e.g., 'english', 'spanish').", 
                .{ lang_len, max_lang_len });
            // Use default 'english' instead of returning error
            language = "english";
        } else {
            language_alloc = allocator.alloc(u8, lang_len) catch {
                c.pfree(@ptrCast(lang_cstr));
                utils.elogWithContext(c.ERROR, "bm25_index_document_native", "Failed to allocate language buffer");
                return c.PointerGetDatum(null);
            };
            @memcpy(language_alloc.?, lang_cstr[0..lang_len]);
            c.pfree(@ptrCast(lang_cstr));
            language = language_alloc.?;
        }
    }
    
    bm25_index.indexDocument(table_id, doc_id, content, language, allocator) catch {
        utils.elogWithContext(c.ERROR, "bm25_index_document_native", "Failed to index document");
        return c.PointerGetDatum(null);
    };
    
    return c.PointerGetDatum(null);
}

export fn pg_finfo_bm25_index_document_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export bm25_delete_document_native
export fn bm25_delete_document_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = utils.PgAllocator.allocator();
    
    // args[0]: table_id (oid)
    const table_id_datum = utils.get_arg_datum(fcinfo, 0);
    const table_id: c.Oid = @intCast(table_id_datum);
    
    // args[1]: doc_id (bigint)
    const doc_id_datum = utils.get_arg_datum(fcinfo, 1);
    const doc_id: i64 = @intCast(c.DatumGetInt64(doc_id_datum));
    
    bm25_index.deleteDocument(table_id, doc_id, allocator) catch {
        utils.elogWithContext(c.ERROR, "bm25_delete_document_native", "Failed to delete document");
        return c.PointerGetDatum(null);
    };
    
    return c.PointerGetDatum(null);
}

export fn pg_finfo_bm25_delete_document_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export bm25_recalculate_statistics_native
export fn bm25_recalculate_statistics_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = utils.PgAllocator.allocator();
    
    // args[0]: table_id (oid)
    const table_id_datum = utils.get_arg_datum(fcinfo, 0);
    const table_id: c.Oid = @intCast(table_id_datum);
    
    bm25_index.recalculateStatistics(table_id, allocator) catch {
        utils.elogWithContext(c.ERROR, "bm25_recalculate_statistics_native", "Failed to recalculate statistics");
        return c.PointerGetDatum(null);
    };
    
    return c.PointerGetDatum(null);
}

export fn pg_finfo_bm25_recalculate_statistics_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export bm25_search_native - uses optimized batch queries (3 queries instead of O(docs × terms))
export fn bm25_search_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return search_native.bm25_search_native(fcinfo);
}

export fn pg_finfo_bm25_search_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export bm25_score_native
export fn bm25_score_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    utils.elog(c.LOG, "[TRACE] bm25_score_native: ENTRY");
    const allocator = utils.PgAllocator.allocator();
    
    // args[0]: table_id (oid)
    const table_id_datum = utils.get_arg_datum(fcinfo, 0);
    const table_id: c.Oid = @intCast(table_id_datum);
    utils.elogFmt(c.NOTICE, "[TRACE] bm25_score_native: table_id={d}", .{table_id});
    
    // args[1]: query (text) - use text_to_cstring for safest extraction
    const query_datum = utils.get_arg_datum(fcinfo, 1);
    if (utils.is_arg_null(fcinfo, 1)) {
        utils.elogWithContext(c.ERROR, "bm25_score_native", "Query text cannot be null");
        return c.Float8GetDatum(0.0);
    }
    const query_cstr = utils.textToCstring(query_datum);
    if (query_cstr == null) {
        utils.elogWithContext(c.ERROR, "bm25_score_native", "Failed to extract query text");
        return c.Float8GetDatum(0.0);
    }
    // Use bounded strlen with max check to prevent reading corrupted memory
    var query_len: usize = 0;
    const max_len: usize = 1048576; // Max 1MB
    while (query_len < max_len and query_cstr[query_len] != 0) {
        query_len += 1;
    }
    if (query_len >= max_len) {
        c.pfree(@ptrCast(query_cstr));
        utils.elogFmt(c.ERROR, 
            "bm25_score_native: Query text too long or not null-terminated (length >= {d} bytes, max {d} bytes). Please reduce query size.", 
            .{ query_len, max_len });
        return c.Float8GetDatum(0.0);
    }
    const query = allocator.alloc(u8, query_len) catch {
        c.pfree(@ptrCast(query_cstr));
        utils.elogWithContext(c.ERROR, "bm25_score_native", "Failed to allocate query buffer");
        return c.Float8GetDatum(0.0);
    };
    defer allocator.free(query);
    defer c.pfree(@ptrCast(query_cstr));
    @memcpy(query, query_cstr[0..query_len]);
    
    // args[2]: doc_id (bigint)
    const doc_id_datum = utils.get_arg_datum(fcinfo, 2);
    const doc_id: i64 = @intCast(c.DatumGetInt64(doc_id_datum));
    
    // args[3]: language (text, optional, default 'english') - copy to stable memory
    var language: []const u8 = "english";
    var language_alloc: ?[]u8 = null;
    defer if (language_alloc) |la| allocator.free(la);
    
    if (!utils.is_arg_null(fcinfo, 3)) {
        const lang_datum = utils.get_arg_datum(fcinfo, 3);
        // Use text_to_cstring for safest extraction
        const lang_cstr = utils.textToCstring(lang_datum);
        if (lang_cstr == null) {
            utils.elogWithContext(c.ERROR, "bm25_score_native", "Failed to extract language text");
            return c.Float8GetDatum(0.0);
        }
        // Use bounded strlen with max check
        var lang_len: usize = 0;
        const max_lang_len: usize = 64;
        while (lang_len < max_lang_len and lang_cstr[lang_len] != 0) {
            lang_len += 1;
        }
        if (lang_len >= max_lang_len) {
            c.pfree(@ptrCast(lang_cstr));
            utils.elogFmt(c.WARNING, 
                "bm25_score_native: Language text too long or not null-terminated (length >= {d} bytes, max {d} bytes). Using default 'english'. Language names should be short (e.g., 'english', 'spanish').", 
                .{ lang_len, max_lang_len });
            // Use default 'english' instead of returning error
            language = "english";
        } else {
            language_alloc = allocator.alloc(u8, lang_len) catch {
                c.pfree(@ptrCast(lang_cstr));
                utils.elogWithContext(c.ERROR, "bm25_score_native", "Failed to allocate language buffer");
                return c.Float8GetDatum(0.0);
            };
            @memcpy(language_alloc.?, lang_cstr[0..lang_len]);
            c.pfree(@ptrCast(lang_cstr));
            language = language_alloc.?;
        }
    }
    
    // args[4]: k1 (float, optional, default 1.2)
    const k1: f64 = if (!utils.is_arg_null(fcinfo, 4))
        c.DatumGetFloat8(utils.get_arg_datum(fcinfo, 4))
    else
        1.2;
    
    // args[5]: b (float, optional, default 0.75)
    const b: f64 = if (!utils.is_arg_null(fcinfo, 5))
        c.DatumGetFloat8(utils.get_arg_datum(fcinfo, 5))
    else
        0.75;
    
    const options = bm25_search.SearchOptions{
        .prefix_match = false,
        .fuzzy_match = false,
        .fuzzy_threshold = 0.3,
        .k1 = k1,
        .b = b,
    };
    
    utils.elogFmt(c.NOTICE, "[TRACE] bm25_score_native: Calling calculateScore for doc_id={d}", .{doc_id});
    const score = bm25_search.calculateScore(table_id, query, doc_id, language, options, allocator) catch {
        utils.elogWithContext(c.ERROR, "bm25_score_native", "BM25 score calculation failed");
        return c.Float8GetDatum(0.0);
    };
    
    utils.elogFmt(c.NOTICE, "[TRACE] bm25_score_native: Returning score={d}", .{score});
    return c.Float8GetDatum(score);
}

export fn pg_finfo_bm25_score_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export bm25_get_matches_bitmap_native
export fn bm25_get_matches_bitmap_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    // Log IMMEDIATELY with LOG level (always visible) to catch early crashes
    utils.elog(c.LOG, "[TRACE] bm25_get_matches_bitmap_native: ENTRY - Function called");
    const allocator = utils.PgAllocator.allocator();
    utils.elog(c.LOG, "[TRACE] bm25_get_matches_bitmap_native: Allocator obtained");
    utils.elog(c.LOG, "[TRACE] bm25_get_matches_bitmap_native: About to check args");
    
    // args[0]: table_id (oid)
    utils.elog(c.LOG, "[TRACE] bm25_get_matches_bitmap_native: Checking arg 0 (table_id)");
    if (utils.is_arg_null(fcinfo, 0)) {
        utils.elogFmt(c.NOTICE, "[TRACE] bm25_get_matches_bitmap_native: table_id is null, returning null", .{});
        utils.set_return_null(fcinfo);
        return c.PointerGetDatum(null);
    }
    const table_id_datum = utils.get_arg_datum(fcinfo, 0);
    const table_id: c.Oid = @intCast(table_id_datum);
    utils.elogFmt(c.NOTICE, "[TRACE] bm25_get_matches_bitmap_native: table_id={d}", .{table_id});
    
    // Check if table_id is 0 (invalid)
    if (table_id == 0) {
        utils.elogFmt(c.NOTICE, "[TRACE] bm25_get_matches_bitmap_native: table_id is 0 (invalid), returning null", .{});
        utils.set_return_null(fcinfo);
        return c.PointerGetDatum(null);
    }
    
    // args[1]: query (text) - copy to stable memory
    utils.elog(c.LOG, "[TRACE] bm25_get_matches_bitmap_native: Checking arg 1 (query)");
    if (utils.is_arg_null(fcinfo, 1)) {
        utils.elog(c.LOG, "[TRACE] bm25_get_matches_bitmap_native: query is null, returning null");
        utils.set_return_null(fcinfo);
        return c.PointerGetDatum(null);
    }
    utils.elog(c.LOG, "[TRACE] bm25_get_matches_bitmap_native: Getting query datum");
    const query_datum = utils.get_arg_datum(fcinfo, 1);
    utils.elog(c.LOG, "[TRACE] bm25_get_matches_bitmap_native: Converting query to C string");
    const query_cstr = utils.textToCstring(query_datum);
    if (query_cstr == null) {
        utils.elogWithContext(c.ERROR, "bm25_get_matches_bitmap_native", "Failed to extract query text");
        return c.PointerGetDatum(null);
    }
    
    // Use bounded strlen with max check to prevent reading corrupted memory
    var query_len: usize = 0;
    const max_query_len: usize = 1048576; // Max 1MB
    while (query_len < max_query_len and query_cstr[query_len] != 0) {
        query_len += 1;
    }
    if (query_len >= max_query_len) {
        c.pfree(@ptrCast(query_cstr));
        utils.elogFmt(c.ERROR, 
            "bm25_get_matches_bitmap_native: Query text too long or not null-terminated (length >= {d} bytes, max {d} bytes). Please reduce query size or split into multiple queries.", 
            .{ query_len, max_query_len });
        return c.PointerGetDatum(null);
    }
    const query = allocator.alloc(u8, query_len) catch {
        c.pfree(@ptrCast(query_cstr));
        utils.elogWithContext(c.ERROR, "bm25_get_matches_bitmap_native", "Failed to allocate query buffer");
        return c.PointerGetDatum(null);
    };
    @memcpy(query, query_cstr[0..query_len]);
    c.pfree(@ptrCast(query_cstr));
    defer allocator.free(query);
    
    // Check if query is empty
    if (query.len == 0) {
        utils.set_return_null(fcinfo);
        return c.PointerGetDatum(null);
    }
    
    // args[2]: language (text, optional) - copy to stable memory
    // If NULL, default to "english"
    var language: []const u8 = "english";
    var language_alloc: ?[]u8 = null;
    defer if (language_alloc) |la| allocator.free(la);
    
    if (!utils.is_arg_null(fcinfo, 2)) {
        const lang_datum = utils.get_arg_datum(fcinfo, 2);
        const lang_cstr = utils.textToCstring(lang_datum);
        if (lang_cstr == null) {
            utils.elogWithContext(c.ERROR, "bm25_get_matches_bitmap_native", "Failed to extract language text");
            return c.PointerGetDatum(null);
        }
        // Use bounded strlen with max check
        var lang_len: usize = 0;
        const max_lang_len: usize = 64;
        while (lang_len < max_lang_len and lang_cstr[lang_len] != 0) {
            lang_len += 1;
        }
        if (lang_len >= max_lang_len) {
            c.pfree(@ptrCast(lang_cstr));
            utils.elogFmt(c.WARNING, 
                "bm25_get_matches_bitmap_native: Language text too long or not null-terminated (length >= {d} bytes, max {d} bytes). Using default 'english'. Language names should be short (e.g., 'english', 'spanish').", 
                .{ lang_len, max_lang_len });
            // Use default 'english' instead of returning error
            language = "english";
        } else {
            language_alloc = allocator.alloc(u8, lang_len) catch {
                c.pfree(@ptrCast(lang_cstr));
                utils.elogWithContext(c.ERROR, "bm25_get_matches_bitmap_native", "Failed to allocate language buffer");
                return c.PointerGetDatum(null);
            };
            @memcpy(language_alloc.?, lang_cstr[0..lang_len]);
            c.pfree(@ptrCast(lang_cstr));
            language = language_alloc.?;
        }
    }
    
    // args[3]: prefix_match (boolean)
    const prefix_match: bool = if (!utils.is_arg_null(fcinfo, 3))
        c.DatumGetBool(utils.get_arg_datum(fcinfo, 3))
    else
        false;
    
    // args[4]: fuzzy_match (boolean)
    const fuzzy_match: bool = if (!utils.is_arg_null(fcinfo, 4))
        c.DatumGetBool(utils.get_arg_datum(fcinfo, 4))
    else
        false;
    
    // args[5]: fuzzy_threshold (float)
    const fuzzy_threshold: f64 = if (!utils.is_arg_null(fcinfo, 5))
        c.DatumGetFloat8(utils.get_arg_datum(fcinfo, 5))
    else
        0.3;
        
    const options = bm25_search.SearchOptions{
        .prefix_match = prefix_match,
        .fuzzy_match = fuzzy_match,
        .fuzzy_threshold = fuzzy_threshold,
    };
    
    utils.elogFmt(c.NOTICE, "[TRACE] bm25_get_matches_bitmap_native: Calling getMatchesBitmap (table_id={d}, query_len={d}, language={s})", .{ table_id, query.len, language });
    const bitmap = bm25_search.getMatchesBitmap(table_id, query, language, options, allocator) catch |err| {
        utils.elogFmt(c.ERROR, "[TRACE] bm25_get_matches_bitmap_native: getMatchesBitmap failed with error: {}", .{err});
        return c.PointerGetDatum(null);
    };
    utils.elogFmt(c.NOTICE, "[TRACE] bm25_get_matches_bitmap_native: getMatchesBitmap returned, bitmap={*}", .{bitmap});
    
    if (bitmap) |bm| {
        utils.elogFmt(c.NOTICE, "[TRACE] bm25_get_matches_bitmap_native: Converting bitmap to Datum, bitmap={*}, cardinality={d}", .{ bm, roaring_index.cardinality(bm) });
        // CRITICAL: Serialize the bitmap to Datum
        // roaring_bitmap_portable_serialize makes a complete copy into the varlena buffer
        // The bitmap can be freed after serialization, but we'll let PostgreSQL manage it
        // via the memory context to avoid use-after-free issues
        const datum = roaring_index.roaringBitmapToDatum(bm);
        utils.elogFmt(c.NOTICE, "[TRACE] bm25_get_matches_bitmap_native: Datum created successfully", .{});
        // NOTE: We do NOT free the bitmap here because:
        // 1. The bitmap is allocated with malloc (via roaring_bitmap_copy)
        // 2. The Datum is returned and may be used later by PostgreSQL
        // 3. Freeing the bitmap immediately could cause use-after-free if PostgreSQL
        //    tries to access it (even though serialize should have copied everything)
        // TODO: Implement proper memory management with PostgreSQL memory contexts
        // For now, this is a memory leak but prevents crashes
        utils.elogFmt(c.NOTICE, "[TRACE] bm25_get_matches_bitmap_native: Returning Datum (bitmap will be leaked but prevents crash)", .{});
        return datum;
    } else {
        utils.elogFmt(c.NOTICE, "[TRACE] bm25_get_matches_bitmap_native: Bitmap is null, returning null", .{});
        utils.set_return_null(fcinfo);
        return c.PointerGetDatum(null);
    }
}

export fn pg_finfo_bm25_get_matches_bitmap_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export bm25_index_worker_native
export fn bm25_index_worker_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return worker_native.bm25_index_worker_native(fcinfo);
}

export fn pg_finfo_bm25_index_worker_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export test_tokenize_only - minimal tokenization test
export fn test_tokenize_only(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return tokenizer_test.test_tokenize_only(fcinfo);
}

export fn pg_finfo_test_tokenize_only() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export bm25_term_stats - term statistics like ts_stat for BM25
export fn bm25_term_stats(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return stats_native.bm25_term_stats(fcinfo);
}

export fn pg_finfo_bm25_term_stats() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export bm25_doc_stats - document statistics for debugging
export fn bm25_doc_stats(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return stats_native.bm25_doc_stats(fcinfo);
}

export fn pg_finfo_bm25_doc_stats() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export bm25_collection_stats - collection-wide statistics
export fn bm25_collection_stats(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return stats_native.bm25_collection_stats(fcinfo);
}

export fn pg_finfo_bm25_collection_stats() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// Export bm25_explain_doc - explain BM25 term weights for a document
export fn bm25_explain_doc(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return stats_native.bm25_explain_doc(fcinfo);
}

export fn pg_finfo_bm25_explain_doc() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

// ============================================================================
// Hardware Support Function
// ============================================================================

// External declaration for CRoaring hardware support function
extern fn croaring_hardware_support() c_int;

// Export current_hardware - returns hardware support as composite type
// Returns: (support_code integer, description text)
// - 0 = No SIMD support
// - 1 = AVX2 support only
// - 2 = AVX-512 support only
// - 3 = Both AVX2 and AVX-512 support
export fn current_hardware(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const support = croaring_hardware_support();
    
    // Get tuple descriptor for composite return type
    var tupdesc: c.TupleDesc = undefined;
    if (c.get_call_result_type(fcinfo, null, &tupdesc) != c.TYPEFUNC_COMPOSITE) {
        utils.elogWithContext(c.ERROR, "current_hardware", "return type must be a row type");
    }
    
    // Get description based on support code
    const description: []const u8 = switch (support) {
        0 => "No SIMD support",
        1 => "AVX2 support only",
        2 => "AVX-512 support only",
        3 => "Both AVX2 and AVX-512 support",
        else => "Unknown hardware support",
    };
    
    // Create text datum for description
    const desc_text = c.cstring_to_text_with_len(description.ptr, @intCast(description.len));
    
    // Create tuple with two values: support_code (integer) and description (text)
    var values = [_]c.Datum{
        c.Int32GetDatum(@intCast(support)),
        c.PointerGetDatum(desc_text),
    };
    var nulls = [_]bool{ false, false };
    
    const tuple = c.heap_form_tuple(tupdesc, &values, &nulls);
    return c.HeapTupleGetDatum(tuple);
}

export fn pg_finfo_current_hardware() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{
            .api_version = 1,
        };
    };
    return &info.val;
}

