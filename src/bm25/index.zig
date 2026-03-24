const std = @import("std");
const utils = @import("../utils.zig");
const c = utils.c;
const PgAllocator = utils.PgAllocator;
const tokenizer = @import("tokenizer.zig");
const roaring_index = @import("roaring_index.zig");

/// Get roaringbitmap type OID
fn getRoaringBitmapOid(_: std.mem.Allocator) !c.Oid {
    const oid_query = "SELECT oid FROM pg_type WHERE typname = 'roaringbitmap'";
    const oid_ret = c.SPI_execute(oid_query.ptr, true, 1);
    if (oid_ret == c.SPI_OK_SELECT and c.SPI_processed > 0) {
        const oid_tuple = c.SPI_tuptable.*.vals[0];
        const oid_tupdesc = c.SPI_tuptable.*.tupdesc;
        var isnull_oid: bool = false;
        const oid_datum = c.SPI_getbinval(oid_tuple, oid_tupdesc, 1, &isnull_oid);
        if (!isnull_oid) {
            return @as(c.Oid, @intCast(c.DatumGetObjectId(oid_datum)));
        }
    }
    return c.BYTEAOID; // Fallback
}

/// Index a document for BM25 search
pub fn indexDocument(
    table_id: c.Oid,
    doc_id: i64,
    text: []const u8,
    config_name: []const u8,
    allocator: std.mem.Allocator
) !void {
    if (c.SPI_connect() != c.SPI_OK_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed");
        return error.SPIConnectFailed;
    }
    defer _ = c.SPI_finish();
    
    // ACID Compliance: Create savepoint for atomic operation (graceful failure)
    var savepoint_created = false;
    const savepoint_sql = "SAVEPOINT bm25_index_doc";
    const savepoint_ret = c.SPI_execute(savepoint_sql, false, 0);
    if (savepoint_ret == c.SPI_OK_UTILITY) {
        savepoint_created = true;
    } else {
        // Log debug message - outer transaction provides atomicity
        utils.elog(c.DEBUG1, "Could not create savepoint (may be in nested transaction), continuing without savepoint");
    }
    
    // On error path: rollback if savepoint exists
    errdefer {
        if (savepoint_created) {
            _ = c.SPI_execute("ROLLBACK TO SAVEPOINT bm25_index_doc", false, 0);
        }
    }
    
    // Get roaringbitmap OID once
    const roaringbitmap_oid = try getRoaringBitmapOid(allocator);
    
    // Tokenize document
    // Use helper that assumes SPI is already connected to avoid nested connections
    // Note: We don't defer free here because SPI memory context handles cleanup
    const tokens = try tokenizer.tokenizeWithExistingConnection(text, config_name, allocator);
    
    if (tokens.items.len == 0) {
        // No tokens - just update document metadata with length 0
        const is_new_doc = try updateDocumentMetadata(table_id, doc_id, 0, config_name, allocator);
        try updateStatisticsIncremental(table_id, 0, is_new_doc, allocator);
        // Release savepoint if it was created
        if (savepoint_created) {
            const release_sql = "RELEASE SAVEPOINT bm25_index_doc";
            _ = c.SPI_execute(release_sql, false, 0);
        }
        return;
    }
    
    // Count term frequencies
    var term_freqs = std.StringHashMap(i32).init(allocator);
    defer term_freqs.deinit();
    
    for (tokens.items) |token| {
        const count = term_freqs.get(token) orelse 0;
        try term_freqs.put(token, count + 1);
    }
    
    const doc_length = @as(i32, @intCast(tokens.items.len));
    
    // Collect all unique terms with their data before doing SPI operations
    // This prevents memory corruption from SPI context changes
    const TermEntry = struct {
        term_copy: []u8,
        freq: i32,
        hash: i64,
    };
    var term_entries = std.ArrayList(TermEntry).empty;
    defer {
        for (term_entries.items) |e| {
            allocator.free(e.term_copy);
        }
        term_entries.deinit(allocator);
    }

    var it = term_freqs.iterator();
    while (it.next()) |entry| {
        const term = entry.key_ptr.*;
        const freq = entry.value_ptr.*;
        const term_hash = @as(i64, tokenizer.hashLexeme(term));
        
        // Copy term to stable memory
        const term_copy = try allocator.alloc(u8, term.len);
        @memcpy(term_copy, term);
        
        try term_entries.append(allocator, .{
            .term_copy = term_copy,
            .freq = freq,
            .hash = term_hash,
        });
    }
    
    // Now iterate the collected entries
    for (term_entries.items) |entry| {
        try updateInvertedIndex(table_id, entry.hash, entry.term_copy, doc_id, roaringbitmap_oid, entry.freq, config_name, allocator);
    }
    
    // Update document metadata (returns true if new document)
    const is_new_doc = try updateDocumentMetadata(table_id, doc_id, doc_length, config_name, allocator);
    
    // Update collection statistics incrementally (O(1) instead of O(n))
    try updateStatisticsIncremental(table_id, doc_length, is_new_doc, allocator);
    
    // ACID Compliance: Release savepoint on success (if it was created)
    if (savepoint_created) {
        const release_sql = "RELEASE SAVEPOINT bm25_index_doc";
        _ = c.SPI_execute(release_sql, false, 0);
    }
}

/// Update inverted index entry for a term using atomic upsert
/// This handles concurrent inserts properly using ON CONFLICT DO UPDATE
fn updateInvertedIndex(
    table_id: c.Oid,
    term_hash: i64,
    term_text: []const u8,
    doc_id: i64,
    roaringbitmap_oid: c.Oid,
    term_freq: i32,
    config_name: []const u8,
    allocator: std.mem.Allocator
) !void {
    _ = roaringbitmap_oid; // Not needed with SQL-based upsert
    
    // Copy term_text to stable memory before any SPI operations
    const term_text_copy = try allocator.alloc(u8, term_text.len);
    @memcpy(term_text_copy, term_text);
    
    // Create a null-terminated copy of term_text for cstring_to_text
    const term_text_z = try allocator.allocSentinel(u8, term_text_copy.len, 0);
    @memcpy(term_text_z, term_text_copy);
    const term_text_datum = c.PointerGetDatum(c.cstring_to_text(term_text_z.ptr));
    
    // Use a single atomic upsert query with ON CONFLICT DO UPDATE
    // This handles concurrent inserts properly by merging doc_ids and term_freqs
    const upsert_query = try std.fmt.allocPrintSentinel(
        allocator,
        \\INSERT INTO facets.bm25_index (table_id, term_hash, term_text, doc_ids, term_freqs, language)
        \\VALUES ({d}, {d}, $1, rb_build(ARRAY[{d}::int]), jsonb_build_object('{d}', {d}), '{s}')
        \\ON CONFLICT (table_id, term_hash) DO UPDATE SET
        \\    doc_ids = rb_or(facets.bm25_index.doc_ids, EXCLUDED.doc_ids),
        \\    term_freqs = facets.bm25_index.term_freqs || EXCLUDED.term_freqs
        ,
        .{ table_id, term_hash, doc_id, doc_id, term_freq, config_name },
        0
    );
    defer allocator.free(upsert_query);
    
    var argtypes = [_]c.Oid{c.TEXTOID};
    var argvalues = [_]c.Datum{term_text_datum};
    var argnulls = [_]u8{' '};
    
    const ret = c.SPI_execute_with_args(
        upsert_query.ptr,
        1,
        &argtypes,
        &argvalues,
        &argnulls,
        false, // not read_only
        0
    );
    
    if (ret != c.SPI_OK_INSERT and ret != c.SPI_OK_UPDATE) {
        utils.elog(c.ERROR, "Failed to upsert into inverted index");
        return error.UpsertFailed;
    }
}

/// Update document metadata
/// Returns true if this was a new document, false if it was an update
fn updateDocumentMetadata(
    table_id: c.Oid,
    doc_id: i64,
    doc_length: i32,
    config_name: []const u8,
    allocator: std.mem.Allocator
) !bool {
    // First check if document exists
    const check_query = try std.fmt.allocPrintSentinel(
        allocator,
        "SELECT 1 FROM facets.bm25_documents WHERE table_id = {d} AND doc_id = {d}",
        .{ table_id, doc_id }, 0);
    defer allocator.free(check_query);
    
    const check_ret = c.SPI_execute(check_query.ptr, true, 1);
    const is_new_doc = (check_ret == c.SPI_OK_SELECT and c.SPI_processed == 0);
    
    const query = try std.fmt.allocPrintSentinel(
        allocator,
        "INSERT INTO facets.bm25_documents (table_id, doc_id, doc_length, language) VALUES ({d}, {d}, {d}, '{s}') ON CONFLICT (table_id, doc_id) DO UPDATE SET doc_length = EXCLUDED.doc_length, updated_at = now()",
        .{ table_id, doc_id, doc_length, config_name },
        0
    );
    defer allocator.free(query);
    
    const ret = c.SPI_execute(query.ptr, false, 0);
    if (ret != c.SPI_OK_INSERT and ret != c.SPI_OK_UPDATE) {
        utils.elog(c.ERROR, "Failed to update document metadata");
        return error.UpdateFailed;
    }
    
    return is_new_doc;
}

/// Update collection statistics incrementally (O(1) instead of O(n))
/// This is much faster than recalculating from scratch for each document
fn updateStatisticsIncremental(
    table_id: c.Oid,
    new_doc_length: i32,
    is_new_doc: bool,
    allocator: std.mem.Allocator
) !void {
    if (is_new_doc) {
        // New document: increment count and update average incrementally
        // Formula: new_avg = (old_avg * old_count + new_length) / (old_count + 1)
        const update_query = try std.fmt.allocPrintSentinel(
            allocator,
            \\INSERT INTO facets.bm25_statistics (table_id, total_documents, avg_document_length)
            \\VALUES ({d}, 1, {d})
            \\ON CONFLICT (table_id) DO UPDATE SET
            \\    avg_document_length = (
            \\        facets.bm25_statistics.avg_document_length * facets.bm25_statistics.total_documents + {d}
            \\    )::float / (facets.bm25_statistics.total_documents + 1),
            \\    total_documents = facets.bm25_statistics.total_documents + 1,
            \\    last_updated = now()
            ,
            .{ table_id, new_doc_length, new_doc_length },
            0
        );
        defer allocator.free(update_query);
        
        const ret = c.SPI_execute(update_query.ptr, false, 0);
        if (ret != c.SPI_OK_INSERT and ret != c.SPI_OK_UPDATE) {
            utils.elog(c.ERROR, "Failed to update statistics incrementally");
            return error.UpdateFailed;
        }
    } else {
        // Existing document update: just update the average
        // This is an approximation - for exact values, use recalculateStatistics
        const update_query = try std.fmt.allocPrintSentinel(
            allocator,
            "UPDATE facets.bm25_statistics SET last_updated = now() WHERE table_id = {d}",
            .{table_id}, 0);
        defer allocator.free(update_query);
        _ = c.SPI_execute(update_query.ptr, false, 0);
    }
}

/// Recalculate statistics from scratch (use sparingly, e.g., after batch operations)
pub fn recalculateStatistics(
    table_id: c.Oid,
    allocator: std.mem.Allocator
) !void {
    if (c.SPI_connect() != c.SPI_OK_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed");
        return error.SPIConnectFailed;
    }
    defer _ = c.SPI_finish();
    
    try recalculateStatisticsInternal(table_id, allocator);
}

/// Internal version (assumes SPI is already connected)
fn recalculateStatisticsInternal(
    table_id: c.Oid,
    allocator: std.mem.Allocator
) !void {
    // Calculate new statistics from scratch
    const stats_query = try std.fmt.allocPrintSentinel(
        allocator,
        "SELECT COUNT(*)::bigint, COALESCE(AVG(doc_length), 0)::float FROM facets.bm25_documents WHERE table_id = {d}",
        .{table_id}, 0);
    defer allocator.free(stats_query);
    
    const ret = c.SPI_execute(stats_query.ptr, false, 1);
    if (ret != c.SPI_OK_SELECT) {
        utils.elog(c.ERROR, "Failed to calculate statistics");
        return error.QueryFailed;
    }
    
    if (c.SPI_processed > 0) {
        const tuple = c.SPI_tuptable.*.vals[0];
        const tupdesc = c.SPI_tuptable.*.tupdesc;
        
        var isnull1: bool = false;
        var isnull2: bool = false;
        const total_docs_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull1);
        const avg_len_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull2);
        
        const total_docs = if (!isnull1) c.DatumGetInt64(total_docs_datum) else 0;
        const avg_len = if (!isnull2) c.DatumGetFloat8(avg_len_datum) else 0.0;
        
        // Update statistics table
        const update_query = try std.fmt.allocPrintSentinel(
            allocator,
            "INSERT INTO facets.bm25_statistics (table_id, total_documents, avg_document_length) VALUES ({d}, {d}, {d}) ON CONFLICT (table_id) DO UPDATE SET total_documents = EXCLUDED.total_documents, avg_document_length = EXCLUDED.avg_document_length, last_updated = now()",
            .{ table_id, total_docs, avg_len }, 0);
        defer allocator.free(update_query);
        
        const update_ret = c.SPI_execute(update_query.ptr, false, 0);
        if (update_ret != c.SPI_OK_INSERT and update_ret != c.SPI_OK_UPDATE) {
            utils.elog(c.ERROR, "Failed to update statistics");
            return error.UpdateFailed;
        }
    }
}

/// Delete a document from the index
pub fn deleteDocument(
    table_id: c.Oid,
    doc_id: i64,
    allocator: std.mem.Allocator
) !void {
    if (c.SPI_connect() != c.SPI_OK_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed");
        return error.SPIConnectFailed;
    }
    defer _ = c.SPI_finish();
    
    // ACID Compliance: Create savepoint for atomic operation (graceful failure)
    var savepoint_created = false;
    const savepoint_sql = "SAVEPOINT bm25_delete_doc";
    const savepoint_ret = c.SPI_execute(savepoint_sql, false, 0);
    if (savepoint_ret == c.SPI_OK_UTILITY) {
        savepoint_created = true;
    } else {
        // Log debug message - outer transaction provides atomicity
        utils.elog(c.DEBUG1, "Could not create savepoint (may be in nested transaction), continuing without savepoint");
    }
    
    // On error path: rollback if savepoint exists
    errdefer {
        if (savepoint_created) {
            _ = c.SPI_execute("ROLLBACK TO SAVEPOINT bm25_delete_doc", false, 0);
        }
    }
    
    // Update bm25_index - remove doc from all term bitmaps (using SQL-only approach)
    const update_query = try std.fmt.allocPrintSentinel(
        allocator,
        "UPDATE facets.bm25_index SET doc_ids = rb_remove(doc_ids, {d}), term_freqs = term_freqs - '{d}' WHERE table_id = {d} AND rb_contains(doc_ids, {d})",
        .{ doc_id, doc_id, table_id, doc_id }, 0);
    defer allocator.free(update_query);
    
    _ = c.SPI_execute(update_query.ptr, false, 0);
    
    // Clean up empty terms
    const cleanup_query = try std.fmt.allocPrintSentinel(
        allocator,
        "DELETE FROM facets.bm25_index WHERE table_id = {d} AND rb_is_empty(doc_ids)",
        .{table_id}, 0);
    defer allocator.free(cleanup_query);
    
    _ = c.SPI_execute(cleanup_query.ptr, false, 0);
    
    // Delete document metadata
    const delete_doc_query = try std.fmt.allocPrintSentinel(
        allocator,
        "DELETE FROM facets.bm25_documents WHERE table_id = {d} AND doc_id = {d}",
        .{ table_id, doc_id }, 0);
    defer allocator.free(delete_doc_query);
    
    _ = c.SPI_execute(delete_doc_query.ptr, false, 0);
    
    // ACID Compliance: Release savepoint on success (if it was created)
    if (savepoint_created) {
        const release_sql = "RELEASE SAVEPOINT bm25_delete_doc";
        _ = c.SPI_execute(release_sql, false, 0);
    }
    
    // Note: Caller should call bm25_recalculate_statistics separately if needed
    // We skip it here to avoid nested SPI issues
}
