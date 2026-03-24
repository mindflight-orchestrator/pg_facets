const std = @import("std");
const utils = @import("../utils.zig");
const c = utils.c;
const PgAllocator = utils.PgAllocator;
const tokenizer_native = @import("tokenizer_native.zig");
const avx2_utils = @import("avx2_utils.zig");
const copy_binary = @import("copy_binary.zig");

/// Buffer threshold for COPY BINARY flush (50MB)
const COPY_BUFFER_THRESHOLD: usize = 50 * 1024 * 1024;

/// Term batch entry for efficient batch inserts (kept for compatibility)
const TermBatchEntry = struct {
    term_hash: i64,
    term_text: []u8,
    doc_id: i64,
    term_freq: i32,
    doc_length: i32,
};

/// Native Zig worker function for parallel BM25 indexing
/// This replaces the SQL-based bm25_index_worker_lockfree with direct C API calls
pub fn bm25_index_worker_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = PgAllocator.allocator();
    
    // Parse arguments
    // p_table_id oid
    if (utils.is_arg_null(fcinfo, 0)) {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "p_table_id cannot be null");
        return c.PointerGetDatum(null);
    }
    const table_id_datum = utils.get_arg_datum(fcinfo, 0);
    const table_id: c.Oid = @intCast(table_id_datum);
    _ = table_id; // Reserved for future use (e.g., table-specific config)
    
    // p_source_staging text
    if (utils.is_arg_null(fcinfo, 1)) {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "p_source_staging cannot be null");
        return c.PointerGetDatum(null);
    }
    const source_staging_datum = utils.get_arg_datum(fcinfo, 1);
    const source_staging_cstr = utils.textToCstring(source_staging_datum);
    if (source_staging_cstr == null) {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "Failed to extract source_staging");
        return c.PointerGetDatum(null);
    }
    defer c.pfree(@ptrCast(source_staging_cstr));
    // Use bounded strlen with max check (table names should be < 256 bytes)
    var source_staging_len: usize = 0;
    const max_table_name_len: usize = 256;
    while (source_staging_len < max_table_name_len and source_staging_cstr[source_staging_len] != 0) {
        source_staging_len += 1;
    }
    if (source_staging_len >= max_table_name_len) {
        utils.elogFmt(c.ERROR,
            "bm25_index_worker_native: source_staging table name too long or not null-terminated (length >= {d} bytes, max {d} bytes). This may indicate corrupted data or invalid table name.",
            .{ source_staging_len, max_table_name_len });
        return c.PointerGetDatum(null);
    }
    const source_staging = allocator.alloc(u8, source_staging_len) catch {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "Failed to allocate source_staging buffer");
        return c.PointerGetDatum(null);
    };
    defer allocator.free(source_staging);
    @memcpy(source_staging, source_staging_cstr[0..source_staging_len]);
    
    // p_output_staging text
    if (utils.is_arg_null(fcinfo, 2)) {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "p_output_staging cannot be null");
        return c.PointerGetDatum(null);
    }
    const output_staging_datum = utils.get_arg_datum(fcinfo, 2);
    const output_staging_cstr = utils.textToCstring(output_staging_datum);
    if (output_staging_cstr == null) {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "Failed to extract output_staging");
        return c.PointerGetDatum(null);
    }
    defer c.pfree(@ptrCast(output_staging_cstr));
    // Use bounded strlen with max check (table names should be < 256 bytes)
    var output_staging_len: usize = 0;
    const max_table_name_len2: usize = 256;
    while (output_staging_len < max_table_name_len2 and output_staging_cstr[output_staging_len] != 0) {
        output_staging_len += 1;
    }
    if (output_staging_len >= max_table_name_len2) {
        utils.elogFmt(c.ERROR, 
            "bm25_index_worker_native: output_staging table name too long or not null-terminated (length >= {d} bytes, max {d} bytes). This may indicate corrupted data or invalid table name.", 
            .{ output_staging_len, max_table_name_len2 });
        return c.PointerGetDatum(null);
    }
    const output_staging = allocator.alloc(u8, output_staging_len) catch {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "Failed to allocate output_staging buffer");
        return c.PointerGetDatum(null);
    };
    defer allocator.free(output_staging);
    @memcpy(output_staging, output_staging_cstr[0..output_staging_len]);
    
    // p_language text
    if (utils.is_arg_null(fcinfo, 3)) {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "p_language cannot be null");
        return c.PointerGetDatum(null);
    }
    const language_datum = utils.get_arg_datum(fcinfo, 3);
    const language_cstr = utils.textToCstring(language_datum);
    if (language_cstr == null) {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "Failed to extract language");
        return c.PointerGetDatum(null);
    }
    defer c.pfree(@ptrCast(language_cstr));
    // Use bounded strlen with max check (language names should be < 64 bytes)
    var language_len: usize = 0;
    const max_language_len: usize = 64;
    while (language_len < max_language_len and language_cstr[language_len] != 0) {
        language_len += 1;
    }
    if (language_len >= max_language_len) {
        utils.elogFmt(c.ERROR, 
            "bm25_index_worker_native: language string too long or not null-terminated (length >= {d} bytes, max {d} bytes). Language names should be short (e.g., 'english', 'spanish').", 
            .{ language_len, max_language_len });
        return c.PointerGetDatum(null);
    }
    const language = allocator.alloc(u8, language_len) catch {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "Failed to allocate language buffer");
        return c.PointerGetDatum(null);
    };
    defer allocator.free(language);
    @memcpy(language, language_cstr[0..language_len]);
    
    // p_total_docs bigint
    if (utils.is_arg_null(fcinfo, 4)) {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "p_total_docs cannot be null");
        return c.PointerGetDatum(null);
    }
    const total_docs_datum = utils.get_arg_datum(fcinfo, 4);
    const total_docs: i64 = @intCast(c.DatumGetInt64(total_docs_datum));
    
    // p_num_workers int
    if (utils.is_arg_null(fcinfo, 5)) {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "p_num_workers cannot be null");
        return c.PointerGetDatum(null);
    }
    const num_workers_datum = utils.get_arg_datum(fcinfo, 5);
    const num_workers: i32 = @intCast(c.DatumGetInt32(num_workers_datum));
    
    // p_worker_id int
    if (utils.is_arg_null(fcinfo, 6)) {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "p_worker_id cannot be null");
        return c.PointerGetDatum(null);
    }
    const worker_id_datum = utils.get_arg_datum(fcinfo, 6);
    const worker_id: i32 = @intCast(c.DatumGetInt32(worker_id_datum));
    
    // Connect to SPI
    if (c.SPI_connect() != c.SPI_OK_CONNECT) {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "SPI_connect failed");
        return c.PointerGetDatum(null);
    }
    defer _ = c.SPI_finish();
    
    const start_time = std.time.milliTimestamp();
    
    // Calculate row number range for this worker (even distribution)
    const base_docs: i64 = @divTrunc(total_docs, @as(i64, @intCast(num_workers)));
    const remainder: i32 = @intCast(@rem(total_docs, @as(i64, @intCast(num_workers))));
    
    var start_rn: i64 = undefined;
    var docs_for_this_worker: i64 = undefined;
    
    if (worker_id <= remainder) {
        docs_for_this_worker = base_docs + 1;
        start_rn = (@as(i64, @intCast(worker_id)) - 1) * (base_docs + 1) + 1;
    } else {
        docs_for_this_worker = base_docs;
        start_rn = @as(i64, @intCast(remainder)) * (base_docs + 1) + (@as(i64, @intCast(worker_id)) - @as(i64, @intCast(remainder)) - 1) * base_docs + 1;
    }
    
    const end_rn = start_rn + docs_for_this_worker - 1;
    
    // Skip if no documents for this worker
    if (start_rn > total_docs or end_rn < start_rn) {
        // Return empty result set
        const rsi_ptr = @as(?*c.ReturnSetInfo, @alignCast(@ptrCast(fcinfo.*.resultinfo)));
        if (rsi_ptr) |rsi| {
            if ((rsi.allowedModes & c.SFRM_Materialize) != 0) {
                rsi.returnMode = c.SFRM_Materialize;
                const oldcontext = c.MemoryContextSwitchTo(rsi.econtext.?.*.ecxt_per_query_memory);
                rsi.setResult = c.tuplestore_begin_heap(true, false, utils.workMem());
                const tupdesc = c.CreateTemplateTupleDesc(3);
                _ = c.TupleDescInitEntry(tupdesc, 1, "docs_indexed", c.INT4OID, -1, 0);
                _ = c.TupleDescInitEntry(tupdesc, 2, "terms_extracted", c.INT8OID, -1, 0);
                _ = c.TupleDescInitEntry(tupdesc, 3, "elapsed_ms", c.FLOAT8OID, -1, 0);
                rsi.setDesc = tupdesc;
                var values = [_]c.Datum{ c.Int32GetDatum(0), c.Int64GetDatum(0), c.Float8GetDatum(0.0) };
                var nulls = [_]bool{ false, false, false };
                const tuple = c.heap_form_tuple(tupdesc, &values, &nulls);
                c.tuplestore_puttuple(rsi.setResult, tuple);
                _ = c.MemoryContextSwitchTo(oldcontext);
            }
        }
        return 0;
    }
    
    // Read documents from source staging table
    // Build query with proper identifier quoting
    const query_fmt = "SELECT doc_id, content FROM facets.{s} WHERE rn BETWEEN {d} AND {d} ORDER BY rn";
    const query = std.fmt.allocPrintSentinel(
        allocator,
        query_fmt,
        .{ source_staging, start_rn, end_rn }, 0) catch {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "Failed to allocate query string");
        return 0;
    };
    defer allocator.free(query);
    
    const ret = c.SPI_execute(query.ptr, false, 0);
    if (ret != c.SPI_OK_SELECT) {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "Failed to read from source staging table");
        return c.PointerGetDatum(null);
    }
    
    var docs_indexed: i32 = 0;
    var terms_extracted: i64 = 0;
    var rows_copied: usize = 0;
    
    // Initialize COPY BINARY writer for fast bulk loading
    // 5 fields: term_hash, term_text, doc_id, term_freq, doc_length
    var copy_writer = copy_binary.CopyBinaryWriter.init(allocator, 5);
    // Don't defer deinit - memory context will clean up
    
    // Process each document
    var i: u64 = 0;
    while (i < c.SPI_processed) : (i += 1) {
        const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
        const tupdesc = c.SPI_tuptable.*.tupdesc;
        
        // Get doc_id
        var isnull_doc_id: bool = false;
        const doc_id_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull_doc_id);
        if (isnull_doc_id) continue;
        const doc_id: i64 = @intCast(c.DatumGetInt64(doc_id_datum));
        
        // Get content
        var isnull_content: bool = false;
        const content_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull_content);
        if (isnull_content) continue;
        
        const content_cstr = utils.textToCstring(content_datum);
        if (content_cstr == null) continue;
        // Don't pfree - memory context handles it
        
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
        if (content_len >= max_content_len) {
            // Document exceeds limit - truncate to max_content_len with UTF-8 safety
            // First, get the raw bytes up to max_content_len
            const raw_slice = content_cstr[0..max_content_len];
            // Find safe UTF-8 truncation point
            content_len = utils.truncateUtf8Safe(raw_slice, max_content_len);
            utils.elogFmt(c.WARNING, 
                "bm25_index_worker_native: Document content exceeds maximum size for doc_id={d}. Truncating to {d} bytes (UTF-8 safe).", 
                .{ doc_id, content_len });
        }
        if (content_len == 0) continue;
        
        const content = allocator.alloc(u8, content_len) catch continue;
        // Don't defer free - memory context handles it
        // Safe copy: content_len is guaranteed to be <= max_content_len and within bounds, UTF-8 safe
        avx2_utils.fastMemcpy(content, content_cstr[0..content_len]);
        
        // Tokenize using native function
        // Use helper that assumes SPI is already connected to avoid nested connections
        const tokens = tokenizer_native.tokenizeNativeWithExistingConnection(content, language, allocator) catch continue;
        // Don't defer free - memory context handles it
        
        if (tokens.items.len == 0) continue;
        
        // Calculate document length (sum of all term frequencies)
        var doc_length: i32 = 0;
        for (tokens.items) |token| {
            doc_length += token.freq;
            terms_extracted += 1;
        }
        
        // Write terms to COPY BINARY buffer
        for (tokens.items) |token| {
            const term_hash = tokenizer_native.hashLexeme(token.lexeme);
            
            copy_writer.writeTermRow(
                term_hash,
                token.lexeme,
                doc_id,
                token.freq,
                doc_length,
            ) catch continue;
            
            // Flush when buffer exceeds threshold (50MB)
            if (copy_writer.bufferSize() >= COPY_BUFFER_THRESHOLD) {
                const flushed = flushCopyBinary(&copy_writer, output_staging, allocator) catch {
                    utils.elog(c.WARNING, "Failed to flush COPY BINARY buffer");
                    continue;
                };
                rows_copied += flushed;
                copy_writer.reset();
            }
        }
        
        docs_indexed += 1;
    }
    
    // Flush remaining data
    if (copy_writer.getRowCount() > 0) {
        if (flushCopyBinary(&copy_writer, output_staging, allocator)) |flushed| {
            rows_copied += flushed;
        } else |_| {
            utils.elog(c.WARNING, "Failed to flush final COPY BINARY buffer");
        }
    }
    
    const elapsed_ms: f64 = @as(f64, @floatFromInt(std.time.milliTimestamp() - start_time));
    
    // Set up ReturnSetInfo for table-returning function
    const rsi_ptr = @as(?*c.ReturnSetInfo, @alignCast(@ptrCast(fcinfo.*.resultinfo)));
    if (rsi_ptr == null) {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "set-valued function called in context that cannot accept a set");
        return 0;
    }
    const rsi = rsi_ptr.?;
    
    // Use Materialize mode
    if ((rsi.allowedModes & c.SFRM_Materialize) == 0) {
        utils.elogWithContext(c.ERROR, "bm25_index_worker_native", "SRF materialize mode not allowed in this context");
        return 0;
    }
    rsi.returnMode = c.SFRM_Materialize;
    
    const oldcontext = c.MemoryContextSwitchTo(rsi.econtext.?.*.ecxt_per_query_memory);
    rsi.setResult = c.tuplestore_begin_heap(true, false, utils.workMem());
    
    // Create tuple descriptor
    const tupdesc = c.CreateTemplateTupleDesc(3);
    _ = c.TupleDescInitEntry(tupdesc, 1, "docs_indexed", c.INT4OID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 2, "terms_extracted", c.INT8OID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 3, "elapsed_ms", c.FLOAT8OID, -1, 0);
    rsi.setDesc = tupdesc;
    
    // Create and store tuple
    var values = [_]c.Datum{
        c.Int32GetDatum(docs_indexed),
        c.Int64GetDatum(terms_extracted),
        c.Float8GetDatum(elapsed_ms),
    };
    var nulls = [_]bool{ false, false, false };
    const tuple = c.heap_form_tuple(tupdesc, &values, &nulls);
    c.tuplestore_puttuple(rsi.setResult, tuple);
    
    _ = c.MemoryContextSwitchTo(oldcontext);
    
    return 0; // Return 0 for SRF
}

/// Flush COPY BINARY buffer to PostgreSQL via temp file
/// COPY BINARY is the fastest bulk loading method (5-10x faster than multi-value INSERT)
fn flushCopyBinary(
    writer: *copy_binary.CopyBinaryWriter,
    output_staging: []const u8,
    allocator: std.mem.Allocator,
) !usize {
    if (writer.getRowCount() == 0) return 0;

    // Finalize the binary buffer
    const data = try writer.finalize();

    // Create temp file path
    const pid = std.os.linux.getpid();
    const timestamp = std.time.milliTimestamp();
    const temp_path = try std.fmt.allocPrintSentinel(
        allocator,
        "/tmp/pg_facets_copy_{d}_{d}.bin",
        .{ pid, timestamp },
        0,
    );
    defer allocator.free(temp_path);

    // Write binary data to temp file
    const file = std.fs.createFileAbsolute(temp_path, .{}) catch {
        utils.elog(c.WARNING, "Failed to create temp file for COPY BINARY");
        return error.TempFileError;
    };
    defer file.close();
    
    file.writeAll(data) catch {
        utils.elog(c.WARNING, "Failed to write COPY BINARY data to temp file");
        return error.TempFileError;
    };

    // Build COPY command
    const copy_cmd = try std.fmt.allocPrintSentinel(
        allocator,
        "COPY facets.\"{s}\" (term_hash, term_text, doc_id, term_freq, doc_length) FROM '{s}' WITH (FORMAT BINARY)",
        .{ output_staging, temp_path },
        0,
    );
    defer allocator.free(copy_cmd);

    // Execute COPY
    const ret = c.SPI_execute(copy_cmd.ptr, false, 0);

    // Clean up temp file
    std.fs.deleteFileAbsolute(temp_path) catch {};

    if (ret != c.SPI_OK_UTILITY) {
        utils.elog(c.WARNING, "COPY BINARY command failed");
        return error.CopyFailed;
    }

    return writer.getRowCount();
}

