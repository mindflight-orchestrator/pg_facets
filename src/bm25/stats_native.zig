const std = @import("std");
const utils = @import("../utils.zig");
const c = utils.c;
const PgAllocator = utils.PgAllocator;

/// Term statistics entry
const TermStats = struct {
    term_hash: i64,
    term_text: []const u8,
    ndoc: i64,      // Number of documents containing this term
    nentry: i64,    // Total occurrences across all documents
};

/// Get BM25 term statistics for a table
/// Similar to ts_stat but for BM25 index
/// Returns: (term_text, ndoc, nentry) sorted by nentry desc
pub fn bm25_term_stats(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = PgAllocator.allocator();

    // Parse arguments
    // p_table_id oid
    if (utils.is_arg_null(fcinfo, 0)) {
        utils.elog(c.ERROR, "p_table_id cannot be null");
        return c.PointerGetDatum(null);
    }
    const table_id: c.Oid = @intCast(utils.get_arg_datum(fcinfo, 0));

    // p_limit int (default 100)
    var limit: i32 = 100;
    if (!utils.is_arg_null(fcinfo, 1)) {
        limit = @intCast(c.DatumGetInt32(utils.get_arg_datum(fcinfo, 1)));
    }

    // Connect to SPI
    const conn_result = c.SPI_connect();
    const need_finish = (conn_result == c.SPI_OK_CONNECT);
    if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed");
        return c.PointerGetDatum(null);
    }
    defer if (need_finish) {
        _ = c.SPI_finish();
    };

    // Query term statistics from BM25 index
    // ndoc = number of documents (cardinality of roaring bitmap)
    // nentry = sum of all term frequencies in term_freqs jsonb
    const query = std.fmt.allocPrintSentinel(allocator,
        \\SELECT 
        \\    term_text,
        \\    rb_cardinality(doc_ids)::bigint as ndoc,
        \\    (SELECT COALESCE(SUM(value::int), 0) FROM jsonb_each_text(term_freqs))::bigint as nentry
        \\FROM facets.bm25_index
        \\WHERE table_id = {d}
        \\ORDER BY nentry DESC, ndoc DESC, term_text
        \\LIMIT {d}
        , .{ table_id, limit }, 0) catch {
        utils.elog(c.ERROR, "Failed to allocate query");
        return c.PointerGetDatum(null);
    };
    defer allocator.free(query);

    const ret = c.SPI_execute(query.ptr, true, 0);

    // Set up ReturnSetInfo
    const rsi_ptr = @as(?*c.ReturnSetInfo, @alignCast(@ptrCast(fcinfo.*.resultinfo)));
    if (rsi_ptr == null) {
        utils.elog(c.ERROR, "set-valued function called in context that cannot accept a set");
        return 0;
    }
    const rsi = rsi_ptr.?;

    if ((rsi.allowedModes & c.SFRM_Materialize) == 0) {
        utils.elog(c.ERROR, "SRF materialize mode not allowed");
        return 0;
    }
    rsi.returnMode = c.SFRM_Materialize;

    const oldcontext = c.MemoryContextSwitchTo(rsi.econtext.?.*.ecxt_per_query_memory);
    rsi.setResult = c.tuplestore_begin_heap(true, false, utils.workMem());

    // Create tuple descriptor: (term_text text, ndoc bigint, nentry bigint)
    const tupdesc = c.CreateTemplateTupleDesc(3);
    _ = c.TupleDescInitEntry(tupdesc, 1, "term_text", c.TEXTOID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 2, "ndoc", c.INT8OID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 3, "nentry", c.INT8OID, -1, 0);
    rsi.setDesc = tupdesc;

    // Process results
    if (ret == c.SPI_OK_SELECT and c.SPI_processed > 0) {
        var i: u64 = 0;
        while (i < c.SPI_processed) : (i += 1) {
            const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
            const spi_tupdesc = c.SPI_tuptable.*.tupdesc;

            var isnull1: bool = false;
            var isnull2: bool = false;
            var isnull3: bool = false;

            const text_datum = c.SPI_getbinval(tuple, spi_tupdesc, 1, &isnull1);
            const ndoc_datum = c.SPI_getbinval(tuple, spi_tupdesc, 2, &isnull2);
            const nentry_datum = c.SPI_getbinval(tuple, spi_tupdesc, 3, &isnull3);

            if (isnull1) continue;

            var values = [_]c.Datum{
                text_datum,
                if (isnull2) c.Int64GetDatum(0) else ndoc_datum,
                if (isnull3) c.Int64GetDatum(0) else nentry_datum,
            };
            var nulls = [_]bool{ false, false, false };
            const result_tuple = c.heap_form_tuple(tupdesc, &values, &nulls);
            c.tuplestore_puttuple(rsi.setResult, result_tuple);
        }
    }

    _ = c.MemoryContextSwitchTo(oldcontext);

    return 0;
}

/// Get document statistics for a table
/// Returns: (doc_id, doc_length, term_count) for debugging
pub fn bm25_doc_stats(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = PgAllocator.allocator();

    // Parse arguments
    if (utils.is_arg_null(fcinfo, 0)) {
        utils.elog(c.ERROR, "p_table_id cannot be null");
        return c.PointerGetDatum(null);
    }
    const table_id: c.Oid = @intCast(utils.get_arg_datum(fcinfo, 0));

    // p_limit int (default 100)
    var limit: i32 = 100;
    if (!utils.is_arg_null(fcinfo, 1)) {
        limit = @intCast(c.DatumGetInt32(utils.get_arg_datum(fcinfo, 1)));
    }

    // Connect to SPI
    const conn_result = c.SPI_connect();
    const need_finish = (conn_result == c.SPI_OK_CONNECT);
    if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed");
        return c.PointerGetDatum(null);
    }
    defer if (need_finish) {
        _ = c.SPI_finish();
    };

    // Query document statistics
    const query = std.fmt.allocPrintSentinel(allocator,
        \\SELECT 
        \\    d.doc_id,
        \\    d.doc_length,
        \\    (SELECT COUNT(DISTINCT term_hash) FROM facets.bm25_index i 
        \\     WHERE i.table_id = d.table_id AND i.term_freqs ? d.doc_id::text)::int as unique_terms
        \\FROM facets.bm25_documents d
        \\WHERE d.table_id = {d}
        \\ORDER BY d.doc_length DESC
        \\LIMIT {d}
        , .{ table_id, limit }, 0) catch {
        utils.elog(c.ERROR, "Failed to allocate query");
        return c.PointerGetDatum(null);
    };
    defer allocator.free(query);

    const ret = c.SPI_execute(query.ptr, true, 0);

    // Set up ReturnSetInfo
    const rsi_ptr = @as(?*c.ReturnSetInfo, @alignCast(@ptrCast(fcinfo.*.resultinfo)));
    if (rsi_ptr == null) {
        utils.elog(c.ERROR, "set-valued function called in context that cannot accept a set");
        return 0;
    }
    const rsi = rsi_ptr.?;

    if ((rsi.allowedModes & c.SFRM_Materialize) == 0) {
        utils.elog(c.ERROR, "SRF materialize mode not allowed");
        return 0;
    }
    rsi.returnMode = c.SFRM_Materialize;

    const oldcontext = c.MemoryContextSwitchTo(rsi.econtext.?.*.ecxt_per_query_memory);
    rsi.setResult = c.tuplestore_begin_heap(true, false, utils.workMem());

    // Create tuple descriptor: (doc_id bigint, doc_length int, unique_terms int)
    const tupdesc = c.CreateTemplateTupleDesc(3);
    _ = c.TupleDescInitEntry(tupdesc, 1, "doc_id", c.INT8OID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 2, "doc_length", c.INT4OID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 3, "unique_terms", c.INT4OID, -1, 0);
    rsi.setDesc = tupdesc;

    // Process results
    if (ret == c.SPI_OK_SELECT and c.SPI_processed > 0) {
        var i: u64 = 0;
        while (i < c.SPI_processed) : (i += 1) {
            const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
            const spi_tupdesc = c.SPI_tuptable.*.tupdesc;

            var isnull1: bool = false;
            var isnull2: bool = false;
            var isnull3: bool = false;

            const docid_datum = c.SPI_getbinval(tuple, spi_tupdesc, 1, &isnull1);
            const doclen_datum = c.SPI_getbinval(tuple, spi_tupdesc, 2, &isnull2);
            const terms_datum = c.SPI_getbinval(tuple, spi_tupdesc, 3, &isnull3);

            var values = [_]c.Datum{
                if (isnull1) c.Int64GetDatum(0) else docid_datum,
                if (isnull2) c.Int32GetDatum(0) else doclen_datum,
                if (isnull3) c.Int32GetDatum(0) else terms_datum,
            };
            var nulls = [_]bool{ false, false, false };
            const result_tuple = c.heap_form_tuple(tupdesc, &values, &nulls);
            c.tuplestore_puttuple(rsi.setResult, result_tuple);
        }
    }

    _ = c.MemoryContextSwitchTo(oldcontext);

    return 0;
}

/// Analyze BM25 term distribution within a single document
/// Returns: (term_text, tf, df, idf, bm25_weight) for each term in the document
/// Useful for debugging why a document ranks high/low
pub fn bm25_explain_doc(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = PgAllocator.allocator();

    // Parse arguments
    if (utils.is_arg_null(fcinfo, 0)) {
        utils.elog(c.ERROR, "p_table_id cannot be null");
        return c.PointerGetDatum(null);
    }
    const table_id: c.Oid = @intCast(utils.get_arg_datum(fcinfo, 0));

    if (utils.is_arg_null(fcinfo, 1)) {
        utils.elog(c.ERROR, "p_doc_id cannot be null");
        return c.PointerGetDatum(null);
    }
    const doc_id: i64 = c.DatumGetInt64(utils.get_arg_datum(fcinfo, 1));

    // BM25 parameters
    var k1: f64 = 1.2;
    var b: f64 = 0.75;
    if (!utils.is_arg_null(fcinfo, 2)) {
        k1 = c.DatumGetFloat8(utils.get_arg_datum(fcinfo, 2));
    }
    if (!utils.is_arg_null(fcinfo, 3)) {
        b = c.DatumGetFloat8(utils.get_arg_datum(fcinfo, 3));
    }

    // Connect to SPI
    const conn_result = c.SPI_connect();
    const need_finish = (conn_result == c.SPI_OK_CONNECT);
    if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed");
        return c.PointerGetDatum(null);
    }
    defer if (need_finish) {
        _ = c.SPI_finish();
    };

    // Get collection stats first
    var stats_query_buf: [256]u8 = undefined;
    const stats_query = std.fmt.bufPrintZ(&stats_query_buf,
        "SELECT total_documents, avg_document_length FROM facets.bm25_statistics WHERE table_id = {d}",
        .{table_id}) catch {
        utils.elog(c.ERROR, "Buffer too small");
        return c.PointerGetDatum(null);
    };

    var total_docs: f64 = 0;
    var avgdl: f64 = 1.0;

    var ret = c.SPI_execute(stats_query.ptr, true, 1);
    if (ret == c.SPI_OK_SELECT and c.SPI_processed > 0) {
        var isnull1: bool = false;
        var isnull2: bool = false;
        const tuple = c.SPI_tuptable.*.vals[0];
        const tupdesc = c.SPI_tuptable.*.tupdesc;
        const td_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull1);
        const ad_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull2);
        if (!isnull1) total_docs = @floatFromInt(c.DatumGetInt64(td_datum));
        if (!isnull2) avgdl = c.DatumGetFloat8(ad_datum);
    }

    // Get document length
    var doc_len_query_buf: [256]u8 = undefined;
    const doc_len_query = std.fmt.bufPrintZ(&doc_len_query_buf,
        "SELECT doc_length FROM facets.bm25_documents WHERE table_id = {d} AND doc_id = {d}",
        .{ table_id, doc_id }) catch {
        utils.elog(c.ERROR, "Buffer too small");
        return c.PointerGetDatum(null);
    };

    var doc_length: f64 = 0;
    ret = c.SPI_execute(doc_len_query.ptr, true, 1);
    if (ret == c.SPI_OK_SELECT and c.SPI_processed > 0) {
        var isnull: bool = false;
        const tuple = c.SPI_tuptable.*.vals[0];
        const tupdesc = c.SPI_tuptable.*.tupdesc;
        const dl_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);
        if (!isnull) doc_length = @floatFromInt(c.DatumGetInt32(dl_datum));
    }

    // Query all terms for this document with their stats
    const query = std.fmt.allocPrintSentinel(allocator,
        \\SELECT 
        \\    i.term_text,
        \\    (i.term_freqs->>'{d}')::int as tf,
        \\    rb_cardinality(i.doc_ids)::bigint as df
        \\FROM facets.bm25_index i
        \\WHERE i.table_id = {d}
        \\  AND i.term_freqs ? '{d}'
        \\ORDER BY tf DESC, i.term_text
        , .{ doc_id, table_id, doc_id }, 0) catch {
        utils.elog(c.ERROR, "Failed to allocate query");
        return c.PointerGetDatum(null);
    };
    defer allocator.free(query);

    ret = c.SPI_execute(query.ptr, true, 0);

    // Set up ReturnSetInfo
    const rsi_ptr = @as(?*c.ReturnSetInfo, @alignCast(@ptrCast(fcinfo.*.resultinfo)));
    if (rsi_ptr == null) {
        utils.elog(c.ERROR, "set-valued function called in context that cannot accept a set");
        return 0;
    }
    const rsi = rsi_ptr.?;

    if ((rsi.allowedModes & c.SFRM_Materialize) == 0) {
        utils.elog(c.ERROR, "SRF materialize mode not allowed");
        return 0;
    }
    rsi.returnMode = c.SFRM_Materialize;

    const oldcontext = c.MemoryContextSwitchTo(rsi.econtext.?.*.ecxt_per_query_memory);
    rsi.setResult = c.tuplestore_begin_heap(true, false, utils.workMem());

    // Create tuple descriptor: (term_text, tf, df, idf, bm25_weight)
    const tupdesc = c.CreateTemplateTupleDesc(5);
    _ = c.TupleDescInitEntry(tupdesc, 1, "term_text", c.TEXTOID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 2, "tf", c.INT4OID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 3, "df", c.INT8OID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 4, "idf", c.FLOAT8OID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 5, "bm25_weight", c.FLOAT8OID, -1, 0);
    rsi.setDesc = tupdesc;

    // Process results and calculate BM25 weights
    if (ret == c.SPI_OK_SELECT and c.SPI_processed > 0) {
        var i: u64 = 0;
        while (i < c.SPI_processed) : (i += 1) {
            const tuple = c.SPI_tuptable.*.vals[@intCast(i)];
            const spi_tupdesc = c.SPI_tuptable.*.tupdesc;

            var isnull1: bool = false;
            var isnull2: bool = false;
            var isnull3: bool = false;

            const text_datum = c.SPI_getbinval(tuple, spi_tupdesc, 1, &isnull1);
            const tf_datum = c.SPI_getbinval(tuple, spi_tupdesc, 2, &isnull2);
            const df_datum = c.SPI_getbinval(tuple, spi_tupdesc, 3, &isnull3);

            if (isnull1 or isnull2 or isnull3) continue;

            const tf: f64 = @floatFromInt(c.DatumGetInt32(tf_datum));
            const df: f64 = @floatFromInt(c.DatumGetInt64(df_datum));

            // Calculate IDF: log((N + 1) / (df + 0.5))
            const idf: f64 = @log((total_docs + 1.0) / (df + 0.5));

            // Calculate BM25 weight for this term in this document
            // weight = IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (dl / avgdl)))
            const numerator = tf * (k1 + 1.0);
            const denominator = tf + k1 * (1.0 - b + b * (doc_length / avgdl));
            const bm25_weight = idf * (numerator / denominator);

            var values = [_]c.Datum{
                text_datum,
                tf_datum,
                df_datum,
                c.Float8GetDatum(idf),
                c.Float8GetDatum(bm25_weight),
            };
            var nulls = [_]bool{ false, false, false, false, false };
            const result_tuple = c.heap_form_tuple(tupdesc, &values, &nulls);
            c.tuplestore_puttuple(rsi.setResult, result_tuple);
        }
    }

    _ = c.MemoryContextSwitchTo(oldcontext);

    return 0;
}

/// Get collection-wide statistics
/// Returns: (total_documents, avg_document_length, total_terms, unique_terms)
pub fn bm25_collection_stats(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = PgAllocator.allocator();

    // Parse arguments
    if (utils.is_arg_null(fcinfo, 0)) {
        utils.elog(c.ERROR, "p_table_id cannot be null");
        return c.PointerGetDatum(null);
    }
    const table_id: c.Oid = @intCast(utils.get_arg_datum(fcinfo, 0));

    // Connect to SPI
    const conn_result = c.SPI_connect();
    const need_finish = (conn_result == c.SPI_OK_CONNECT);
    if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed");
        return c.PointerGetDatum(null);
    }
    defer if (need_finish) {
        _ = c.SPI_finish();
    };

    // Query collection statistics
    const query = std.fmt.allocPrintSentinel(allocator,
        \\SELECT 
        \\    s.total_documents,
        \\    s.avg_document_length,
        \\    (SELECT SUM(doc_length) FROM facets.bm25_documents WHERE table_id = {d})::bigint as total_terms,
        \\    (SELECT COUNT(*) FROM facets.bm25_index WHERE table_id = {d})::bigint as unique_terms
        \\FROM facets.bm25_statistics s
        \\WHERE s.table_id = {d}
        , .{ table_id, table_id, table_id }, 0) catch {
        utils.elog(c.ERROR, "Failed to allocate query");
        return c.PointerGetDatum(null);
    };
    defer allocator.free(query);

    const ret = c.SPI_execute(query.ptr, true, 1);

    // Set up ReturnSetInfo
    const rsi_ptr = @as(?*c.ReturnSetInfo, @alignCast(@ptrCast(fcinfo.*.resultinfo)));
    if (rsi_ptr == null) {
        utils.elog(c.ERROR, "set-valued function called in context that cannot accept a set");
        return 0;
    }
    const rsi = rsi_ptr.?;

    if ((rsi.allowedModes & c.SFRM_Materialize) == 0) {
        utils.elog(c.ERROR, "SRF materialize mode not allowed");
        return 0;
    }
    rsi.returnMode = c.SFRM_Materialize;

    const oldcontext = c.MemoryContextSwitchTo(rsi.econtext.?.*.ecxt_per_query_memory);
    rsi.setResult = c.tuplestore_begin_heap(true, false, utils.workMem());

    // Create tuple descriptor
    const tupdesc = c.CreateTemplateTupleDesc(4);
    _ = c.TupleDescInitEntry(tupdesc, 1, "total_documents", c.INT8OID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 2, "avg_document_length", c.FLOAT8OID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 3, "total_terms", c.INT8OID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 4, "unique_terms", c.INT8OID, -1, 0);
    rsi.setDesc = tupdesc;

    // Process results
    if (ret == c.SPI_OK_SELECT and c.SPI_processed > 0) {
        const tuple = c.SPI_tuptable.*.vals[0];
        const spi_tupdesc = c.SPI_tuptable.*.tupdesc;

        var isnull1: bool = false;
        var isnull2: bool = false;
        var isnull3: bool = false;
        var isnull4: bool = false;

        const total_docs_datum = c.SPI_getbinval(tuple, spi_tupdesc, 1, &isnull1);
        const avg_len_datum = c.SPI_getbinval(tuple, spi_tupdesc, 2, &isnull2);
        const total_terms_datum = c.SPI_getbinval(tuple, spi_tupdesc, 3, &isnull3);
        const unique_terms_datum = c.SPI_getbinval(tuple, spi_tupdesc, 4, &isnull4);

        var values = [_]c.Datum{
            if (isnull1) c.Int64GetDatum(0) else total_docs_datum,
            if (isnull2) c.Float8GetDatum(0.0) else avg_len_datum,
            if (isnull3) c.Int64GetDatum(0) else total_terms_datum,
            if (isnull4) c.Int64GetDatum(0) else unique_terms_datum,
        };
        var nulls = [_]bool{ false, false, false, false };
        const result_tuple = c.heap_form_tuple(tupdesc, &values, &nulls);
        c.tuplestore_puttuple(rsi.setResult, result_tuple);
    }

    _ = c.MemoryContextSwitchTo(oldcontext);

    return 0;
}

