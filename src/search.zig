const std = @import("std");
const utils = @import("utils.zig");
const c = utils.c;
const PgAllocator = utils.PgAllocator;
const filters = @import("filters.zig");

pub fn search_documents_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = PgAllocator.allocator();

    // args[0]: table_id (oid)
    const table_id_datum = utils.get_arg_datum(fcinfo, 0);
    const table_id: c.Oid = @intCast(table_id_datum);

    // args[1]: filters array (nullable)
    // args[2]: limit (int)
    // args[3]: offset (int)

    const limit: i32 = if (!utils.is_arg_null(fcinfo, 2)) @intCast(c.DatumGetInt32(utils.get_arg_datum(fcinfo, 2))) else 100;
    const offset: i32 = if (!utils.is_arg_null(fcinfo, 3)) @intCast(c.DatumGetInt32(utils.get_arg_datum(fcinfo, 3))) else 0;

    // Parse filters
    var filter_bitmap: ?*c.roaring_bitmap_t = null;

    if (!utils.is_arg_null(fcinfo, 1)) {
        const datum = utils.get_arg_datum(fcinfo, 1);
        const filters_array = @as(*c.ArrayType, @ptrCast(@alignCast(utils.detoast_datum(datum))));
        var parsed = filters.parse_filters(allocator, filters_array) catch |err| {
            if (err == error.OutOfMemory) utils.elog(c.ERROR, "OutOfMemory");
            return c.PointerGetDatum(null);
        };
        defer parsed.deinit(allocator);

        if (parsed.items.len > 0) {
            // We need to build the bitmap.
            if (c.SPI_connect() != c.SPI_OK_CONNECT) {
                utils.elog(c.ERROR, "SPI_connect failed");
            }
            defer _ = c.SPI_finish();

            // Get table info INCLUDING chunk_bits for ID reconstruction
            const table_info_query = std.fmt.allocPrintSentinel(allocator, "SELECT schemaname, facets_table, chunk_bits FROM facets.faceted_table WHERE table_id = {d}", .{table_id}, 0) catch unreachable;
            if (c.SPI_execute(table_info_query.ptr, true, 1) != c.SPI_OK_SELECT or c.SPI_processed != 1) {
                utils.elog(c.ERROR, "Table not found in facets.faceted_table");
            }

            const schema_name = c.SPI_getvalue(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 1);
            const facets_table = c.SPI_getvalue(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 2);
            var isnull_chunk_bits: bool = false;
            const chunk_bits_datum = c.SPI_getbinval(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 3, &isnull_chunk_bits);
            const chunk_bits: u5 = if (isnull_chunk_bits) 20 else @intCast(c.DatumGetInt32(chunk_bits_datum));

            // Resolve facet IDs
            var facets_map = std.StringHashMap(std.ArrayList([]const u8)).init(allocator);
            defer facets_map.deinit();

            for (parsed.items) |entry| {
                const res = facets_map.getOrPut(entry.facet_name) catch unreachable;
                if (!res.found_existing) {
                    res.value_ptr.* = std.ArrayList([]const u8).empty;
                }
                res.value_ptr.append(allocator, entry.facet_value) catch unreachable;
            }

            var it = facets_map.iterator();
            while (it.next()) |entry| {
                const facet_name = entry.key_ptr.*;
                const values = entry.value_ptr.*;

                const id_query = std.fmt.allocPrintSentinel(allocator, "SELECT facet_id FROM facets.facet_definition WHERE table_id = {d} AND facet_name = $1", .{table_id}, 0) catch unreachable;
                var id_argtypes = [_]c.Oid{c.TEXTOID};
                var id_values = [_]c.Datum{c.PointerGetDatum(c.cstring_to_text_with_len(facet_name.ptr, @intCast(facet_name.len)))};
                var id_nulls = [_]u8{' '};

                if (c.SPI_execute_with_args(id_query.ptr, 1, &id_argtypes, &id_values, &id_nulls, true, 1) != c.SPI_OK_SELECT) continue;
                if (c.SPI_processed == 0) {
                    if (filter_bitmap != null) c.roaring_bitmap_free(filter_bitmap);
                    filter_bitmap = null;
                    break;
                }

                var isnull_facet_id: bool = false;
                const facet_id_datum = c.SPI_getbinval(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 1, &isnull_facet_id);
                const facet_id = c.DatumGetInt32(facet_id_datum);

                // Helper to construct array
                const datums = allocator.alloc(c.Datum, values.items.len) catch unreachable;
                for (values.items, 0..) |s, i| {
                    datums[i] = c.PointerGetDatum(c.cstring_to_text_with_len(s.ptr, @intCast(s.len)));
                }
                const val_arr = c.construct_array(datums.ptr, @intCast(values.items.len), c.TEXTOID, -1, false, 'i');
                const val_arr_datum = c.PointerGetDatum(val_arr);

                // IMPORTANT: Also select chunk_id to reconstruct original IDs
                const bitmap_query = std.fmt.allocPrintSentinel(allocator, "SELECT chunk_id, postinglist FROM \"{s}\".\"{s}\" WHERE facet_id = {d} AND facet_value = ANY($1)", .{ schema_name, facets_table, facet_id }, 0) catch unreachable;

                var b_argtypes = [_]c.Oid{c.TEXTARRAYOID};
                var b_values = [_]c.Datum{val_arr_datum};
                var b_nulls = [_]u8{' '};

                if (c.SPI_execute_with_args(bitmap_query.ptr, 1, &b_argtypes, &b_values, &b_nulls, true, 0) != c.SPI_OK_SELECT) continue;

                // Build bitmap with RECONSTRUCTED original IDs
                var facet_bitmap: ?*c.roaring_bitmap_t = null;
                const proc = c.SPI_processed;
                var k: u64 = 0;
                while (k < proc) : (k += 1) {
                    const tuple = c.SPI_tuptable.*.vals[k];
                    const desc = c.SPI_tuptable.*.tupdesc;

                    // Get chunk_id (column 1)
                    var isnull_chunk_id: bool = false;
                    const chunk_id_datum = c.SPI_getbinval(tuple, desc, 1, &isnull_chunk_id);
                    const chunk_id: u32 = @intCast(c.DatumGetInt32(chunk_id_datum));

                    // Get postinglist (column 2)
                    var isnull_p: bool = false;
                    const p_datum = c.SPI_getbinval(tuple, desc, 2, &isnull_p);
                    const p_ptr = utils.detoast_datum((p_datum));
                    const p_len = utils.varsize(p_ptr) - utils.varhdrsz();
                    const p_data = utils.vardata(p_ptr);
                    const bm = c.roaring_bitmap_portable_deserialize_safe(p_data, @intCast(p_len));
                    defer c.roaring_bitmap_free(bm);

                    // Iterate through postinglist and reconstruct original IDs
                    var pl_iter: c.roaring_uint32_iterator_t = undefined;
                    c.roaring_iterator_init(bm, &pl_iter);
                    while (pl_iter.has_value) {
                        const in_chunk_id = pl_iter.current_value;
                        // Reconstruct: original_id = (chunk_id << chunk_bits) | in_chunk_id
                        const original_id: u32 = (chunk_id << chunk_bits) | in_chunk_id;

                        if (facet_bitmap == null) {
                            facet_bitmap = c.roaring_bitmap_create();
                        }
                        c.roaring_bitmap_add(facet_bitmap, original_id);
                        _ = c.roaring_uint32_iterator_advance(&pl_iter);
                    }
                }

                if (facet_bitmap == null) {
                    if (filter_bitmap != null) c.roaring_bitmap_free(filter_bitmap);
                    filter_bitmap = null;
                    break;
                }

                if (filter_bitmap == null) {
                    filter_bitmap = facet_bitmap;
                } else {
                    c.roaring_bitmap_and_inplace(filter_bitmap, facet_bitmap);
                    c.roaring_bitmap_free(facet_bitmap);
                    if (c.roaring_bitmap_is_empty(filter_bitmap)) break;
                }
            }
        }
    }

    // ReturnSetInfo setup - skip the isA check for now and just use the pointer directly
    // The isA check appears to fail on some PostgreSQL versions due to header differences
    const rsi_ptr = @as(?*c.ReturnSetInfo, @alignCast(@ptrCast(fcinfo.*.resultinfo)));
    if (rsi_ptr == null) {
        utils.elog(c.ERROR, "set-valued function called in context that cannot accept a set");
    }
    const rsi = rsi_ptr.?;
    
    // Check if the allowedModes includes SFRM_Materialize
    if ((rsi.allowedModes & c.SFRM_Materialize) == 0) {
        utils.elog(c.ERROR, "SRF materialize mode not allowed in this context");
    }
    rsi.returnMode = c.SFRM_Materialize;

    const oldcontext = c.MemoryContextSwitchTo(rsi.econtext.?.*.ecxt_per_query_memory);
    rsi.setResult = c.tuplestore_begin_heap(true, false, utils.workMem());

    var tupdesc: c.TupleDesc = undefined;
    tupdesc = c.CreateTemplateTupleDesc(1);
    c.TupleDescInitEntry(tupdesc, 1, "document_id", c.INT8OID, -1, 0);
    rsi.setDesc = tupdesc;

    _ = c.MemoryContextSwitchTo(oldcontext);

    if (filter_bitmap != null) {
        defer c.roaring_bitmap_free(filter_bitmap);

        if (c.roaring_bitmap_is_empty(filter_bitmap)) {
            return 0;
        }

        const card = c.roaring_bitmap_get_cardinality(filter_bitmap);

        if (offset >= card) {
            return 0;
        }

        var iter: c.roaring_uint32_iterator_t = undefined;
        c.roaring_iterator_init(filter_bitmap, &iter);

        var current_idx: i32 = 0;
        var sent_count: i32 = 0;

        while (iter.has_value) {
            if (sent_count >= limit) break;

            if (current_idx >= offset) {
                const val = iter.current_value;

                var values = [_]c.Datum{c.Int64GetDatum(@intCast(val))};
                var nulls = [_]bool{false};

                c.tuplestore_putvalues(rsi.setResult, rsi.setDesc, &values, &nulls);
                sent_count += 1;
            }

            current_idx += 1;
            _ = c.roaring_uint32_iterator_advance(&iter);
        }
    }

    return 0;
}

