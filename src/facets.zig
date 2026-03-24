const std = @import("std");
const utils = @import("utils.zig");
const c = utils.c;
const PgAllocator = utils.PgAllocator;

const FacetCount = struct {
    facet_name: []const u8,
    facet_value: []const u8,
    cardinality: u64,
    facet_id: i32,
};

pub fn get_facet_counts_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = PgAllocator.allocator();

    // Arguments:
    // 0: table_id (oid)
    // 1: filter_bitmap (roaringbitmap) (nullable)
    // 2: facets (text[]) (nullable)
    // 3: top_n (int) (default 5)

    const table_id_datum = utils.get_arg_datum(fcinfo, 0);
    const table_id: c.Oid = @intCast(table_id_datum);

    var filter_bitmap: ?*c.roaring_bitmap_t = null;
    if (!utils.is_arg_null(fcinfo, 1)) {
        const datum = utils.get_arg_datum(fcinfo, 1);
        const ptr = utils.detoast_datum(datum);
        const len = utils.varsize(ptr) - utils.varhdrsz();
        const data = utils.vardata(ptr);
        filter_bitmap = c.roaring_bitmap_portable_deserialize_safe(data, @intCast(len));
    }
    defer if (filter_bitmap) |bm| c.roaring_bitmap_free(bm);

    var target_facets = std.ArrayList([]const u8).empty;
    defer target_facets.deinit(allocator);

    if (!utils.is_arg_null(fcinfo, 2)) {
        const datum = utils.get_arg_datum(fcinfo, 2);
        const array = @as(*c.ArrayType, @alignCast(@ptrCast(c.DatumGetPointer(datum))));
        const elemtype = c.ARR_ELEMTYPE(array);
        var elmlen: i16 = undefined;
        var elmbyval: bool = undefined;
        var elmalign: u8 = undefined;
        c.get_typlenbyvalalign(elemtype, &elmlen, &elmbyval, &elmalign);

        var elems_datum_ptr: [*c]c.Datum = undefined;
        var elems_null_ptr: [*c]bool = undefined;
        var nelems: c_int = undefined;
        
        c.deconstruct_array(array, elemtype, elmlen, elmbyval, @intCast(elmalign), &elems_datum_ptr, &elems_null_ptr, &nelems);
        
        const elems_datum: [*]c.Datum = elems_datum_ptr;
        const elems_null: [*]bool = elems_null_ptr;

        var i: usize = 0;
        while (i < nelems) : (i += 1) {
            if (!elems_null[i]) {
                const s = c.TextDatumGetCString(elems_datum[i]);
                const len = std.mem.len(s);
                const s_copy = allocator.alloc(u8, len) catch unreachable;
                @memcpy(s_copy, s[0..len]);
                target_facets.append(allocator, s_copy) catch unreachable;
            }
        }
    }

    const limit: i32 = if (!utils.is_arg_null(fcinfo, 3)) @intCast(c.DatumGetInt32(utils.get_arg_datum(fcinfo, 3))) else 5;

    // ReturnSetInfo setup
    const rsi_ptr = @as(?*c.ReturnSetInfo, @alignCast(@ptrCast(fcinfo.*.resultinfo)));
    if (rsi_ptr == null or !utils.isA(@ptrCast(rsi_ptr), utils.tReturnSetInfo())) {
        utils.elog(c.ERROR, "set-valued function called in context that cannot accept a set");
    }
    const rsi = rsi_ptr.?;

    rsi.returnMode = c.SFRM_Materialize;

    var tupdesc: c.TupleDesc = undefined;
    if (c.get_call_result_type(fcinfo, null, &tupdesc) != c.TYPEFUNC_COMPOSITE) {
        utils.elog(c.ERROR, "return type must be a row type");
    }

    const oldcontext = c.MemoryContextSwitchTo(rsi.econtext.?.*.ecxt_per_query_memory);
    rsi.setResult = c.tuplestore_begin_heap(true, false, utils.workMem());
    rsi.setDesc = c.CreateTupleDescCopy(tupdesc);
    _ = c.MemoryContextSwitchTo(oldcontext);

    if (c.SPI_connect() != c.SPI_OK_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed");
    }
    defer _ = c.SPI_finish();

    // Get table info
    const table_info_query = std.fmt.allocPrintSentinel(allocator, "SELECT schemaname, facets_table FROM facets.faceted_table WHERE table_id = {d}", .{table_id}, 0) catch unreachable;
    if (c.SPI_execute(table_info_query.ptr, true, 1) != c.SPI_OK_SELECT or c.SPI_processed != 1) {
        utils.elog(c.ERROR, "Table not found in facets.faceted_table");
    }

    const schema_name = c.SPI_getvalue(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 1);
    const facets_table = c.SPI_getvalue(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 2);

    // Get all facet definitions if not specified
    const FacetDef = struct { id: i32, name: []const u8 };
    var facets_to_process = std.ArrayList(FacetDef).empty;
    defer facets_to_process.deinit(allocator);

    if (target_facets.items.len > 0) {
        for (target_facets.items) |fname| {
            const f_query = std.fmt.allocPrintSentinel(allocator, "SELECT facet_id FROM facets.facet_definition WHERE table_id = {d} AND facet_name = $1", .{table_id}, 0) catch unreachable;
            var f_argtypes = [_]c.Oid{c.TEXTOID};
            var f_values = [_]c.Datum{c.PointerGetDatum(c.cstring_to_text_with_len(fname.ptr, @intCast(fname.len)))};
            var f_nulls = [_]u8{' '};

            if (c.SPI_execute_with_args(f_query.ptr, 1, &f_argtypes, &f_values, &f_nulls, true, 1) == c.SPI_OK_SELECT and c.SPI_processed > 0) {
                var isnull_fid: bool = false;
                const fid_datum = c.SPI_getbinval(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 1, &isnull_fid);
                facets_to_process.append(allocator, .{ .id = c.DatumGetInt32(fid_datum), .name = fname }) catch unreachable;
            }
        }
    } else {
        const all_query = std.fmt.allocPrintSentinel(allocator, "SELECT facet_id, facet_name FROM facets.facet_definition WHERE table_id = {d}", .{table_id}, 0) catch unreachable;
        if (c.SPI_execute(all_query.ptr, true, 0) == c.SPI_OK_SELECT) {
            const proc = c.SPI_processed;
            var k: u64 = 0;
            while (k < proc) : (k += 1) {
                const tuple = c.SPI_tuptable.*.vals[k];
                const desc = c.SPI_tuptable.*.tupdesc;
                var isnull_fid: bool = false;
                var isnull_fname: bool = false;
                const fid_datum = c.SPI_getbinval(tuple, desc, 1, &isnull_fid);
                const fname_datum = c.SPI_getbinval(tuple, desc, 2, &isnull_fname);
                const fname = c.TextDatumGetCString(fname_datum);

                const flen = std.mem.len(fname);
                const fcopy = allocator.alloc(u8, flen) catch unreachable;
                @memcpy(fcopy, fname[0..flen]);

                facets_to_process.append(allocator, .{ .id = c.DatumGetInt32(fid_datum), .name = fcopy }) catch unreachable;
            }
        }
    }

    // Process each facet
    for (facets_to_process.items) |f| {
        var query: []u8 = undefined;
        if (filter_bitmap != null) {
            query = std.fmt.allocPrintSentinel(allocator, "SELECT facet_value, postinglist FROM \"{s}\".\"{s}\" WHERE facet_id = {d}", .{ schema_name, facets_table, f.id }, 0) catch unreachable;
        } else {
            query = std.fmt.allocPrintSentinel(allocator, "SELECT facet_value, rb_cardinality(postinglist) as card FROM \"{s}\".\"{s}\" WHERE facet_id = {d} ORDER BY card DESC LIMIT {d}", .{ schema_name, facets_table, f.id, limit }, 0) catch unreachable;

            if (c.SPI_execute(query.ptr, true, 0) == c.SPI_OK_SELECT) {
                const proc = c.SPI_processed;
                var k: u64 = 0;
                while (k < proc) : (k += 1) {
                    const tuple = c.SPI_tuptable.*.vals[k];
                    const desc = c.SPI_tuptable.*.tupdesc;

                    var isnull_val: bool = false;
                    const val_datum = c.SPI_getbinval(tuple, desc, 1, &isnull_val);
                    var isnull_card: bool = false;
                    const card_datum = c.SPI_getbinval(tuple, desc, 2, &isnull_card);

                    var values = [_]c.Datum{ c.PointerGetDatum(c.cstring_to_text_with_len(f.name.ptr, @intCast(f.name.len))), val_datum, c.Int64GetDatum(@intCast(c.DatumGetInt32(card_datum))), c.Int32GetDatum(f.id) };
                    var nulls = [_]bool{ false, false, false, false };

                    c.tuplestore_putvalues(rsi.setResult, rsi.setDesc, &values, &nulls);
                }
            }
            continue;
        }

        if (c.SPI_execute(query.ptr, true, 0) != c.SPI_OK_SELECT) continue;

        const proc = c.SPI_processed;
        var k: u64 = 0;

        var counts = std.ArrayList(FacetCount).empty;
        defer counts.deinit(allocator);

        while (k < proc) : (k += 1) {
            const tuple = c.SPI_tuptable.*.vals[k];
            const desc = c.SPI_tuptable.*.tupdesc;

            var isnull_val: bool = false;
            const val_datum = c.SPI_getbinval(tuple, desc, 1, &isnull_val);
            var isnull_p: bool = false;
            const p_datum = c.SPI_getbinval(tuple, desc, 2, &isnull_p);

            const p_ptr = utils.detoast_datum((p_datum));
            const p_len = utils.varsize(p_ptr) - utils.varhdrsz();
            const p_data = utils.vardata(p_ptr);

            const bm = c.roaring_bitmap_portable_deserialize_safe(p_data, @intCast(p_len));
            defer c.roaring_bitmap_free(bm);

            const intersection_card = c.roaring_bitmap_and_cardinality(bm, filter_bitmap.?);

            if (intersection_card > 0) {
                const v_text = c.TextDatumGetCString(val_datum);
                const v_len = std.mem.len(v_text);
                const v_copy = allocator.alloc(u8, v_len) catch unreachable;
                @memcpy(v_copy, v_text[0..v_len]);

                counts.append(allocator, .{ .facet_name = f.name, .facet_value = v_copy, .cardinality = intersection_card, .facet_id = f.id }) catch unreachable;
            }
        }

        const Sorter = struct {
            fn lessThan(_: void, lhs: FacetCount, rhs: FacetCount) bool {
                return lhs.cardinality > rhs.cardinality; // Descending
            }
        };
        std.sort.block(FacetCount, counts.items, {}, Sorter.lessThan);

        var count: usize = 0;
        for (counts.items) |item| {
            if (count >= limit) break;

            var values = [_]c.Datum{ c.PointerGetDatum(c.cstring_to_text_with_len(item.facet_name.ptr, @intCast(item.facet_name.len))), c.PointerGetDatum(c.cstring_to_text_with_len(item.facet_value.ptr, @intCast(item.facet_value.len))), c.Int64GetDatum(@intCast(item.cardinality)), c.Int32GetDatum(item.facet_id) };
            var nulls = [_]bool{ false, false, false, false };

            c.tuplestore_putvalues(rsi.setResult, rsi.setDesc, &values, &nulls);
            count += 1;
        }
    }

    return 0;
}

