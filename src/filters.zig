const std = @import("std");
const utils = @import("utils.zig");
const c = utils.c;
const PgAllocator = utils.PgAllocator;

pub const FilterEntry = struct {
    facet_name: []const u8,
    facet_value: []const u8,
};

// C helper function declaration
extern fn extract_facet_filter_fields(
    composite_datum: c.Datum,
    composite_type: c.Oid,
    facet_name_out: [*c][*c]u8,
    facet_name_len: [*c]c_int,
    facet_value_out: [*c][*c]u8,
    facet_value_len: [*c]c_int,
    facet_value_isnull: [*c]bool,
) c_int;

pub fn parse_filters(allocator: std.mem.Allocator, filters_array: *c.ArrayType) !std.ArrayList(FilterEntry) {
    const elemtype = c.ARR_ELEMTYPE(filters_array);
    var elmlen: i16 = undefined;
    var elmbyval: bool = undefined;
    var elmalign: u8 = undefined;
    c.get_typlenbyvalalign(elemtype, &elmlen, &elmbyval, &elmalign);

    var elems_datum_ptr: [*c]c.Datum = undefined;
    var elems_null_ptr: [*c]bool = undefined;
    var nelems: c_int = undefined;
    
    c.deconstruct_array(filters_array, elemtype, elmlen, elmbyval, @intCast(elmalign), &elems_datum_ptr, &elems_null_ptr, &nelems);
    
    const elems_datum: [*]c.Datum = elems_datum_ptr;
    const elems_null: [*]bool = elems_null_ptr;

    if (nelems == 0) {
        return std.ArrayList(FilterEntry).empty;
    }

    var parsed_filters = std.ArrayList(FilterEntry).empty;

    var i: usize = 0;
    while (i < nelems) : (i += 1) {
        if (elems_null[i]) continue;

        const datum = elems_datum[i];
        
        // Use C helper function to extract fields
        var name_ptr: [*c]u8 = undefined;
        var name_len: c_int = undefined;
        var value_ptr: [*c]u8 = undefined;
        var value_len: c_int = undefined;
        var value_isnull: bool = undefined;
        
        if (extract_facet_filter_fields(datum, elemtype, &name_ptr, &name_len, &value_ptr, &value_len, &value_isnull) == 0) {
            continue; // Skip invalid entries
        }
        
        // Copy facet_name
        const name = try allocator.alloc(u8, @intCast(name_len));
        @memcpy(name, name_ptr[0..@intCast(name_len)]);
        
        // Copy facet_value if not null
        var value: []const u8 = "";
        if (!value_isnull) {
            const v = try allocator.alloc(u8, @intCast(value_len));
            @memcpy(v, value_ptr[0..@intCast(value_len)]);
            value = v;
        }

        try parsed_filters.append(allocator, FilterEntry{ .facet_name = name, .facet_value = value });
    }

    return parsed_filters;
}

// Note: JSONB parsing is handled via SQL in filter_documents_by_facets_bitmap_jsonb_native
// This function is not currently used but kept for future native JSONB parsing

pub fn build_filter_bitmap_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = PgAllocator.allocator();

    // Argument 0: table_id (oid)
    const table_id_datum = utils.get_arg_datum(fcinfo, 0);
    const table_id: c.Oid = @intCast(table_id_datum);

    if (utils.is_arg_null(fcinfo, 1)) {
        return c.PointerGetDatum(null);
    }

    const datum = utils.get_arg_datum(fcinfo, 1);
    const filters_array = @as(*c.ArrayType, @ptrCast(@alignCast(c.DatumGetPointer(datum))));

    var parsed_filters = parse_filters(allocator, filters_array) catch |err| {
        if (err == error.OutOfMemory) utils.elog(c.ERROR, "OutOfMemory");
        return c.PointerGetDatum(null);
    };
    defer parsed_filters.deinit(allocator);

    if (parsed_filters.items.len == 0) {
        return c.PointerGetDatum(null);
    }

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

    for (parsed_filters.items) |entry| {
        const res = facets_map.getOrPut(entry.facet_name) catch unreachable;
        if (!res.found_existing) {
            res.value_ptr.* = std.ArrayList([]const u8).empty;
        }
        res.value_ptr.append(allocator, entry.facet_value) catch unreachable;
    }

    var final_bitmap: ?*c.roaring_bitmap_t = null;

    var it = facets_map.iterator();
    while (it.next()) |entry| {
        const facet_name = entry.key_ptr.*;
        const values = entry.value_ptr.*;

        const id_query = std.fmt.allocPrintSentinel(allocator, "SELECT facet_id FROM facets.facet_definition WHERE table_id = {d} AND facet_name = $1", .{table_id}, 0) catch unreachable;
        var id_argtypes = [_]c.Oid{c.TEXTOID};
        var id_values = [_]c.Datum{c.PointerGetDatum(c.cstring_to_text_with_len(facet_name.ptr, @intCast(facet_name.len)))};
        var id_nulls = [_]u8{' '};

        if (c.SPI_execute_with_args(id_query.ptr, 1, &id_argtypes, &id_values, &id_nulls, true, 1) != c.SPI_OK_SELECT) {
            utils.elog(c.ERROR, "Failed to fetch facet ID");
        }

        if (c.SPI_processed == 0) {
            if (final_bitmap != null) c.roaring_bitmap_free(final_bitmap);
            return c.PointerGetDatum(null);
        }

        var isnull_facet_id: bool = false;
        const facet_id_datum = c.SPI_getbinval(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 1, &isnull_facet_id);
        const facet_id = c.DatumGetInt32(facet_id_datum);

        const value_arr_datum = construct_text_array(allocator, values.items);

        // OPTIMIZED: Use GROUP BY to aggregate postinglists by chunk_id first
        // This reduces the number of bitmap operations needed
        const bitmap_query = std.fmt.allocPrintSentinel(allocator, 
            \\SELECT chunk_id, rb_or_agg(postinglist) AS postinglist
            \\FROM "{s}"."{s}"
            \\WHERE facet_id = {d} AND facet_value = ANY($1)
            \\GROUP BY chunk_id
        , .{ schema_name, facets_table, facet_id }, 0) catch unreachable;

        var b_argtypes = [_]c.Oid{c.TEXTARRAYOID};
        var b_values = [_]c.Datum{value_arr_datum};
        var b_nulls = [_]u8{' '};

        if (c.SPI_execute_with_args(bitmap_query.ptr, 1, &b_argtypes, &b_values, &b_nulls, true, 0) != c.SPI_OK_SELECT) {
            utils.elog(c.ERROR, "Failed to fetch bitmaps");
        }

        // Build bitmap with RECONSTRUCTED original IDs
        var facet_bitmap: ?*c.roaring_bitmap_t = null;

        const proc = c.SPI_processed;
        var k: u64 = 0;
        while (k < proc) : (k += 1) {
            const tuple = c.SPI_tuptable.*.vals[k];
            const desc = c.SPI_tuptable.*.tupdesc;

            // Get chunk_id (column 1)
            var isnull_chunk_id: bool = false;
            const chunk_id_datum_inner = c.SPI_getbinval(tuple, desc, 1, &isnull_chunk_id);
            const chunk_id: u32 = @intCast(c.DatumGetInt32(chunk_id_datum_inner));

            // Get postinglist (column 2) - already aggregated by chunk_id
            var isnull_p: bool = false;
            const p_datum = c.SPI_getbinval(tuple, desc, 2, &isnull_p);

            const p_ptr = utils.detoast_datum(p_datum);
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
            if (final_bitmap != null) c.roaring_bitmap_free(final_bitmap);
            return c.PointerGetDatum(null);
        }

        if (final_bitmap == null) {
            final_bitmap = facet_bitmap;
        } else {
            c.roaring_bitmap_and_inplace(final_bitmap, facet_bitmap);
            c.roaring_bitmap_free(facet_bitmap);

            if (c.roaring_bitmap_is_empty(final_bitmap)) {
                c.roaring_bitmap_free(final_bitmap);
                return c.PointerGetDatum(null);
            }
        }
    }

    if (final_bitmap == null) {
        return c.PointerGetDatum(null);
    }

    const size = c.roaring_bitmap_portable_size_in_bytes(final_bitmap);
    const res_bytea = @as([*c]c.struct_varlena, @ptrCast(c.palloc(size + @as(usize, @intCast(utils.varhdrsz())))));
    utils.set_varsize(res_bytea, @intCast(size + @as(usize, @intCast(utils.varhdrsz()))));
    _ = c.roaring_bitmap_portable_serialize(final_bitmap, utils.vardata(res_bytea));
    c.roaring_bitmap_free(final_bitmap);

    return c.PointerGetDatum(res_bytea);
}

// Native implementation of filter_documents_by_facets_bitmap that accepts JSONB
// This is an optimized wrapper that converts JSONB to array and calls build_filter_bitmap_native
pub fn filter_documents_by_facets_bitmap_jsonb_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = PgAllocator.allocator();

    // Argument 0: schema_name (text)
    const schema_name_datum = utils.get_arg_datum(fcinfo, 0);
    const schema_name_ptr = c.DatumGetTextP(schema_name_datum);
    
    // Argument 1: facets (jsonb)
    if (utils.is_arg_null(fcinfo, 1)) {
        utils.set_return_null(fcinfo);
        return @as(c.Datum, 0);
    }
    const jsonb_datum = utils.get_arg_datum(fcinfo, 1);
    
    // Argument 2: table_name (text, nullable)
    const has_table_name = !utils.is_arg_null(fcinfo, 2);
    
    if (c.SPI_connect() != c.SPI_OK_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed");
    }
    defer _ = c.SPI_finish();

    // Convert JSONB to array format and get table_id in one optimized query
    if (has_table_name) {
        const table_name_datum = utils.get_arg_datum(fcinfo, 2);
        const table_name_ptr = c.DatumGetTextP(table_name_datum);
        
        const q = std.fmt.allocPrintSentinel(allocator,
            \\WITH parsed_filters AS (
            \\    SELECT array_agg(ROW(key, value)::facets.facet_filter) FILTER (WHERE key IS NOT NULL) AS filters
            \\    FROM jsonb_each_text($1)
            \\    WHERE value IS NOT NULL
            \\),
            \\table_info AS (
            \\    SELECT table_id FROM facets.faceted_table WHERE schemaname = $2 AND tablename = $3
            \\)
            \\SELECT 
            \\    (SELECT table_id FROM table_info) AS table_id,
            \\    (SELECT filters FROM parsed_filters) AS filters
        , .{}, 0) catch unreachable;
        
        var argtypes = [_]c.Oid{c.JSONBOID, c.TEXTOID, c.TEXTOID};
        var values = [_]c.Datum{
            jsonb_datum,
            c.PointerGetDatum(schema_name_ptr),
            c.PointerGetDatum(table_name_ptr)
        };
        var nulls = [_]u8{' ', ' ', ' '};
        
        if (c.SPI_execute_with_args(q.ptr, 3, &argtypes, &values, &nulls, true, 1) != c.SPI_OK_SELECT or c.SPI_processed == 0) {
            utils.set_return_null(fcinfo);
            return @as(c.Datum, 0);
        }
    } else {
        const q = std.fmt.allocPrintSentinel(allocator,
            \\WITH parsed_filters AS (
            \\    SELECT array_agg(ROW(key, value)::facets.facet_filter) FILTER (WHERE key IS NOT NULL) AS filters
            \\    FROM jsonb_each_text($1)
            \\    WHERE value IS NOT NULL
            \\),
            \\table_info AS (
            \\    SELECT table_id FROM facets.faceted_table WHERE schemaname = $2 LIMIT 1
            \\)
            \\SELECT 
            \\    (SELECT table_id FROM table_info) AS table_id,
            \\    (SELECT filters FROM parsed_filters) AS filters
        , .{}, 0) catch unreachable;
        
        var argtypes = [_]c.Oid{c.JSONBOID, c.TEXTOID};
        var values = [_]c.Datum{
            jsonb_datum,
            c.PointerGetDatum(schema_name_ptr)
        };
        var nulls = [_]u8{' ', ' '};
        
        if (c.SPI_execute_with_args(q.ptr, 2, &argtypes, &values, &nulls, true, 1) != c.SPI_OK_SELECT or c.SPI_processed == 0) {
            utils.set_return_null(fcinfo);
            return @as(c.Datum, 0);
        }
    }
    
    var isnull_table_id: bool = false;
    var isnull_filters: bool = false;
    const table_id_datum = c.SPI_getbinval(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 1, &isnull_table_id);
    const filters_datum = c.SPI_getbinval(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 2, &isnull_filters);
    
    if (isnull_table_id or isnull_filters) {
        utils.set_return_null(fcinfo);
        return @as(c.Datum, 0);
    }
    
    // Call build_filter_bitmap_native with the converted array
    const call_query = std.fmt.allocPrintSentinel(allocator, "SELECT build_filter_bitmap_native($1, $2)", .{}, 0) catch unreachable;
    
    var call_argtypes = [_]c.Oid{c.OIDOID, c.ANYARRAYOID};
    var call_values = [_]c.Datum{table_id_datum, filters_datum};
    var call_nulls = [_]u8{' ', ' '};
    
    if (c.SPI_execute_with_args(call_query.ptr, 2, &call_argtypes, &call_values, &call_nulls, true, 1) != c.SPI_OK_SELECT) {
        utils.set_return_null(fcinfo);
        return @as(c.Datum, 0);
    }
    
    if (c.SPI_processed == 0) {
        utils.set_return_null(fcinfo);
        return @as(c.Datum, 0);
    }
    
    var isnull_result: bool = false;
    const result_datum = c.SPI_getbinval(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 1, &isnull_result);
    
    if (isnull_result) {
        utils.set_return_null(fcinfo);
        return @as(c.Datum, 0);
    }
    
    return result_datum;
}

fn construct_text_array(allocator: std.mem.Allocator, strings: [][]const u8) c.Datum {
    const datums = allocator.alloc(c.Datum, strings.len) catch unreachable;
    for (strings, 0..) |s, i| {
        datums[i] = c.PointerGetDatum(c.cstring_to_text_with_len(s.ptr, @intCast(s.len)));
    }

    const arr = c.construct_array(datums.ptr, @intCast(strings.len), c.TEXTOID, -1, false, 'i');
    return c.PointerGetDatum(arr);
}
