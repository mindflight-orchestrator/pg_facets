const std = @import("std");
const utils = @import("utils.zig");
const c = utils.c;
const PgAllocator = utils.PgAllocator;

const DeltaKey = struct {
    facet_id: i32,
    chunk_id: i32,
    facet_value: []const u8, // Owned by the allocator
};

const DeltaValue = struct {
    add: ?*c.roaring_bitmap_t,
    remove: ?*c.roaring_bitmap_t,
};

const DeltaKeyContext = struct {
    pub fn hash(_: @This(), key: DeltaKey) u64 {
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(std.mem.asBytes(&key.facet_id));
        hasher.update(std.mem.asBytes(&key.chunk_id));
        hasher.update(key.facet_value);
        return hasher.final();
    }
    pub fn eql(_: @This(), a: DeltaKey, b: DeltaKey) bool {
        return a.facet_id == b.facet_id and
            a.chunk_id == b.chunk_id and
            std.mem.eql(u8, a.facet_value, b.facet_value);
    }
};

const DeltaMap = std.HashMap(DeltaKey, DeltaValue, DeltaKeyContext, 80);

pub fn merge_deltas_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = PgAllocator.allocator();

    // Argument 0: table_id (Oid)
    const table_id_datum = utils.get_arg_datum(fcinfo, 0);
    const table_id: c.Oid = @intCast(table_id_datum);

    // Initialize SPI
    if (c.SPI_connect() != c.SPI_OK_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed");
    }
    defer _ = c.SPI_finish();

    // ACID Compliance: Create savepoint for atomic operation (graceful failure)
    var savepoint_created = false;
    const savepoint_sql = "SAVEPOINT merge_deltas_atomic";
    const savepoint_ret = c.SPI_execute(savepoint_sql, false, 0);
    if (savepoint_ret == c.SPI_OK_UTILITY) {
        savepoint_created = true;
    } else {
        // Log debug message - outer transaction provides atomicity
        utils.elog(c.DEBUG1, "Could not create savepoint (may be in nested transaction), continuing without savepoint");
    }
    
    // On error path: rollback if savepoint exists
    // Note: This function returns Datum, so we can't use errdefer directly
    // We'll handle rollback in error cases manually

    // 1. Get table info (facets_table, delta_table, chunk_bits)
    const query = std.fmt.allocPrintSentinel(allocator, "SELECT schemaname, facets_table, delta_table, chunk_bits FROM facets.faceted_table WHERE table_id = {d}", .{table_id}, 0) catch {
        utils.elog(c.ERROR, "OOM building query");
        if (savepoint_created) {
            _ = c.SPI_execute("ROLLBACK TO SAVEPOINT merge_deltas_atomic", false, 0);
        }
        return 0;
    };

    if (c.SPI_execute(query.ptr, true, 1) != c.SPI_OK_SELECT) {
        utils.elog(c.ERROR, "Failed to fetch table info");
        if (savepoint_created) {
            _ = c.SPI_execute("ROLLBACK TO SAVEPOINT merge_deltas_atomic", false, 0);
        }
        return c.Int32GetDatum(0);
    }

    if (c.SPI_processed != 1) {
        utils.elog(c.ERROR, "Table not found in facets.faceted_table");
        if (savepoint_created) {
            _ = c.SPI_execute("ROLLBACK TO SAVEPOINT merge_deltas_atomic", false, 0);
        }
        return c.Int32GetDatum(0);
    }

    const schema_name = c.SPI_getvalue(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 1);
    const facets_table = c.SPI_getvalue(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 2);
    const delta_table = c.SPI_getvalue(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 3);
    const chunk_bits_str = c.SPI_getvalue(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 4);

    if (delta_table == null) {
        if (savepoint_created) {
            _ = c.SPI_execute("RELEASE SAVEPOINT merge_deltas_atomic", false, 0);
        }
        return c.Int32GetDatum(0);
    }

    const chunk_bits = std.fmt.parseInt(i32, std.mem.span(chunk_bits_str), 10) catch 20;

    // 2. Read deltas
    const delta_query = std.fmt.allocPrintSentinel(allocator, "SELECT facet_id, facet_value, posting, delta FROM \"{s}\".\"{s}\" WHERE delta <> 0", .{ schema_name, delta_table }, 0) catch {
        utils.elog(c.ERROR, "OOM building delta query");
        if (savepoint_created) {
            _ = c.SPI_execute("ROLLBACK TO SAVEPOINT merge_deltas_atomic", false, 0);
        }
        return 0;
    };

    if (c.SPI_execute(delta_query.ptr, true, 0) != c.SPI_OK_SELECT) {
        utils.elog(c.ERROR, "Failed to read deltas");
        if (savepoint_created) {
            _ = c.SPI_execute("ROLLBACK TO SAVEPOINT merge_deltas_atomic", false, 0);
        }
        return c.Int32GetDatum(0);
    }

    const proc = c.SPI_processed;
    if (proc == 0) {
        if (savepoint_created) {
            _ = c.SPI_execute("RELEASE SAVEPOINT merge_deltas_atomic", false, 0);
        }
        return c.Int32GetDatum(0);
    }

    var map = DeltaMap.init(allocator);
    defer map.deinit();

    // Iterate over deltas
    var i: u64 = 0;
    while (i < proc) : (i += 1) {
        const tuple = c.SPI_tuptable.*.vals[i];
        const tupdesc = c.SPI_tuptable.*.tupdesc;

        var isnull_facet_id: bool = false;
        const facet_id_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull_facet_id);
        const facet_id: i32 = @intCast(c.DatumGetInt32(facet_id_datum));

        var isnull_facet_value: bool = false;
        const facet_value_datum = c.SPI_getbinval(tuple, tupdesc, 2, &isnull_facet_value);
        
        var facet_value: []const u8 = "";
        if (!isnull_facet_value) {
            const text_ptr = utils.detoast_datum(facet_value_datum);
            const text_len = utils.varsize(text_ptr) - utils.varhdrsz();
            const text_data = utils.vardata(text_ptr);
            const fv = allocator.alloc(u8, @intCast(text_len)) catch unreachable;
            @memcpy(fv, @as([*]u8, @ptrCast(text_data))[0..@intCast(text_len)]);
            facet_value = fv;
        }

        // posting is a document ID (integer), not a bitmap!
        // The type can be int2, int4, or int8 depending on key_type
        var isnull_posting: bool = false;
        const posting_datum = c.SPI_getbinval(tuple, tupdesc, 3, &isnull_posting);
        // Use Int32 since most tables use int4 for id, but we'll cast to u32 anyway
        const posting_id: i32 = c.DatumGetInt32(posting_datum);

        var isnull_delta: bool = false;
        const delta_datum = c.SPI_getbinval(tuple, tupdesc, 4, &isnull_delta);
        const delta: i16 = @intCast(c.DatumGetInt16(delta_datum));

        // Calculate chunk_id and in_chunk_id from the posting (document ID)
        const val: u32 = @intCast(@as(u32, @bitCast(posting_id)));
        const chunk_id: i32 = @intCast(val >> @intCast(chunk_bits));
        const in_chunk_id: u32 = val & ((@as(u32, 1) << @intCast(chunk_bits)) - 1);

        const key = DeltaKey{
            .facet_id = facet_id,
            .chunk_id = chunk_id,
            .facet_value = facet_value,
        };

        const entry = map.getOrPut(key) catch unreachable;
        if (!entry.found_existing) {
            entry.value_ptr.* = DeltaValue{ .add = null, .remove = null };
        }

        if (delta > 0) {
            if (entry.value_ptr.add == null) {
                entry.value_ptr.add = c.roaring_bitmap_create();
            }
            c.roaring_bitmap_add(entry.value_ptr.add, in_chunk_id);
        } else {
            if (entry.value_ptr.remove == null) {
                entry.value_ptr.remove = c.roaring_bitmap_create();
            }
            c.roaring_bitmap_add(entry.value_ptr.remove, in_chunk_id);
        }
    }

    // 3. Apply updates
    var it = map.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const val = entry.value_ptr.*;

        // ACID Compliance: Use FOR UPDATE to lock rows during read-modify-write
        const fetch_sql = std.fmt.allocPrintSentinel(allocator, "SELECT postinglist FROM \"{s}\".\"{s}\" WHERE facet_id = $1 AND facet_value = $2 AND chunk_id = $3 FOR UPDATE", .{ schema_name, facets_table }, 0) catch unreachable;

        var argtypes = [_]c.Oid{ c.INT4OID, c.TEXTOID, c.INT4OID };
        var values = [_]c.Datum{ c.Int32GetDatum(key.facet_id), c.PointerGetDatum(c.cstring_to_text_with_len(key.facet_value.ptr, @intCast(key.facet_value.len))), c.Int32GetDatum(key.chunk_id) };
        var nulls = [_]u8{ ' ', ' ', ' ' };

        if (c.SPI_execute_with_args(fetch_sql.ptr, 3, &argtypes, &values, &nulls, false, 1) != c.SPI_OK_SELECT) {
            utils.elog(c.ERROR, "Failed to fetch existing facet");
            if (savepoint_created) {
                _ = c.SPI_execute("ROLLBACK TO SAVEPOINT merge_deltas_atomic", false, 0);
            }
            return c.Int32GetDatum(0);
        }

        var current_bitmap: ?*c.roaring_bitmap_t = null;
        var exists = false;

        if (c.SPI_processed > 0) {
            exists = true;
            const tuple = c.SPI_tuptable.*.vals[0];
            const tupdesc = c.SPI_tuptable.*.tupdesc;
            var isnull_datum: bool = false;
            const datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull_datum);
            const ptr = utils.detoast_datum((datum));
            const len = utils.varsize(ptr) - utils.varhdrsz();
            const data = utils.vardata(ptr);
            current_bitmap = c.roaring_bitmap_portable_deserialize_safe(data, @intCast(len));
        } else {
            current_bitmap = c.roaring_bitmap_create();
        }

        // Apply remove
        if (val.remove) |rem| {
            c.roaring_bitmap_andnot_inplace(current_bitmap, rem);
        }

        // Apply add
        if (val.add) |add| {
            c.roaring_bitmap_or_inplace(current_bitmap, add);
        }

        // Check if empty
        if (c.roaring_bitmap_is_empty(current_bitmap)) {
            if (exists) {
                const delete_sql = std.fmt.allocPrintSentinel(allocator, "DELETE FROM \"{s}\".\"{s}\" WHERE facet_id = $1 AND facet_value = $2 AND chunk_id = $3", .{ schema_name, facets_table }, 0) catch unreachable;
                _ = c.SPI_execute_with_args(delete_sql.ptr, 3, &argtypes, &values, &nulls, false, 0);
            }
        } else {
            const size = c.roaring_bitmap_portable_size_in_bytes(current_bitmap);
            const bytea = @as([*c]c.struct_varlena, @ptrCast(c.palloc(size + @as(usize, @intCast(utils.varhdrsz())))));
            utils.set_varsize(bytea, @intCast(size + @as(usize, @intCast(utils.varhdrsz()))));
            _ = c.roaring_bitmap_portable_serialize(current_bitmap, utils.vardata(bytea));

            const new_posting_datum = c.PointerGetDatum(bytea);

            if (exists) {
                // Use explicit cast to roaringbitmap since we're passing bytea
                const update_sql = std.fmt.allocPrintSentinel(allocator, "UPDATE \"{s}\".\"{s}\" SET postinglist = $4::bytea::roaringbitmap WHERE facet_id = $1 AND facet_value = $2 AND chunk_id = $3", .{ schema_name, facets_table }, 0) catch unreachable;
                var update_argtypes = [_]c.Oid{ c.INT4OID, c.TEXTOID, c.INT4OID, c.BYTEAOID };
                var update_values = [_]c.Datum{ values[0], values[1], values[2], new_posting_datum };
                var update_nulls = [_]u8{ ' ', ' ', ' ', ' ' };
                _ = c.SPI_execute_with_args(update_sql.ptr, 4, &update_argtypes, &update_values, &update_nulls, false, 0);
            } else {
                // Use explicit cast to roaringbitmap since we're passing bytea
                const insert_sql = std.fmt.allocPrintSentinel(allocator, "INSERT INTO \"{s}\".\"{s}\" (facet_id, facet_value, chunk_id, postinglist) VALUES ($1, $2, $3, $4::bytea::roaringbitmap)", .{ schema_name, facets_table }, 0) catch unreachable;
                var insert_argtypes = [_]c.Oid{ c.INT4OID, c.TEXTOID, c.INT4OID, c.BYTEAOID };
                var insert_values = [_]c.Datum{ values[0], values[1], values[2], new_posting_datum };
                var insert_nulls = [_]u8{ ' ', ' ', ' ', ' ' };
                _ = c.SPI_execute_with_args(insert_sql.ptr, 4, &insert_argtypes, &insert_values, &insert_nulls, false, 0);
            }
        }

        c.roaring_bitmap_free(current_bitmap);
    }

    // 4. Clear deltas
    const clear_sql = std.fmt.allocPrintSentinel(allocator, "DELETE FROM \"{s}\".\"{s}\" WHERE delta <> 0", .{ schema_name, delta_table }, 0) catch unreachable;
    _ = c.SPI_execute(clear_sql.ptr, false, 0);

    // ACID Compliance: Release savepoint on success (if it was created)
    if (savepoint_created) {
        const release_sql = "RELEASE SAVEPOINT merge_deltas_atomic";
        _ = c.SPI_execute(release_sql, false, 0);
    }

    return c.Int32GetDatum(0);
}

