const std = @import("std");
const utils = @import("../utils.zig");
const c = utils.c;
const PgAllocator = utils.PgAllocator;

/// Convert PostgreSQL roaringbitmap datum to C roaring_bitmap_t
/// IMPORTANT: This function copies the data to ensure it persists after SPI_finish()
/// CRITICAL: pg_detoast_datum may return a pointer to tuple memory, so we must
/// read all data immediately and copy it before any SPI operations complete.
pub fn datumToRoaringBitmap(datum: c.Datum) !*c.roaring_bitmap_t {
    utils.elogFmt(c.NOTICE, "[TRACE] datumToRoaringBitmap: Starting deserialization", .{});
    
    // Step 1: Detoast the datum (may return pointer to tuple memory or a copy)
    utils.elogFmt(c.NOTICE, "[TRACE] datumToRoaringBitmap: Step 1 - Calling detoast_datum", .{});
    const varlena = utils.detoast_datum(datum);
    const varlena_ptr = @as(*align(1) c.struct_varlena, @ptrCast(varlena));
    utils.elogFmt(c.NOTICE, "[TRACE] datumToRoaringBitmap: Step 1 - detoast_datum returned varlena_ptr={*}", .{varlena_ptr});
    
    // Step 2: Read header IMMEDIATELY (before any potential memory invalidation)
    utils.elogFmt(c.NOTICE, "[TRACE] datumToRoaringBitmap: Step 2 - Reading header", .{});
    const header_bytes = @as(*align(1) [4]u8, @ptrCast(varlena_ptr));
    const header_u32 = std.mem.readInt(u32, header_bytes, .little);
    const total_size = (header_u32 >> 2) & 0x3FFFFFFF;
    const varhdrsz = @as(usize, @intCast(utils.varhdrsz()));
    const len = @as(usize, @intCast(total_size)) - varhdrsz;
    utils.elogFmt(c.NOTICE, "[TRACE] datumToRoaringBitmap: Step 2 - Header read: total_size={d}, varhdrsz={d}, len={d}", .{ total_size, varhdrsz, len });
    
    // Step 3: Read data pointer (may point to tuple memory)
    utils.elogFmt(c.NOTICE, "[TRACE] datumToRoaringBitmap: Step 3 - Getting data pointer", .{});
    const data = @as([*]u8, @ptrCast(varlena_ptr)) + varhdrsz;
    utils.elogFmt(c.NOTICE, "[TRACE] datumToRoaringBitmap: Step 3 - Data pointer={*}, len={d}", .{ data, len });
    
    // Step 4: Copy ALL data to stable memory IMMEDIATELY
    // This must happen before SPI_finish() or any memory context changes
    utils.elogFmt(c.NOTICE, "[TRACE] datumToRoaringBitmap: Step 4 - Allocating stable memory (len={d})", .{len});
    const data_copy = std.c.malloc(len) orelse {
        utils.elog(c.ERROR, "[TRACE] datumToRoaringBitmap: Failed to allocate memory for bitmap deserialization");
        return error.OutOfMemory;
    };
    const data_copy_ptr = @as([*]u8, @ptrCast(data_copy));
    utils.elogFmt(c.NOTICE, "[TRACE] datumToRoaringBitmap: Step 4 - Copying data from {*} to {*}", .{ data, data_copy_ptr });
    @memcpy(data_copy_ptr[0..len], data[0..len]);
    utils.elogFmt(c.NOTICE, "[TRACE] datumToRoaringBitmap: Step 4 - Data copied successfully", .{});
    
    // Step 5: Deserialize from the copied data
    // roaring_bitmap_portable_deserialize_safe makes its own copy, so data_copy
    // can be freed after this, but we'll let the caller manage bitmap lifetime
    utils.elogFmt(c.NOTICE, "[TRACE] datumToRoaringBitmap: Step 5 - Deserializing bitmap from copied data", .{});
    const bitmap = c.roaring_bitmap_portable_deserialize_safe(@as([*c]const u8, @ptrCast(data_copy_ptr)), @intCast(len));
    if (bitmap == null) {
        std.c.free(data_copy);
        utils.elog(c.ERROR, "[TRACE] datumToRoaringBitmap: Failed to deserialize roaring bitmap");
        return error.DeserializeFailed;
    }
    utils.elogFmt(c.NOTICE, "[TRACE] datumToRoaringBitmap: Step 5 - Bitmap deserialized successfully, bitmap={*}", .{bitmap});
    
    // Free data_copy now that bitmap has its own copy
    utils.elogFmt(c.NOTICE, "[TRACE] datumToRoaringBitmap: Step 6 - Freeing data_copy", .{});
    std.c.free(data_copy);
    utils.elogFmt(c.NOTICE, "[TRACE] datumToRoaringBitmap: Step 6 - Returning bitmap={*}", .{bitmap});
    
    return bitmap.?;
}

/// Convert C roaring_bitmap_t to PostgreSQL roaringbitmap datum
pub fn roaringBitmapToDatum(bitmap: *c.roaring_bitmap_t) c.Datum {
    utils.elogFmt(c.NOTICE, "[TRACE] roaringBitmapToDatum: Starting, bitmap={*}, cardinality={d}", .{ bitmap, cardinality(bitmap) });

    const size = c.roaring_bitmap_portable_size_in_bytes(bitmap);
    const varhdrsz = @as(usize, @intCast(utils.varhdrsz()));
    const total_size = varhdrsz + size;
    utils.elogFmt(c.NOTICE, "[TRACE] roaringBitmapToDatum: size={d}, varhdrsz={d}, total_size={d}", .{ size, varhdrsz, total_size });

    utils.elogFmt(c.NOTICE, "[TRACE] roaringBitmapToDatum: Allocating varlena", .{});
    const varlena = @as(*c.struct_varlena, @ptrCast(c.palloc(@intCast(total_size))));
    utils.set_varsize(varlena, @intCast(total_size));
    utils.elogFmt(c.NOTICE, "[TRACE] roaringBitmapToDatum: varlena allocated at {*}", .{varlena});

    const data = utils.vardata(varlena);
    utils.elogFmt(c.NOTICE, "[TRACE] roaringBitmapToDatum: Serializing bitmap to data={*}", .{data});
    // CRITICAL: roaring_bitmap_portable_serialize makes a complete copy of the bitmap data
    // into the provided buffer. The bitmap can be freed after this call.
    const written = c.roaring_bitmap_portable_serialize(bitmap, data);
    utils.elogFmt(c.NOTICE, "[TRACE] roaringBitmapToDatum: Serialized, written={d}, expected={d}", .{ written, size });
    if (written != size) {
        utils.elogFmt(c.ERROR, "[TRACE] roaringBitmapToDatum: Failed to serialize roaring bitmap (written={d}, expected={d})", .{ written, size });
        return c.PointerGetDatum(null);
    }

    // The serialized data is now in varlena, which is allocated with palloc (PostgreSQL memory context)
    // The bitmap is no longer needed - it can be freed safely
    const datum = c.PointerGetDatum(varlena);
    utils.elogFmt(c.NOTICE, "[TRACE] roaringBitmapToDatum: Returning Datum (bitmap can now be freed)", .{});
    return datum;
}

/// Create an empty roaring bitmap
pub fn createEmptyBitmap() *c.roaring_bitmap_t {
    return c.roaring_bitmap_create();
}

/// Add a document ID to a bitmap
pub fn addDocument(bitmap: *c.roaring_bitmap_t, doc_id: u32) void {
    c.roaring_bitmap_add(bitmap, doc_id);
}

/// Check if a document ID is in the bitmap
pub fn contains(bitmap: *c.roaring_bitmap_t, doc_id: u32) bool {
    return c.roaring_bitmap_contains(bitmap, doc_id);
}

/// Get cardinality (number of documents) in bitmap
pub fn cardinality(bitmap: *c.roaring_bitmap_t) u64 {
    return c.roaring_bitmap_get_cardinality(bitmap);
}

/// Check if bitmap is empty
pub fn isEmpty(bitmap: *c.roaring_bitmap_t) bool {
    return c.roaring_bitmap_is_empty(bitmap);
}

/// Perform AND operation (intersection) - modifies bitmap1 in place
pub fn andInPlace(bitmap1: *c.roaring_bitmap_t, bitmap2: *c.roaring_bitmap_t) void {
    c.roaring_bitmap_and_inplace(bitmap1, bitmap2);
}

/// Perform OR operation (union) - modifies bitmap1 in place
pub fn orInPlace(bitmap1: *c.roaring_bitmap_t, bitmap2: *c.roaring_bitmap_t) void {
    c.roaring_bitmap_or_inplace(bitmap1, bitmap2);
}

/// Create a new bitmap that is the AND of two bitmaps
pub fn andBitmap(bitmap1: *c.roaring_bitmap_t, bitmap2: *c.roaring_bitmap_t) *c.roaring_bitmap_t {
    return c.roaring_bitmap_and(bitmap1, bitmap2);
}

/// Create a new bitmap that is the OR of two bitmaps
pub fn orBitmap(bitmap1: *c.roaring_bitmap_t, bitmap2: *c.roaring_bitmap_t) *c.roaring_bitmap_t {
    return c.roaring_bitmap_or(bitmap1, bitmap2);
}

/// Free a roaring bitmap
pub fn free(bitmap: *c.roaring_bitmap_t) void {
    c.roaring_bitmap_free(bitmap);
}

/// Copy a roaring bitmap
pub fn copy(bitmap: *const c.roaring_bitmap_t) *c.roaring_bitmap_t {
    return c.roaring_bitmap_copy(bitmap);
}

/// Iterator for roaring bitmap
pub const Iterator = struct {
    iter: c.roaring_uint32_iterator_t,
    
    pub fn init(bitmap: *c.roaring_bitmap_t) Iterator {
        var iter: c.roaring_uint32_iterator_t = undefined;
        c.roaring_iterator_init(bitmap, &iter);
        return Iterator{ .iter = iter };
    }
    
    pub fn hasValue(self: *Iterator) bool {
        return self.iter.has_value;
    }
    
    pub fn currentValue(self: *Iterator) u32 {
        return self.iter.current_value;
    }
    
    pub fn advance(self: *Iterator) void {
        _ = c.roaring_uint32_iterator_advance(&self.iter);
    }
};
