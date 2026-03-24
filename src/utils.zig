const std = @import("std");

// Import C headers
pub const c = @cImport({
    // Define the macros as functions if possible or provide necessary definitions
    @cDefine("PG_MODULE_MAGIC", "1");
    // Workaround for VARHDRSZ_EXTERNAL macro that Zig can't translate
    // This macro is not used in our code, but PostgreSQL headers define it
    @cDefine("VARHDRSZ_EXTERNAL", "VARHDRSZ");

    @cInclude("postgres.h");
    @cInclude("fmgr.h");
    @cInclude("executor/spi.h");
    @cInclude("utils/builtins.h");
    @cInclude("utils/elog.h");
    @cInclude("utils/memutils.h"); // For MemoryContext
    @cInclude("utils/array.h"); // For ArrayType
    @cInclude("utils/lsyscache.h"); // For get_typlenbyvalalign
    @cInclude("utils/typcache.h"); // For lookup_type_cache
    @cInclude("catalog/pg_type.h"); // For type OIDs
    @cInclude("access/htup_details.h"); // For heap_getattr
    @cInclude("tsearch/ts_utils.h"); // For text search functions
    @cInclude("tsearch/ts_type.h"); // For TSVector types
    @cInclude("roaringbitmap.h");
    @cInclude("roaring.h"); // Ensure we have access to CRoaring types
});

// C helper function declarations
extern fn detoast_datum_helper(d: c.Datum) [*c]c.struct_varlena;
extern fn varsize_helper(ptr: [*c]c.struct_varlena) c_int;
extern fn vardata_helper(ptr: [*c]c.struct_varlena) [*c]u8;
extern fn varhdrsz_helper() c_int;
extern fn set_varsize_helper(ptr: [*c]c.struct_varlena, size: c_int) void;
extern fn isa_helper(node: ?*anyopaque, tag: c.NodeTag) bool;
extern fn t_returnsetinfo_helper() c.NodeTag;
extern fn elog_helper(level: c_int, msg: [*c]const u8) void;
extern fn work_mem_helper() c_int;
extern fn datum_get_textp_helper(d: c.Datum) [*c]c.struct_varlena;
extern fn varsize_any_exhdr_helper(ptr: [*c]c.struct_varlena) c_int;
extern fn vardata_any_helper(ptr: [*c]c.struct_varlena) [*c]u8;
extern fn text_to_cstring_helper(d: c.Datum) [*c]u8;
extern fn strlen_helper(str: [*c]const u8) usize;
extern fn fcinfo_get_arg_value_helper(fcinfo: c.FunctionCallInfo, n: c_int) c.Datum;
extern fn fcinfo_get_arg_isnull_helper(fcinfo: c.FunctionCallInfo, n: c_int) bool;
extern fn fcinfo_set_isnull_helper(fcinfo: c.FunctionCallInfo, isnull: bool) void;

// Text search C API functions
extern fn get_ts_config_oid(config_name: [*c]const u8, missing_ok: bool) c.Oid;
extern fn to_tsvector_byid(config_oid: c.Oid, text_datum: c.Datum) c.Datum;

// PostgreSQL tsvector internal structures (from tsearch/ts_type.h)
// WordEntry is a packed 32-bit bitfield, NOT 3 separate u32 fields!
// typedef struct { uint32 haspos:1, len:11, pos:20; } WordEntry;
pub const WordEntry = packed struct {
    haspos: u1,   // Bit 0: has positions
    len: u11,     // Bits 1-11: lexeme length (max 2047)
    pos: u20,     // Bits 12-31: offset to lexeme string (max 1M)
};

pub const WordEntryPos = packed struct {
    // typedef struct { uint16 weight:2, pos:14; } WordEntryPosIn;
    weight: u2,  // Weight (A=3, B=2, C=1, D=0)
    pos: u14,    // Position in document (1-based, max 16383)
};

pub const TSVectorData = extern struct {
    vl_len_: c_int, // varlena header (4 bytes)
    size: i32,      // Number of lexemes (signed, PostgreSQL uses int32)
    // Followed by: WordEntry[size], then lexeme strings
};

// Macros for accessing tsvector data
// Based on PostgreSQL's internal structure in tsearch/ts_type.h:
// - TSVectorData header with size
// - WordEntry array (size entries)  
// - Lexeme strings (variable length, starting at STRPTR)
// - Position data: if haspos is set, after each lexeme there's a uint16 count
//   followed by WordEntryPos array
pub fn POSDATALEN(tsv: [*c]c.struct_varlena, entry: *align(1) const WordEntry) u32 {
    if (entry.haspos == 0) return 0;
    const tsv_data = @as(*TSVectorData, @alignCast(@ptrCast(@constCast(tsv))));
    const str_ptr = STRPTR(tsv_data);
    // Position data is SHORTALIGN'ed (2-byte aligned) after the lexeme string
    // PostgreSQL: #define _POSVECPTR(x, e) ((WordEntryPosVector *)(STRPTR(x) + SHORTALIGN((e)->pos + (e)->len)))
    const offset = @as(usize, entry.pos) + @as(usize, entry.len);
    const aligned_offset = (offset + 1) & ~@as(usize, 1); // SHORTALIGN: round up to 2-byte boundary
    const lexeme_end = str_ptr + aligned_offset;
    const pos_count_ptr = @as(*align(1) u16, @ptrCast(lexeme_end));
    return @as(u32, @intCast(pos_count_ptr.*));
}

pub fn POSDATAPTR(tsv: [*c]c.struct_varlena, entry: *align(1) const WordEntry) [*]align(1) WordEntryPos {
    if (entry.haspos == 0) {
        // Return a null pointer for no positions
        return @as([*]align(1) WordEntryPos, @ptrFromInt(0));
    }
    const tsv_data = @as(*TSVectorData, @alignCast(@ptrCast(@constCast(tsv))));
    const str_ptr = STRPTR(tsv_data);
    // Position data is SHORTALIGN'ed after the lexeme string
    const offset = @as(usize, entry.pos) + @as(usize, entry.len);
    const aligned_offset = (offset + 1) & ~@as(usize, 1); // SHORTALIGN
    const lexeme_end = str_ptr + aligned_offset;
    const pos_ptr = lexeme_end + @sizeOf(u16); // Skip the npos count
    return @as([*]align(1) WordEntryPos, @ptrCast(pos_ptr));
}

pub fn ARRPTR(tsv: *const TSVectorData) [*]align(1) WordEntry {
    const base = @as([*]u8, @ptrCast(@constCast(tsv)));
    // WordEntry array starts immediately after the TSVectorData header (8 bytes: vl_len_ + size)
    const offset_ptr = base + @sizeOf(TSVectorData);
    return @as([*]align(1) WordEntry, @ptrCast(offset_ptr));
}

pub fn STRPTR(tsv: *const TSVectorData) [*]u8 {
    const arr = ARRPTR(tsv);
    const num_entries: usize = @intCast(tsv.size);
    // Lexeme strings start after the WordEntry array
    // Each WordEntry is 4 bytes (packed u32)
    // The pos field in WordEntry is relative to STRPTR
    const arr_as_bytes = @as([*]u8, @ptrCast(arr));
    const word_entry_end = arr_as_bytes + (num_entries * @sizeOf(WordEntry));
    return word_entry_end;
}

// Wrapper for pg_detoast_datum to avoid macro translation issues
pub fn detoast_datum(d: c.Datum) [*c]c.struct_varlena {
    return detoast_datum_helper(d);
}

// Wrappers for VARSIZE and VARDATA to avoid macro translation issues
pub fn varsize(ptr: [*c]c.struct_varlena) c_int {
    return varsize_helper(ptr);
}

pub fn vardata(ptr: [*c]c.struct_varlena) [*c]u8 {
    return vardata_helper(ptr);
}

// Wrapper for VARHDRSZ constant
pub fn varhdrsz() c_int {
    return varhdrsz_helper();
}

// Wrapper for SET_VARSIZE macro
pub fn set_varsize(ptr: [*c]c.struct_varlena, size: c_int) void {
    set_varsize_helper(ptr, size);
}

// Wrapper for IsA macro
pub fn isA(node: ?*anyopaque, tag: c.NodeTag) bool {
    return isa_helper(node, tag);
}

// Wrapper for T_ReturnSetInfo constant
pub fn tReturnSetInfo() c.NodeTag {
    return t_returnsetinfo_helper();
}

// Wrapper for elog macro (variadic)
pub fn elog(level: c_int, msg: []const u8) void {
    // Convert Zig string to C string
    const c_msg = c.palloc(msg.len + 1);
    @memcpy(@as([*]u8, @ptrCast(c_msg))[0..msg.len], msg);
    @as([*]u8, @ptrCast(c_msg))[msg.len] = 0;
    elog_helper(level, @as([*c]const u8, @ptrCast(c_msg)));
}

// Format and log an error message with values
// This safely formats error messages with actual values for better debugging
pub fn elogFmt(level: c_int, comptime fmt: []const u8, args: anytype) void {
    // Use a reasonable buffer size for error messages (512 bytes should be enough)
    var buf: [512]u8 = undefined;
    const msg = std.fmt.bufPrintZ(&buf, fmt, args) catch {
        // If formatting fails, fall back to a simple message
        elog(level, "Error message formatting failed");
        return;
    };
    elog(level, msg);
}

// Format and log an error message with function context
// Automatically prefixes the function name to the error message
pub fn elogWithContext(level: c_int, comptime func_name: []const u8, comptime msg: []const u8) void {
    elogFmt(level, func_name ++ ": " ++ msg, .{});
}

// Wrapper for work_mem global variable
pub fn workMem() c_int {
    return work_mem_helper();
}

// Wrapper for DatumGetTextP macro
pub fn datumGetTextP(d: c.Datum) [*c]c.struct_varlena {
    return datum_get_textp_helper(d);
}

// Wrapper for VARSIZE_ANY_EXHDR macro
pub fn varsizeAnyExhdr(ptr: [*c]c.struct_varlena) c_int {
    return varsize_any_exhdr_helper(ptr);
}

// Wrapper for VARDATA_ANY macro
pub fn vardataAny(ptr: [*c]c.struct_varlena) [*c]u8 {
    return vardata_any_helper(ptr);
}

// Wrapper for text_to_cstring - safest way to extract text from datum
// Returns a palloc'd null-terminated string (caller must pfree)
pub fn textToCstring(d: c.Datum) [*c]u8 {
    return text_to_cstring_helper(d);
}

// Wrapper for strlen - safely get length of C string
pub fn strlen(str: [*c]const u8) usize {
    return strlen_helper(str);
}

/// Truncate a UTF-8 string to a maximum byte length, ensuring we don't cut in the middle of a UTF-8 character
/// Returns the safe byte length (<= max_bytes) that doesn't break UTF-8 encoding
pub fn truncateUtf8Safe(bytes: []const u8, max_bytes: usize) usize {
    if (bytes.len <= max_bytes) {
        return bytes.len;
    }
    
    // Start from max_bytes and work backwards to find a valid UTF-8 boundary
    var pos: usize = max_bytes;
    
    // UTF-8 continuation bytes have the pattern 10xxxxxx (0x80-0xBF)
    // We need to find the start of the last complete UTF-8 character that fits within max_bytes
    while (pos > 0) {
        const byte = bytes[pos - 1];
        
        // If this byte is not a continuation byte (0x80-0xBF), we found a potential character start
        if (byte & 0xC0 != 0x80) {
            // Determine how many bytes this UTF-8 character needs
            var char_len: usize = 1;
            if ((byte & 0xE0) == 0xC0) {
                char_len = 2; // 2-byte UTF-8 character
            } else if ((byte & 0xF0) == 0xE0) {
                char_len = 3; // 3-byte UTF-8 character
            } else if ((byte & 0xF8) == 0xF0) {
                char_len = 4; // 4-byte UTF-8 character
            }
            
            // Check if this character fits within max_bytes
            // pos-1 is where the character starts, so we need pos-1 + char_len <= max_bytes
            if ((pos - 1) + char_len <= max_bytes) {
                // This character fits - truncate after it
                return (pos - 1) + char_len;
            }
            // Character doesn't fit - continue backwards
        }
        
        pos -= 1;
        
        // Safety: if we go too far back, just truncate at a safe point
        // Worst case: we might cut a 4-byte UTF-8 character, so go back 4 bytes
        if (pos + 4 < max_bytes) {
            return if (max_bytes >= 4) max_bytes - 4 else 0;
        }
    }
    
    // Fallback: return 0 if we can't find a safe boundary
    return 0;
}

pub fn get_arg_datum(fcinfo: c.FunctionCallInfo, n: usize) c.Datum {
    return fcinfo_get_arg_value_helper(fcinfo, @intCast(n));
}

pub fn is_arg_null(fcinfo: c.FunctionCallInfo, n: usize) bool {
    return fcinfo_get_arg_isnull_helper(fcinfo, @intCast(n));
}

pub fn set_return_null(fcinfo: c.FunctionCallInfo) void {
    fcinfo_set_isnull_helper(fcinfo, true);
}

// PostgreSQL Allocator
// Compatible with Zig 0.15.2+
pub const PgAllocator = struct {
    pub fn allocator() std.mem.Allocator {
        return std.mem.Allocator{
            .ptr = undefined,
            .vtable = &vtable,
        };
    }

    const vtable = std.mem.Allocator.VTable{
        .alloc = alloc,
        .resize = resize,
        .free = free,
        .remap = remap,
    };

    fn alloc(_: *anyopaque, len: usize, _: std.mem.Alignment, _: usize) ?[*]u8 {
        const ptr = c.palloc(len);
        if (ptr == null) return null;
        return @ptrCast(ptr);
    }

    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }

    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }

    fn free(_: *anyopaque, buf: []u8, _: std.mem.Alignment, _: usize) void {
        c.pfree(buf.ptr);
    }
};

