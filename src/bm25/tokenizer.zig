const std = @import("std");
const utils = @import("../utils.zig");
const c = utils.c;
const PgAllocator = utils.PgAllocator;

/// Token extracted from text using PostgreSQL's to_tsvector
pub const Token = struct {
    lexeme: []const u8,
    positions: []u32, // Positions in document (1-indexed)
};

/// Tokenize text using PostgreSQL's to_tsvector
/// Uses PostgreSQL's text search configs (english_stem, french_stem, etc.)
/// If SPI is already connected, it will reuse the existing connection
pub fn tokenize(
    text: []const u8,
    config_name: []const u8,
    allocator: std.mem.Allocator
) !std.ArrayList([]const u8) {
    // Try to connect - may already be connected from caller
    const conn_result = c.SPI_connect();
    const need_finish = (conn_result == c.SPI_OK_CONNECT);
    if (conn_result != c.SPI_OK_CONNECT and conn_result != c.SPI_ERROR_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed unexpectedly");
        return error.SPIConnectFailed;
    }
    defer if (need_finish) {
        _ = c.SPI_finish();
    };
    
    return tokenizeWithExistingConnection(text, config_name, allocator);
}

/// Tokenize text assuming SPI is already connected
/// Internal helper to avoid nested SPI connections
/// WARNING: Caller must ensure SPI is connected before calling this
pub fn tokenizeWithExistingConnection(
    text: []const u8,
    config_name: []const u8,
    allocator: std.mem.Allocator
) !std.ArrayList([]const u8) {
    // We need to copy the lexemes string before SPI_finish releases the memory
    // Use a fixed-size buffer for the lexemes string (should be enough for most cases)
    const MAX_LEXEMES_LEN = 65536;
    var lexemes_buf: [MAX_LEXEMES_LEN]u8 = undefined;
    var lexemes_len: usize = 0;
    
    // Phase 1: Execute SPI and copy result to local buffer
    {
        
        // Tokenize using to_tsvector(regconfig, text). We pass the config as a parameter and
        // cast it to regconfig in SQL to avoid quoting/escaping issues.
        const query = "SELECT array_to_string(tsvector_to_array(to_tsvector($1::regconfig, $2)), ' ') AS lexemes";
        
        // Prepare arguments ($1=config, $2=text)
        // Use cstring_to_text_with_len which explicitly takes pointer and length
        var argtypes = [_]c.Oid{ c.TEXTOID, c.TEXTOID };
        const config_datum = c.PointerGetDatum(c.cstring_to_text_with_len(@ptrCast(config_name.ptr), @intCast(config_name.len)));
        const text_datum = c.PointerGetDatum(c.cstring_to_text_with_len(@ptrCast(text.ptr), @intCast(text.len)));
        var argvalues = [_]c.Datum{ config_datum, text_datum };
        var argnulls = [_]u8{ ' ', ' ' };
        
        // Execute query
        const ret = c.SPI_execute_with_args(
            query.ptr,
            2,
            &argtypes,
            &argvalues,
            &argnulls,
            true, // read_only
            1     // limit
        );
        
        if (ret != c.SPI_OK_SELECT) {
            utils.elog(c.ERROR, "Failed to execute tokenization query");
            return error.QueryFailed;
        }
        
        if (c.SPI_processed == 0) {
            return std.ArrayList([]const u8).empty;
        }
        
        // Get result
        const tuple = c.SPI_tuptable.*.vals[0];
        const tupdesc = c.SPI_tuptable.*.tupdesc;
        
        var isnull: bool = false;
        const lexemes_datum = c.SPI_getbinval(tuple, tupdesc, 1, &isnull);
        
        if (isnull) {
            return std.ArrayList([]const u8).empty;
        }
        
        // Extract text via text_to_cstring() (handles detoasting safely) and copy to local buffer
        const lexemes_cstr = utils.textToCstring(lexemes_datum);
        if (lexemes_cstr == null) {
            utils.elog(c.ERROR, "Failed to extract tokenization result");
            return error.QueryFailed;
        }
        defer c.pfree(@ptrCast(lexemes_cstr));
        
        // Bounded strlen (protect against corrupted memory / missing terminator)
        var result_len: usize = 0;
        while (result_len < MAX_LEXEMES_LEN and lexemes_cstr[result_len] != 0) {
            result_len += 1;
        }
        if (result_len >= MAX_LEXEMES_LEN) {
            utils.elog(c.ERROR, "Lexemes string too long or not null-terminated");
            return error.BufferOverflow;
        }
        
        if (result_len > 0) {
            @memcpy(lexemes_buf[0..result_len], lexemes_cstr[0..result_len]);
            lexemes_len = result_len;
        }
        // SPI_finish called here via defer
    }
    
    // Phase 2: Parse lexemes from local buffer (outside SPI context)
    var tokens = std.ArrayList([]const u8).empty;
    
    if (lexemes_len == 0) {
        return tokens;
    }
    
    const lexemes_str = lexemes_buf[0..lexemes_len];
    
    // Split by space
    var it = std.mem.splitScalar(u8, lexemes_str, ' ');
    while (it.next()) |lexeme| {
        if (lexeme.len > 0) {
            // Allocate and copy lexeme (now in function's memory context)
            const lexeme_copy = try allocator.alloc(u8, lexeme.len);
            @memcpy(lexeme_copy, lexeme);
            try tokens.append(allocator, lexeme_copy);
        }
    }
    
    return tokens;
}

/// Get unique tokens (deduplicated)
pub fn getUniqueTokens(
    tokens: []const []const u8,
    allocator: std.mem.Allocator
) !std.ArrayList([]const u8) {
    var unique = std.ArrayList([]const u8).empty;
    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();
    
    for (tokens) |token| {
        const entry = try seen.getOrPut(token);
        if (!entry.found_existing) {
            const token_copy = try allocator.alloc(u8, token.len);
            @memcpy(token_copy, token);
            try unique.append(allocator, token_copy);
        }
    }
    
    return unique;
}

/// Calculate hash of a lexeme (for term_hash)
/// Returns a value that fits in PostgreSQL's signed bigint range
pub fn hashLexeme(lexeme: []const u8) i64 {
    // Use FNV-1a 64-bit hash, then ensure it fits in signed bigint range
    const hash_u64 = std.hash.Fnv1a_64.hash(lexeme);
    // PostgreSQL bigint is signed (range: -2^63 to 2^63-1)
    // Use modulo to ensure it fits: hash % (2^63) gives us 0 to 2^63-1
    // Then subtract 2^62 to center it around 0 for better distribution
    const max_bigint: u64 = 0x7FFFFFFFFFFFFFFF; // 2^63 - 1
    const masked = hash_u64 % (max_bigint + 1);
    return @as(i64, @intCast(masked));
}
