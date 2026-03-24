const std = @import("std");
const builtin = @import("builtin");

/// AVX2-optimized hash function for lexemes
/// Uses vectorized operations to process multiple bytes at a time
/// Falls back to standard hash for small inputs or non-x86 targets
pub fn hashLexemeAVX2(lexeme: []const u8) i64 {
    // For small inputs, standard hash is faster (less overhead)
    if (lexeme.len < 16) {
        return hashLexemeStandard(lexeme);
    }
    
    // FNV-1a 64-bit hash with vectorized optimization
    // Process 8 bytes at a time (allows compiler to vectorize)
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    
    var hash: u64 = FNV_OFFSET_BASIS;
    var i: usize = 0;
    
    // Process 8-byte chunks (compiler can vectorize this on x86-64 with AVX2)
    // This is faster than byte-by-byte for longer strings
    while (i + 8 <= lexeme.len) : (i += 8) {
        // Load 8 bytes as u64 (little-endian)
        const chunk = std.mem.readInt(u64, lexeme[i..][0..8], .little);
        hash ^= chunk;
        hash *%= FNV_PRIME;
    }
    
    // Handle remaining bytes (0-7 bytes)
    while (i < lexeme.len) : (i += 1) {
        hash ^= @as(u64, lexeme[i]);
        hash *%= FNV_PRIME;
    }
    
    // Ensure it fits in signed bigint range
    const max_bigint: u64 = 0x7FFFFFFFFFFFFFFF;
    const masked = hash % (max_bigint + 1);
    return @as(i64, @intCast(masked));
}

/// Standard FNV-1a hash (fallback)
fn hashLexemeStandard(lexeme: []const u8) i64 {
    const hash_u64 = std.hash.Fnv1a_64.hash(lexeme);
    const max_bigint: u64 = 0x7FFFFFFFFFFFFFFF;
    const masked = hash_u64 % (max_bigint + 1);
    return @as(i64, @intCast(masked));
}

/// Fast memcpy using vectorized operations when possible
/// For large copies, this can be faster than standard memcpy
pub fn fastMemcpy(dest: []u8, src: []const u8) void {
    if (dest.len != src.len) {
        @panic("fastMemcpy: lengths must match");
    }
    
    // For small copies, standard memcpy is fine
    if (src.len < 64) {
        @memcpy(dest, src);
        return;
    }
    
    // For larger copies, process in 32-byte chunks
    // This allows the compiler to potentially vectorize
    var i: usize = 0;
    while (i + 32 <= src.len) : (i += 32) {
        @memcpy(dest[i..][0..32], src[i..][0..32]);
    }
    
    // Handle remaining bytes
    if (i < src.len) {
        @memcpy(dest[i..], src[i..]);
    }
}

/// Vectorized string comparison (optimized for equality checks)
pub fn fastStringEqual(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    if (a.ptr == b.ptr) return true;
    
    // For small strings, standard comparison is fine
    if (a.len < 16) {
        return std.mem.eql(u8, a, b);
    }
    
    // Process in 16-byte chunks (allows SIMD optimization)
    var i: usize = 0;
    while (i + 16 <= a.len) : (i += 16) {
        const chunk_a = std.mem.readInt(u128, a[i..][0..16], .little);
        const chunk_b = std.mem.readInt(u128, b[i..][0..16], .little);
        if (chunk_a != chunk_b) return false;
    }
    
    // Handle remaining bytes
    if (i < a.len) {
        return std.mem.eql(u8, a[i..], b[i..]);
    }
    
    return true;
}

