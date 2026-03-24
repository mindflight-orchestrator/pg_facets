const std = @import("std");
const avx2_utils = @import("avx2_utils.zig");

/// Pure Zig tokenizer - no PostgreSQL dependencies
/// This tokenizer does simple word splitting and lowercasing
/// It doesn't do stemming (like PostgreSQL's to_tsvector), but it's safe to call
/// from any context without SPI.
///
/// NOTE: The tokens produced by this tokenizer may not exactly match those from
/// to_tsvector. For consistent BM25 scoring, the same tokenizer should be used
/// for both indexing and querying. Since indexing uses to_tsvector, queries
/// should ideally also use to_tsvector. However, when SPI is not available
/// (e.g., inside an EXECUTE statement), this pure tokenizer provides a fallback.

/// Common English stop words to filter out
const ENGLISH_STOP_WORDS = [_][]const u8{
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
    "has", "have", "he", "in", "is", "it", "its", "of", "on", "or",
    "that", "the", "this", "to", "was", "were", "will", "with",
};

/// Check if a word is a stop word
fn isStopWord(word: []const u8) bool {
    for (ENGLISH_STOP_WORDS) |stop| {
        if (std.mem.eql(u8, word, stop)) {
            return true;
        }
    }
    return false;
}

/// Simple lowercase conversion for ASCII
fn toLowerAscii(char: u8) u8 {
    if (char >= 'A' and char <= 'Z') {
        return char + 32;
    }
    return char;
}

/// Check if a character is a word character (alphanumeric)
fn isWordChar(char: u8) bool {
    return (char >= 'a' and char <= 'z') or
           (char >= 'A' and char <= 'Z') or
           (char >= '0' and char <= '9') or
           (char >= 0x80); // Keep high bytes for UTF-8
}

/// Tokenize text into words (pure Zig, no PostgreSQL)
/// Returns a list of lowercase tokens with stop words removed
pub fn tokenizePure(
    text: []const u8,
    allocator: std.mem.Allocator
) !std.ArrayList([]const u8) {
    var tokens = std.ArrayList([]const u8).empty;
    errdefer {
        for (tokens.items) |token| {
            allocator.free(token);
        }
        tokens.deinit(allocator);
    }
    
    var i: usize = 0;
    while (i < text.len) {
        // Skip non-word characters
        while (i < text.len and !isWordChar(text[i])) {
            i += 1;
        }
        
        if (i >= text.len) break;
        
        // Find end of word
        const start = i;
        while (i < text.len and isWordChar(text[i])) {
            i += 1;
        }
        
        const word = text[start..i];
        if (word.len == 0 or word.len > 100) continue; // Skip empty or very long words
        
        // Convert to lowercase
        var lower = try allocator.alloc(u8, word.len);
        for (word, 0..) |char, j| {
            lower[j] = toLowerAscii(char);
        }
        
        // Skip stop words and very short words
        if (lower.len <= 1 or isStopWord(lower)) {
            allocator.free(lower);
            continue;
        }
        
        try tokens.append(allocator, lower);
    }
    
    return tokens;
}

/// Calculate hash of a lexeme (for term_hash)
/// Uses the same algorithm as tokenizer_native for consistency
pub fn hashLexeme(lexeme: []const u8) i64 {
    return avx2_utils.hashLexemeAVX2(lexeme);
}

/// Tokenize and return hashes directly
pub fn tokenizeToHashes(
    text: []const u8,
    allocator: std.mem.Allocator
) !std.ArrayList(i64) {
    var tokens = try tokenizePure(text, allocator);
    defer {
        for (tokens.items) |token| {
            allocator.free(token);
        }
        tokens.deinit(allocator);
    }
    
    var hashes = std.ArrayList(i64).empty;
    errdefer hashes.deinit(allocator);
    
    for (tokens.items) |token| {
        try hashes.append(allocator, hashLexeme(token));
    }
    
    return hashes;
}

test "tokenizePure basic" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    const tokens = try tokenizePure("Hello World", allocator);
    defer {
        for (tokens.items) |token| {
            allocator.free(token);
        }
        tokens.deinit(allocator);
    }
    
    try std.testing.expectEqual(@as(usize, 2), tokens.items.len);
    try std.testing.expectEqualStrings("hello", tokens.items[0]);
    try std.testing.expectEqualStrings("world", tokens.items[1]);
}

test "tokenizePure with stop words" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    const tokens = try tokenizePure("the quick brown fox", allocator);
    defer {
        for (tokens.items) |token| {
            allocator.free(token);
        }
        tokens.deinit(allocator);
    }
    
    // "the" is a stop word, should be filtered
    try std.testing.expectEqual(@as(usize, 3), tokens.items.len);
    try std.testing.expectEqualStrings("quick", tokens.items[0]);
    try std.testing.expectEqualStrings("brown", tokens.items[1]);
    try std.testing.expectEqualStrings("fox", tokens.items[2]);
}

test "tokenizeToHashes" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    const hashes = try tokenizeToHashes("laptop computer", allocator);
    defer hashes.deinit(allocator);
    
    try std.testing.expectEqual(@as(usize, 2), hashes.items.len);
    // Hashes should be consistent
    try std.testing.expectEqual(hashLexeme("laptop"), hashes.items[0]);
    try std.testing.expectEqual(hashLexeme("computer"), hashes.items[1]);
}

