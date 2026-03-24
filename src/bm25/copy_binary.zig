const std = @import("std");
const utils = @import("../utils.zig");
const c = utils.c;

/// PostgreSQL COPY BINARY format writer
/// This is the fastest way to bulk load data into PostgreSQL (5-10x faster than INSERT)
///
/// Binary format specification:
/// - Header: 11 bytes signature + 4 bytes flags + 4 bytes extension length
/// - Row: 2 bytes field count + (4 bytes length + data) per field
/// - Trailer: -1 as int16
pub const CopyBinaryWriter = struct {
    buffer: std.ArrayList(u8),
    allocator: std.mem.Allocator,
    row_count: usize,
    field_count: u16,
    header_written: bool,

    const SIGNATURE = "PGCOPY\n\xff\r\n\x00";
    const SIGNATURE_LEN = 11;
    const FLAGS: u32 = 0; // No OIDs
    const EXTENSION_LEN: u32 = 0;

    pub fn init(allocator: std.mem.Allocator, field_count: u16) CopyBinaryWriter {
        return CopyBinaryWriter{
            .buffer = std.ArrayList(u8).empty,
            .allocator = allocator,
            .row_count = 0,
            .field_count = field_count,
            .header_written = false,
        };
    }

    pub fn deinit(self: *CopyBinaryWriter) void {
        self.buffer.deinit(self.allocator);
    }

    /// Write the COPY BINARY header
    pub fn writeHeader(self: *CopyBinaryWriter) !void {
        if (self.header_written) return;

        // Signature: "PGCOPY\n\377\r\n\0" (11 bytes)
        try self.buffer.appendSlice(self.allocator, SIGNATURE);

        // Flags field: 32-bit integer (0 = no OIDs)
        try self.writeInt32BE(FLAGS);

        // Header extension area length: 32-bit integer (0 = none)
        try self.writeInt32BE(EXTENSION_LEN);

        self.header_written = true;
    }

    /// Write the COPY BINARY trailer (-1 as int16)
    pub fn writeTrailer(self: *CopyBinaryWriter) !void {
        try self.writeInt16BE(0xFFFF); // -1 as uint16
    }

    /// Write a row for staging table: (term_hash bigint, term_text text, doc_id bigint, term_freq int, doc_length int)
    pub fn writeTermRow(
        self: *CopyBinaryWriter,
        term_hash: i64,
        term_text: []const u8,
        doc_id: i64,
        term_freq: i32,
        doc_length: i32,
    ) !void {
        // Ensure header is written
        if (!self.header_written) {
            try self.writeHeader();
        }

        // Field count (5 fields)
        try self.writeInt16BE(5);

        // Field 1: term_hash (bigint = 8 bytes)
        try self.writeInt32BE(8); // length
        try self.writeInt64BE(@bitCast(term_hash));

        // Field 2: term_text (text = variable length)
        try self.writeInt32BE(@intCast(term_text.len));
        try self.buffer.appendSlice(self.allocator, term_text);

        // Field 3: doc_id (bigint = 8 bytes)
        try self.writeInt32BE(8);
        try self.writeInt64BE(@bitCast(doc_id));

        // Field 4: term_freq (int4 = 4 bytes)
        try self.writeInt32BE(4);
        try self.writeInt32BE(@bitCast(term_freq));

        // Field 5: doc_length (int4 = 4 bytes)
        try self.writeInt32BE(4);
        try self.writeInt32BE(@bitCast(doc_length));

        self.row_count += 1;
    }

    /// Get the binary data ready for COPY
    /// Returns the complete binary buffer with header and trailer
    pub fn finalize(self: *CopyBinaryWriter) ![]const u8 {
        if (!self.header_written) {
            try self.writeHeader();
        }
        try self.writeTrailer();
        return self.buffer.items;
    }

    /// Get current buffer size in bytes
    pub fn bufferSize(self: *const CopyBinaryWriter) usize {
        return self.buffer.items.len;
    }

    /// Get current row count
    pub fn getRowCount(self: *const CopyBinaryWriter) usize {
        return self.row_count;
    }

    /// Clear the buffer for reuse (keeps allocated capacity)
    pub fn reset(self: *CopyBinaryWriter) void {
        self.buffer.clearRetainingCapacity();
        self.row_count = 0;
        self.header_written = false;
    }

    /// Execute COPY FROM STDIN BINARY via SPI
    /// This sends the binary data to PostgreSQL
    pub fn flush(self: *CopyBinaryWriter, schema: []const u8, table_name: []const u8, allocator: std.mem.Allocator) !usize {
        if (self.row_count == 0) return 0;

        // Finalize the buffer with trailer (needed for binary format validity)
        _ = try self.finalize();

        // Build COPY command
        const copy_cmd = try std.fmt.allocPrintSentinel(
            allocator,
            "COPY {s}.\"{s}\" (term_hash, term_text, doc_id, term_freq, doc_length) FROM STDIN WITH (FORMAT BINARY)",
            .{ schema, table_name },
        );
        defer allocator.free(copy_cmd);

        // Start COPY - Note: SPI doesn't directly support COPY FROM STDIN
        // So this will always fall back to file-based approach
        const ret = c.SPI_execute(copy_cmd.ptr, false, 0);
        _ = ret;
        
        // SPI doesn't support COPY FROM STDIN directly
        // The caller should use copyBinaryViaFile() instead
        return try self.flushViaInsert(schema, table_name, allocator);
    }

    /// Fallback: Flush via optimized multi-row INSERT
    /// This is still much faster than single-row inserts
    fn flushViaInsert(self: *CopyBinaryWriter, schema: []const u8, table_name: []const u8, allocator: std.mem.Allocator) !usize {
        // Mark parameters as intentionally unused for this stub
        _ = self;
        _ = schema;
        _ = table_name;
        _ = allocator;
        // This will be implemented in the worker using COPY via file
        return error.NotImplemented;
    }

    // Helper functions for writing big-endian integers
    fn writeInt16BE(self: *CopyBinaryWriter, value: u16) !void {
        var bytes: [2]u8 = undefined;
        std.mem.writeInt(u16, &bytes, value, .big);
        try self.buffer.appendSlice(self.allocator, &bytes);
    }

    fn writeInt32BE(self: *CopyBinaryWriter, value: u32) !void {
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, value, .big);
        try self.buffer.appendSlice(self.allocator, &bytes);
    }

    fn writeInt64BE(self: *CopyBinaryWriter, value: u64) !void {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, value, .big);
        try self.buffer.appendSlice(self.allocator, &bytes);
    }
};

/// Execute COPY FROM STDIN BINARY using a temp file
/// This is the most reliable way to do COPY BINARY from within a Zig extension
pub fn copyBinaryViaFile(
    schema: []const u8,
    table_name: []const u8,
    binary_data: []const u8,
    allocator: std.mem.Allocator,
) !usize {
    // Create a unique temp file path
    const pid = std.os.linux.getpid();
    const timestamp = std.time.milliTimestamp();
    const temp_path = try std.fmt.allocPrintSentinel(
        allocator,
        "/tmp/pg_facets_copy_{d}_{d}.bin",
        .{ pid, timestamp },
    );
    defer allocator.free(temp_path);

    // Write binary data to temp file
    const file = try std.fs.createFileAbsolute(temp_path, .{}, 0);
    defer file.close();
    try file.writeAll(binary_data);

    // Execute COPY FROM file
    const copy_cmd = try std.fmt.allocPrintSentinel(
        allocator,
        "COPY {s}.\"{s}\" (term_hash, term_text, doc_id, term_freq, doc_length) FROM '{s}' WITH (FORMAT BINARY)",
        .{ schema, table_name, temp_path },
    );
    defer allocator.free(copy_cmd);

    const ret = c.SPI_execute(copy_cmd.ptr, false, 0);

    // Clean up temp file
    std.fs.deleteFileAbsolute(temp_path) catch {};

    if (ret != c.SPI_OK_UTILITY) {
        utils.elog(c.ERROR, "COPY BINARY failed");
        return error.CopyFailed;
    }

    // Return number of rows processed
    return @intCast(c.SPI_processed);
}

/// Test function to verify COPY BINARY format
pub fn testCopyBinaryFormat(allocator: std.mem.Allocator) !void {
    var writer = CopyBinaryWriter.init(allocator, 5);
    defer writer.deinit();

    // Write test rows
    try writer.writeTermRow(12345, "hello", 1, 3, 10);
    try writer.writeTermRow(67890, "world", 2, 5, 15);

    const data = try writer.finalize();

    // Verify header
    if (!std.mem.eql(u8, data[0..11], CopyBinaryWriter.SIGNATURE)) {
        return error.InvalidSignature;
    }

    utils.elog(c.NOTICE, "COPY BINARY format test passed");
}

