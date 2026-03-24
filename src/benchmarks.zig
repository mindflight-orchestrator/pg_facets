const std = @import("std");
const utils = @import("utils.zig");
const tokenizer_pure = @import("bm25/tokenizer_pure.zig");

/// Benchmark results
pub const BenchmarkResult = struct {
    operation: []const u8,
    iterations: usize,
    total_time_ns: u64,
    avg_time_ns: u64,
    min_time_ns: u64,
    max_time_ns: u64,
    throughput: f64, // operations per second

    pub fn format(self: BenchmarkResult, writer: anytype) !void {
        try writer.print("{s}: {} iterations, avg: {}ns, min: {}ns, max: {}ns, throughput: {d:.2} ops/sec\n",
            .{ self.operation, self.iterations, self.avg_time_ns, self.min_time_ns, self.max_time_ns, self.throughput }, 0);
    }
};

/// Run a benchmark function multiple times and collect statistics
pub fn benchmarkFunction(
    comptime name: []const u8,
    comptime func: anytype,
    iterations: usize
) !BenchmarkResult {
    const allocator = std.heap.page_allocator;
    var times = std.ArrayList(u64).empty;
    defer times.deinit(allocator);

    var total_time: u64 = 0;

    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        const start = std.time.nanoTimestamp();

        // Call the benchmark function - assume it takes no arguments for simplicity
        _ = @call(.auto, func, .{}, 0);

        const end = std.time.nanoTimestamp();
        const duration = @as(u64, @intCast(end - start));

        try times.append(allocator, duration);
        total_time += duration;
    }

    // Calculate statistics
    var min_time = times.items[0];
    var max_time = times.items[0];

    for (times.items) |time| {
        if (time < min_time) min_time = time;
        if (time > max_time) max_time = time;
    }

    const avg_time = total_time / iterations;
    const throughput = @as(f64, @floatFromInt(iterations)) / (@as(f64, @floatFromInt(total_time)) / 1_000_000_000.0);

    return BenchmarkResult{
        .operation = name,
        .iterations = iterations,
        .total_time_ns = total_time,
        .avg_time_ns = avg_time,
        .min_time_ns = min_time,
        .max_time_ns = max_time,
        .throughput = throughput,
    };
}

// Benchmark functions

/// Benchmark pure tokenization performance
fn benchmarkTokenization(iterations: usize) !BenchmarkResult {
    const test_text = "This is a test document with some words to tokenize for performance measurement";

    return try benchmarkFunction(
        "Pure Tokenization",
        struct {
            fn tokenize() void {
                const alloc = std.heap.page_allocator;
                var tokens = tokenizer_pure.tokenizePure(test_text, alloc) catch return;
                defer {
                    for (tokens.items) |token| {
                        alloc.free(token);
                    }
                    tokens.deinit(alloc);
                }
                // Prevent optimization of the tokenization work
                for (tokens.items) |token| {
                    std.mem.doNotOptimizeAway(token.len);
                }
            }
        }.tokenize,
        iterations
    );
}


/// Benchmark UTF-8 truncation
fn benchmarkUtf8Truncation(iterations: usize) !BenchmarkResult {
    const test_text = "This is a very long UTF-8 string with émojis 🚀 and special characters 中文 that needs to be truncated safely to prevent buffer overflows and ensure proper encoding boundaries are respected.";
    const max_bytes = 50;

    return try benchmarkFunction(
        "UTF-8 Safe Truncation",
        struct {
            fn truncate() void {
                const result = utils.truncateUtf8Safe(test_text, max_bytes);
                std.mem.doNotOptimizeAway(result);
            }
        }.truncate,
        iterations
    );
}

/// Benchmark hash calculation
fn benchmarkHashCalculation(iterations: usize) !BenchmarkResult {
    const test_terms = [_][]const u8{
        "test", "document", "search", "bm25", "algorithm", "performance", "benchmark",
        "tokenization", "indexing", "query", "term", "frequency", "inverse", "document",
        "frequency", "scoring", "ranking", "relevance"
    };

    return try benchmarkFunction(
        "Term Hash Calculation",
        struct {
            fn hashCalc() void {
                for (test_terms) |term| {
                    const hash_val = tokenizer_pure.hashLexeme(term);
                    std.mem.doNotOptimizeAway(hash_val);
                }
            }
        }.hashCalc,
        iterations
    );
}

/// Run all benchmarks and print results
pub fn runBenchmarks(_: std.mem.Allocator) !void {
    std.debug.print("Running pg_facets performance benchmarks...\n", .{}, 0);

    const benchmark_functions = [_]struct {
        name: []const u8,
        func: *const fn(usize) anyerror!BenchmarkResult,
        iterations: usize,
    }{
        .{ .name = "Tokenization", .func = benchmarkTokenization, .iterations = 5 },
        .{ .name = "UTF-8 Truncation", .func = benchmarkUtf8Truncation, .iterations = 10 },
        .{ .name = "Hash Calculation", .func = benchmarkHashCalculation, .iterations = 10 },
    };

    for (benchmark_functions) |bench| {
        std.debug.print("Running {s} benchmark...\n", .{bench.name}, 0);
        const result = try bench.func(bench.iterations);
        std.debug.print("{s}: {} iterations, avg: {}ns, throughput: {d:.0} ops/sec\n",
            .{result.operation, result.iterations, result.avg_time_ns, result.throughput}, 0);
    }

    std.debug.print("Benchmarking complete!\n", .{}, 0);
}

// Benchmark test that can be run with Zig's test framework
test "benchmarks" {
    try runBenchmarks(std.testing.allocator);
}
