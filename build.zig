const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create root module (Zig 0.15: addSharedLibrary replaced by addLibrary + Module)
    const root_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    const lib = b.addLibrary(.{
        .name = "pg_facets",
        .root_module = root_mod,
        .linkage = .dynamic,
    });

    // Find PostgreSQL include path (primary from pg_config, with fallbacks for version mismatch)
    var pg_server_include: []const u8 = "/usr/include/postgresql/server"; // Default fallback

    const run_result = std.process.Child.run(.{
        .allocator = b.allocator,
        .argv = &.{ "pg_config", "--includedir-server" },
    }) catch null;

    if (run_result) |res| {
        switch (res.term) {
            .Exited => |code| {
                if (code == 0) {
                    pg_server_include = std.mem.trim(u8, res.stdout, " \n\r\t");
                }
            },
            else => {},
        }
    }

    root_mod.addSystemIncludePath(.{ .cwd_relative = pg_server_include });
    // Fallback paths when pg_config version doesn't match installed headers (e.g. pg 17 vs 16)
    root_mod.addSystemIncludePath(.{ .cwd_relative = "/usr/include/postgresql/16/server" });
    root_mod.addSystemIncludePath(.{ .cwd_relative = "/usr/include/postgresql/17/server" });
    // macOS Homebrew paths (Intel: /usr/local, Apple Silicon: /opt/homebrew)
    root_mod.addSystemIncludePath(.{ .cwd_relative = "/opt/homebrew/opt/postgresql@17/include/server" });
    root_mod.addSystemIncludePath(.{ .cwd_relative = "/usr/local/opt/postgresql@17/include/server" });
    // Vendored PostgreSQL (Option A: ext_pg17_src submodule, built to .pg_install)
    root_mod.addSystemIncludePath(.{ .cwd_relative = "../../.pg_install/include/server" });
    
    // Add pg_roaringbitmap include path (from deps submodule)
    root_mod.addIncludePath(.{ .cwd_relative = "deps/pg_roaringbitmap" });

    // Add C helper file
    root_mod.addCSourceFile(.{ .file = b.path("src/filter_helper.c"), .flags = &.{} });

    // Check if it's safe to compile for AVX2/AVX-512
    // This works in Docker because we check the TARGET, not the host CPU
    const resolved_target = target; // standardTargetOptions already returns ResolvedTarget
    const is_x86_64 = resolved_target.result.cpu.arch == .x86_64;
    
    // Get CPU features from resolved target
    const cpu_features = resolved_target.result.cpu.features;
    const x86_feature = @import("std").Target.x86.Feature;
    const has_avx512f = is_x86_64 and cpu_features.isEnabled(@intFromEnum(x86_feature.avx512f));
    const has_avx512dq = is_x86_64 and cpu_features.isEnabled(@intFromEnum(x86_feature.avx512dq));
    const has_avx512bw = is_x86_64 and cpu_features.isEnabled(@intFromEnum(x86_feature.avx512bw));
    const has_avx512vbmi2 = is_x86_64 and cpu_features.isEnabled(@intFromEnum(x86_feature.avx512vbmi2));
    const has_avx512bitalg = is_x86_64 and cpu_features.isEnabled(@intFromEnum(x86_feature.avx512bitalg));
    const has_avx512vpopcntdq = is_x86_64 and cpu_features.isEnabled(@intFromEnum(x86_feature.avx512vpopcntdq));
    
    // AVX-512 requires all these features to be safe
    const supports_avx512 = has_avx512f and has_avx512dq and has_avx512bw and 
                            has_avx512vbmi2 and has_avx512bitalg and has_avx512vpopcntdq;
    
    // Build flags for CRoaring (Zig 0.15: ArrayList is Unmanaged, use .empty + append(allocator))
    var roaring_flags = std.ArrayList([]const u8).empty;
    defer roaring_flags.deinit(b.allocator);

    if (supports_avx512) {
        // Enable AVX-512 if all required features are present
        roaring_flags.append(b.allocator, "-DCROARING_COMPILER_SUPPORTS_AVX512=1") catch @panic("OOM");
    } else {
        // Disable AVX-512 if any required feature is missing
        // This is safe for Docker builds - we check the target, not the host
        roaring_flags.append(b.allocator, "-DCROARING_COMPILER_SUPPORTS_AVX512=0") catch @panic("OOM");
    }

    // Add CRoaring source (bundled in pg_roaringbitmap root)
    // AVX-512 support is conditionally enabled based on target CPU features
    root_mod.addCSourceFile(.{
        .file = .{ .cwd_relative = "deps/pg_roaringbitmap/roaring.c" },
        .flags = roaring_flags.items,
    });

    lib.linker_allow_shlib_undefined = true;

    b.installArtifact(lib);

    // Add unit tests (Zig 0.15: addTest uses root_module)
    const unit_tests_mod = b.createModule(.{
        .root_source_file = b.path("src/test_utils.zig"),
        .target = target,
        .optimize = optimize,
    });
    const unit_tests = b.addTest(.{
        .root_module = unit_tests_mod,
    });

    // Add memory safety tests
    const memory_safety_mod = b.createModule(.{
        .root_source_file = b.path("src/memory_safety_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    memory_safety_mod.addSystemIncludePath(.{ .cwd_relative = pg_server_include });
    memory_safety_mod.addSystemIncludePath(.{ .cwd_relative = "/usr/include/postgresql/16/server" });
    memory_safety_mod.addSystemIncludePath(.{ .cwd_relative = "/usr/include/postgresql/17/server" });
    memory_safety_mod.addSystemIncludePath(.{ .cwd_relative = "/opt/homebrew/opt/postgresql@17/include/server" });
    memory_safety_mod.addSystemIncludePath(.{ .cwd_relative = "/usr/local/opt/postgresql@17/include/server" });
    memory_safety_mod.addSystemIncludePath(.{ .cwd_relative = "../../.pg_install/include/server" });
    memory_safety_mod.addIncludePath(.{ .cwd_relative = "deps/pg_roaringbitmap" });
    const memory_safety_tests = b.addTest(.{
        .root_module = memory_safety_mod,
    });

    // Add benchmark tests
    const benchmark_mod = b.createModule(.{
        .root_source_file = b.path("src/benchmarks.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    const benchmark_tests = b.addTest(.{
        .root_module = benchmark_mod,
    });

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const run_memory_tests = b.addRunArtifact(memory_safety_tests);
    const run_benchmark_tests = b.addRunArtifact(benchmark_tests);

    const test_step = b.step("test", "Run all unit tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_memory_tests.step);

    // Separate step for memory safety tests
    const memory_test_step = b.step("test-memory", "Run memory safety tests");
    memory_test_step.dependOn(&run_memory_tests.step);

    // Separate step for benchmarks
    const benchmark_step = b.step("bench", "Run performance benchmarks");
    benchmark_step.dependOn(&run_benchmark_tests.step);

    // Find PostgreSQL extension directory (where extension files go)
    var extension_dir: []const u8 = "/usr/share/postgresql/16/extension"; // Default fallback
    const sharedir_result = std.process.Child.run(.{
        .allocator = b.allocator,
        .argv = &.{ "pg_config", "--sharedir" },
    }) catch null;

    if (sharedir_result) |res| {
        switch (res.term) {
            .Exited => |code| {
                if (code == 0) {
                    const sharedir = std.mem.trim(u8, res.stdout, " \n\r\t");
                    extension_dir = std.fmt.allocPrint(b.allocator, "{s}/extension", .{sharedir}) catch "/usr/share/postgresql/16/extension";
                }
            },
            else => {},
        }
    }

    // Install control file
    _ = b.addInstallFileWithDir(
        b.path("pg_facets.control"),
        .{ .custom = extension_dir },
        "pg_facets.control",
    );

    // Install SQL files - base version
    _ = b.addInstallFileWithDir(
        b.path("sql/pg_facets--0.4.3.sql"),
        .{ .custom = extension_dir },
        "pg_facets--0.4.3.sql",
    );
    
    // Install migration files (for upgrades from older versions)
    const migration_files = [_][]const u8{
        "sql/pg_facets--0.3.6--0.3.8.sql",
        "sql/pg_facets--0.3.8--0.3.9.sql",
        "sql/pg_facets--0.3.9--0.3.10.sql",
        "sql/pg_facets--0.3.10--0.4.0.sql",
        "sql/pg_facets--0.4.0--0.4.1.sql",
        "sql/pg_facets--0.4.1--0.4.2.sql",
        "sql/pg_facets--0.4.2--0.4.3.sql",
    };
    
    for (migration_files) |migration_file| {
        _ = b.addInstallFileWithDir(
            b.path(migration_file),
            .{ .custom = extension_dir },
            std.fmt.allocPrint(b.allocator, "{s}", .{std.fs.path.basename(migration_file)}) catch migration_file,
        );
    }
}

