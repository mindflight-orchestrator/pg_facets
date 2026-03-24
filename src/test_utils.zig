const std = @import("std");
const testing = std.testing;

// Test simple pour vérifier que la logique de base fonctionne
test "basic allocator test" {
    var list = std.ArrayList(i32).empty;
    defer list.deinit(testing.allocator);

    try list.append(testing.allocator, 1);
    try list.append(testing.allocator, 2);
    try list.append(testing.allocator, 3);

    try testing.expect(list.items.len == 3);
    try testing.expect(list.items[0] == 1);
    try testing.expect(list.items[2] == 3);
}

test "string hash map test" {
    var map = std.StringHashMap(i32).init(testing.allocator);
    defer map.deinit();

    try map.put("test", 42);
    try map.put("hello", 100);

    try testing.expect(map.get("test").? == 42);
    try testing.expect(map.get("hello").? == 100);
    try testing.expect(map.get("missing") == null);
}

test "memory alignment calculation" {
    const base_size: usize = 30;
    const alignment: usize = 8;
    const aligned = std.mem.alignForward(usize, base_size, alignment);
    try testing.expect(aligned == 32);
}

