const std = @import("std");
const utils = @import("../utils.zig");
const c = utils.c;
const PgAllocator = utils.PgAllocator;
const tokenizer_native = @import("tokenizer_native.zig");

/// Minimal test function that only tests tokenization
/// Returns: (lexeme text, frequency int)[]
pub fn test_tokenize_only(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    // Parse arguments: text, config_name
    if (utils.is_arg_null(fcinfo, 0)) {
        utils.elog(c.ERROR, "text cannot be null");
        return c.PointerGetDatum(null);
    }
    if (utils.is_arg_null(fcinfo, 1)) {
        utils.elog(c.ERROR, "config_name cannot be null");
        return c.PointerGetDatum(null);
    }
    
    const text_datum = utils.get_arg_datum(fcinfo, 0);
    const config_datum = utils.get_arg_datum(fcinfo, 1);
    
    const text_cstr = utils.textToCstring(text_datum);
    if (text_cstr == null) {
        utils.elog(c.ERROR, "Failed to extract text");
        return c.PointerGetDatum(null);
    }
    // Don't defer pfree - let PostgreSQL handle it via memory context
    
    const config_cstr = utils.textToCstring(config_datum);
    if (config_cstr == null) {
        utils.elog(c.ERROR, "Failed to extract config name");
        return c.PointerGetDatum(null);
    }
    // Don't defer pfree
    
    // Use bounded strlen with max check to prevent reading corrupted memory
    var text_len: usize = 0;
    const max_text_len: usize = 10485760; // Max 10MB for test text
    while (text_len < max_text_len and text_cstr[text_len] != 0) {
        text_len += 1;
    }
    if (text_len >= max_text_len) {
        utils.elogFmt(c.ERROR,
            "test_tokenize_only: Text string too long or not null-terminated (length >= {d} bytes, max {d} bytes). Please reduce text size.",
            .{ text_len, max_text_len });
        return c.PointerGetDatum(null);
    }
    
    var config_len: usize = 0;
    const max_config_len: usize = 64; // Config names should be < 64 bytes
    while (config_len < max_config_len and config_cstr[config_len] != 0) {
        config_len += 1;
    }
    if (config_len >= max_config_len) {
        utils.elogFmt(c.ERROR, 
            "test_tokenize_only: Config string too long or not null-terminated (length >= {d} bytes, max {d} bytes). Config names should be short (e.g., 'english', 'simple').", 
            .{ config_len, max_config_len });
        return c.PointerGetDatum(null);
    }
    
    const text_slice = text_cstr[0..text_len];
    const config_slice = config_cstr[0..config_len];
    
    // Set up ReturnSetInfo FIRST before any allocations
    const rsi_ptr = @as(?*c.ReturnSetInfo, @alignCast(@ptrCast(fcinfo.*.resultinfo)));
    if (rsi_ptr == null) {
        utils.elog(c.ERROR, "set-valued function called in context that cannot accept a set");
        return 0;
    }
    const rsi = rsi_ptr.?;
    
    // Use Materialize mode
    if ((rsi.allowedModes & c.SFRM_Materialize) == 0) {
        utils.elog(c.ERROR, "SRF materialize mode not allowed in this context");
        return 0;
    }
    rsi.returnMode = c.SFRM_Materialize;
    
    // Switch to per-query memory context for all allocations
    const oldcontext = c.MemoryContextSwitchTo(rsi.econtext.?.*.ecxt_per_query_memory);
    defer _ = c.MemoryContextSwitchTo(oldcontext);
    
    rsi.setResult = c.tuplestore_begin_heap(true, false, utils.workMem());
    
    // Create tuple descriptor: (lexeme text, freq int)
    const tupdesc = c.CreateTemplateTupleDesc(2);
    _ = c.TupleDescInitEntry(tupdesc, 1, "lexeme", c.TEXTOID, -1, 0);
    _ = c.TupleDescInitEntry(tupdesc, 2, "freq", c.INT4OID, -1, 0);
    rsi.setDesc = tupdesc;
    
    // Use the per-query allocator for tokenization
    const allocator = PgAllocator.allocator();
    
    // Tokenize - DON'T free tokens manually, let memory context handle it
    const tokens = tokenizer_native.tokenizeNative(text_slice, config_slice, allocator) catch {
        utils.elog(c.ERROR, "Tokenization failed");
        return c.PointerGetDatum(null);
    };
    // Don't defer free - memory context will clean up
    
    // Create and store tuples
    for (tokens.items) |token| {
        const lexeme_text = c.cstring_to_text_with_len(@ptrCast(token.lexeme.ptr), @intCast(token.lexeme.len));
        const lexeme_datum = c.PointerGetDatum(lexeme_text);
        
        var values = [_]c.Datum{
            lexeme_datum,
            c.Int32GetDatum(token.freq),
        };
        var nulls = [_]bool{ false, false };
        const tuple = c.heap_form_tuple(tupdesc, &values, &nulls);
        c.tuplestore_puttuple(rsi.setResult, tuple);
    }
    
    return 0; // Return 0 for SRF
}

