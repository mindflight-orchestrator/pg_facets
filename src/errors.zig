/// Comprehensive error types for pg_facets extension
/// This centralizes all error handling and provides better error messages

/// SPI (Server Programming Interface) errors
pub const SPIError = error{
    SPIConnectFailed,
    SPIDisconnectFailed,
    SPIExecuteFailed,
    SPIResultInvalid,
    SPIProcessError,
    SPINoResults,
    SPITupleError,
    SPIGetValueError,
};

/// Memory management errors
pub const MemoryError = error{
    OutOfMemory,
    BufferTooSmall,
    AllocationFailed,
    DeallocationFailed,
    MemoryContextError,
};

/// Database operation errors
pub const DatabaseError = error{
    QueryFailed,
    UpdateFailed,
    InsertFailed,
    DeleteFailed,
    UpsertFailed,
    TransactionFailed,
    SavepointFailed,
    RollbackFailed,
    CommitFailed,
};

/// Text processing errors
pub const TextError = error{
    TextExtractionFailed,
    TextConversionFailed,
    InvalidUtf8,
    TextTooLong,
    CstringConversionFailed,
    TsvectorParseError,
};

/// Bitmap/Roaring errors
pub const BitmapError = error{
    BitmapCreationFailed,
    BitmapSerializationFailed,
    BitmapDeserializationFailed,
    BitmapOperationFailed,
    BitmapCopyFailed,
    BitmapFreeFailed,
};

/// BM25 search errors
pub const BM25Error = error{
    TokenizationFailed,
    IndexingFailed,
    SearchFailed,
    ScoringFailed,
    StatisticsCalculationFailed,
    DocumentNotFound,
    TermNotFound,
};

/// Tokenizer errors
pub const TokenizerError = error{
    ConfigNotFound,
    ConfigInvalid,
    TokenizeFailed,
    StemmingFailed,
    LanguageUnsupported,
    BufferOverflow,
};

/// File system errors
pub const FileSystemError = error{
    FileOpenFailed,
    FileReadFailed,
    FileWriteFailed,
    FileCloseFailed,
    TempFileError,
    PathInvalid,
};

/// General utility errors
pub const UtilityError = error{
    InvalidArgument,
    NullPointer,
    TypeMismatch,
    ConversionFailed,
    NotImplemented,
    InvalidState,
    OperationTimeout,
};

/// Union of all error types for convenience
pub const ExtensionError = SPIError || MemoryError || DatabaseError || TextError ||
                           BitmapError || BM25Error || TokenizerError || FileSystemError ||
                           UtilityError;

/// Helper function to convert error to human-readable string
pub fn errorToString(err: anyerror) []const u8 {
    return switch (err) {
        // SPI errors
        SPIError.SPIConnectFailed => "Failed to connect to SPI",
        SPIError.SPIDisconnectFailed => "Failed to disconnect from SPI",
        SPIError.SPIExecuteFailed => "SPI query execution failed",
        SPIError.SPIResultInvalid => "Invalid SPI result",
        SPIError.SPIProcessError => "SPI processing error",
        SPIError.SPINoResults => "No results from SPI query",
        SPIError.SPITupleError => "SPI tuple access error",
        SPIError.SPIGetValueError => "Failed to get value from SPI tuple",

        // Memory errors
        MemoryError.OutOfMemory => "Out of memory",
        MemoryError.BufferTooSmall => "Buffer too small for operation",
        MemoryError.AllocationFailed => "Memory allocation failed",
        MemoryError.DeallocationFailed => "Memory deallocation failed",
        MemoryError.MemoryContextError => "PostgreSQL memory context error",

        // Database errors
        DatabaseError.QueryFailed => "Database query failed",
        DatabaseError.UpdateFailed => "Database update failed",
        DatabaseError.InsertFailed => "Database insert failed",
        DatabaseError.DeleteFailed => "Database delete failed",
        DatabaseError.UpsertFailed => "Database upsert failed",
        DatabaseError.TransactionFailed => "Database transaction failed",
        DatabaseError.SavepointFailed => "Database savepoint creation failed",
        DatabaseError.RollbackFailed => "Database rollback failed",
        DatabaseError.CommitFailed => "Database commit failed",

        // Text errors
        TextError.TextExtractionFailed => "Failed to extract text from datum",
        TextError.TextConversionFailed => "Text conversion failed",
        TextError.InvalidUtf8 => "Invalid UTF-8 encoding",
        TextError.TextTooLong => "Text too long for processing",
        TextError.CstringConversionFailed => "C string conversion failed",
        TextError.TsvectorParseError => "TSVector parsing failed",

        // Bitmap errors
        BitmapError.BitmapCreationFailed => "Failed to create bitmap",
        BitmapError.BitmapSerializationFailed => "Bitmap serialization failed",
        BitmapError.BitmapDeserializationFailed => "Bitmap deserialization failed",
        BitmapError.BitmapOperationFailed => "Bitmap operation failed",
        BitmapError.BitmapCopyFailed => "Bitmap copy failed",
        BitmapError.BitmapFreeFailed => "Bitmap free failed",

        // BM25 errors
        BM25Error.TokenizationFailed => "Document tokenization failed",
        BM25Error.IndexingFailed => "Document indexing failed",
        BM25Error.SearchFailed => "Search operation failed",
        BM25Error.ScoringFailed => "BM25 scoring failed",
        BM25Error.StatisticsCalculationFailed => "Statistics calculation failed",
        BM25Error.DocumentNotFound => "Document not found",
        BM25Error.TermNotFound => "Search term not found",

        // Tokenizer errors
        TokenizerError.ConfigNotFound => "Text search configuration not found",
        TokenizerError.ConfigInvalid => "Invalid text search configuration",
        TokenizerError.TokenizeFailed => "Tokenization failed",
        TokenizerError.StemmingFailed => "Stemming operation failed",
        TokenizerError.LanguageUnsupported => "Unsupported language",
        TokenizerError.BufferOverflow => "Tokenizer buffer overflow",

        // File system errors
        FileSystemError.FileOpenFailed => "Failed to open file",
        FileSystemError.FileReadFailed => "Failed to read file",
        FileSystemError.FileWriteFailed => "Failed to write file",
        FileSystemError.FileCloseFailed => "Failed to close file",
        FileSystemError.TempFileError => "Temporary file operation failed",
        FileSystemError.PathInvalid => "Invalid file path",

        // Utility errors
        UtilityError.InvalidArgument => "Invalid argument provided",
        UtilityError.NullPointer => "Null pointer encountered",
        UtilityError.TypeMismatch => "Type mismatch",
        UtilityError.ConversionFailed => "Type conversion failed",
        UtilityError.NotImplemented => "Feature not implemented",
        UtilityError.InvalidState => "Invalid internal state",
        UtilityError.OperationTimeout => "Operation timed out",

        else => "Unknown error",
    };
}

/// Log an error with context and return null datum
pub fn logErrorAndReturnNull(comptime func_name: []const u8, err: anyerror) void {
    const error_msg = errorToString(err);
    const utils = @import("utils.zig");
    utils.elogFmt(utils.c.ERROR, "{s}: {s}", .{ func_name, error_msg });
}
