#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "access/htup_details.h"
#include "utils/typcache.h"
#include "access/tupmacs.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"  // For text_to_cstring

// Helpers for FunctionCallInfo argument access.
// Zig does not import the flexible array member `args[]` reliably, so we expose
// stable helpers compiled against the server headers.
Datum fcinfo_get_arg_value_helper(FunctionCallInfo fcinfo, int n) {
    return fcinfo->args[n].value;
}

bool fcinfo_get_arg_isnull_helper(FunctionCallInfo fcinfo, int n) {
    return fcinfo->args[n].isnull;
}

void fcinfo_set_isnull_helper(FunctionCallInfo fcinfo, bool isnull) {
    fcinfo->isnull = isnull;
}

// Helper function to extract facet_name and facet_value from a facet_filter composite type
// Returns 1 if successful, 0 if error
int extract_facet_filter_fields(Datum composite_datum, Oid composite_type,
                                char **facet_name_out, int *facet_name_len,
                                char **facet_value_out, int *facet_value_len,
                                bool *facet_value_isnull) {
    HeapTupleHeader header = DatumGetHeapTupleHeader(composite_datum);
    TupleDesc tupdesc;
    bool isnull;
    Datum name_datum, value_datum;
    text *name_text, *value_text;
    
    // Get tuple descriptor for the composite type
    tupdesc = lookup_rowtype_tupdesc_copy(composite_type, -1);
    if (tupdesc == NULL) {
        return 0;
    }
    
    // Build HeapTupleData for fastgetattr
    HeapTupleData tuple;
    tuple.t_len = HeapTupleHeaderGetDatumLength(header);
    tuple.t_data = header;
    
    // Extract facet_name (attribute 1)
    name_datum = fastgetattr(&tuple, 1, tupdesc, &isnull);
    if (isnull) {
        ReleaseTupleDesc(tupdesc);
        return 0;
    }
    name_text = DatumGetTextP(name_datum);
    *facet_name_out = VARDATA(name_text);
    *facet_name_len = VARSIZE(name_text) - VARHDRSZ;
    
    // Extract facet_value (attribute 2)
    value_datum = fastgetattr(&tuple, 2, tupdesc, &isnull);
    *facet_value_isnull = isnull;
    if (!isnull) {
        value_text = DatumGetTextP(value_datum);
        *facet_value_out = VARDATA(value_text);
        *facet_value_len = VARSIZE(value_text) - VARHDRSZ;
    } else {
        *facet_value_out = NULL;
        *facet_value_len = 0;
    }
    
    ReleaseTupleDesc(tupdesc);
    return 1;
}

// Helper function to detoast a datum (wrapper around pg_detoast_datum macro)
struct varlena *detoast_datum_helper(Datum d) {
    return pg_detoast_datum((struct varlena *)DatumGetPointer(d));
}

// Helper functions for VARSIZE and VARDATA macros
int varsize_helper(struct varlena *ptr) {
    return VARSIZE(ptr);
}

char *vardata_helper(struct varlena *ptr) {
    return VARDATA(ptr);
}

// Helper for VARHDRSZ constant
int varhdrsz_helper(void) {
    return VARHDRSZ;
}

// Helper for SET_VARSIZE macro
void set_varsize_helper(struct varlena *ptr, int size) {
    SET_VARSIZE(ptr, size);
}

// Helper for IsA macro (checks if node is of a specific type)
// We avoid using the IsA macro directly to prevent macro expansion issues
bool isa_helper(Node *node, NodeTag tag) {
    if (node == NULL) return false;
    return node->type == tag;
}

// Helper to get T_ReturnSetInfo constant
NodeTag t_returnsetinfo_helper(void) {
    return T_ReturnSetInfo;
}

// Helper for elog macro (variadic, so we create a simple wrapper)
// elog(level, fmt, ...) - we'll use a simple version with just level and message
void elog_helper(int level, const char *msg) {
    elog(level, "%s", msg);
}

// Helper to get work_mem value (global variable)
int work_mem_helper(void) {
    extern int work_mem;
    return work_mem;
}

// Helper for DatumGetTextP macro
struct varlena *datum_get_textp_helper(Datum d) {
    return DatumGetTextP(d);
}

// Helper for VARSIZE_ANY_EXHDR macro
int varsize_any_exhdr_helper(struct varlena *ptr) {
    return VARSIZE_ANY_EXHDR(ptr);
}

// Helper for VARDATA_ANY macro
char *vardata_any_helper(struct varlena *ptr) {
    return VARDATA_ANY(ptr);
}

// Helper for text_to_cstring - safest way to extract text from datum
// Returns a palloc'd null-terminated string (caller must pfree)
char *text_to_cstring_helper(Datum d) {
    return text_to_cstring((text *)DatumGetTextP(d));
}

// Helper to get length of a C string safely
size_t strlen_helper(const char *str) {
    if (str == NULL) return 0;
    return strlen(str);
}

// CRoaring defines croaring_hardware_support only for x86_64 builds.
// pg_facets references this symbol from Zig; provide a portable fallback on
// non-x86_64 targets so the extension loads correctly on ARM.
#if !defined(__x86_64__) && !defined(_M_AMD64)
__attribute__((weak)) int croaring_hardware_support(void) {
    return 0;
}
#endif

