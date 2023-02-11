#include <string.h>

/* local */
#include "macro_utils.h"

AMobjType suffix_to_obj_type(char const* obj_type_label) {
    if (!strcmp(obj_type_label, "List"))
        return AM_OBJ_TYPE_LIST;
    else if (!strcmp(obj_type_label, "Map"))
        return AM_OBJ_TYPE_MAP;
    else if (!strcmp(obj_type_label, "Text"))
        return AM_OBJ_TYPE_TEXT;
    else
        return AM_OBJ_TYPE_DEFAULT;
}

AMvalType suffix_to_val_type(char const* suffix) {
    if (!strcmp(suffix, "Bool"))
        return AM_VAL_TYPE_BOOL;
    else if (!strcmp(suffix, "Bytes"))
        return AM_VAL_TYPE_BYTES;
    else if (!strcmp(suffix, "Counter"))
        return AM_VAL_TYPE_COUNTER;
    else if (!strcmp(suffix, "F64"))
        return AM_VAL_TYPE_F64;
    else if (!strcmp(suffix, "Int"))
        return AM_VAL_TYPE_INT;
    else if (!strcmp(suffix, "Null"))
        return AM_VAL_TYPE_NULL;
    else if (!strcmp(suffix, "Str"))
        return AM_VAL_TYPE_STR;
    else if (!strcmp(suffix, "Timestamp"))
        return AM_VAL_TYPE_TIMESTAMP;
    else if (!strcmp(suffix, "Uint"))
        return AM_VAL_TYPE_UINT;
    else
        return AM_VAL_TYPE_DEFAULT;
}
