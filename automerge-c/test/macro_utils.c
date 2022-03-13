#include <string.h>

/* local */
#include "macro_utils.h"

AMvalueVariant AMvalue_discriminant(char const* suffix) {
    if (!strcmp(suffix, "Bytes"))          return AM_VALUE_BYTES;
    else if (!strcmp(suffix, "Counter"))   return AM_VALUE_COUNTER;
    else if (!strcmp(suffix, "F64"))       return AM_VALUE_F64;
    else if (!strcmp(suffix, "Int"))       return AM_VALUE_INT;
    else if (!strcmp(suffix, "Null"))      return AM_VALUE_NULL;
    else if (!strcmp(suffix, "Str"))       return AM_VALUE_STR;
    else if (!strcmp(suffix, "Timestamp")) return AM_VALUE_TIMESTAMP;
    else if (!strcmp(suffix, "Uint"))      return AM_VALUE_UINT;
    else return AM_VALUE_NOTHING;
}

AMobjType AMobjType_tag(char const* obj_type_label) {
    if (!strcmp(obj_type_label, "List")) return AM_OBJ_TYPE_LIST;
    else if (!strcmp(obj_type_label, "Map")) return AM_OBJ_TYPE_MAP;
    else if (!strcmp(obj_type_label, "Text")) return AM_OBJ_TYPE_TEXT;
    else return 0;
}
