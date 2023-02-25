#include <stdarg.h>

#include <automerge-c/utils/result.h>

AMresult* AMresultFrom(int count, ...) {
    AMresult* result = NULL;
    bool is_ok = true;
    va_list args;
    va_start(args, count);
    for (int i = 0; i != count; ++i) {
        AMresult* src = va_arg(args, AMresult*);
        AMresult* dest = result;
        is_ok = (AMresultStatus(src) == AM_STATUS_OK);
        if (is_ok) {
            if (dest) {
                result = AMresultCat(dest, src);
                is_ok = (AMresultStatus(result) == AM_STATUS_OK);
                AMresultFree(dest);
                AMresultFree(src);
            } else {
                result = src;
            }
        } else {
            AMresultFree(src);
        }
    }
    va_end(args);
    if (!is_ok) {
        AMresultFree(result);
        result = NULL;
    }
    return result;
}
