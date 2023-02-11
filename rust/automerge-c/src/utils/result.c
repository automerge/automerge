#include <stdarg.h>

#include <automerge-c/utils/result.h>

AMresult* AMresultFrom(int count, ...) {
	AMresult* result = NULL;
	bool is_error = false;
    va_list args;
    va_start(args, count);
    for (int i = 0; i != count; ++i) {
        AMresult* src = va_arg(args, AMresult*);
		AMresult* dest = result;
		if (!is_error && (AMresultStatus(src) == AM_STATUS_OK)) {
			if (dest) {
				result = AMresultCat(dest, src);
				AMfree(dest);
				AMfree(src);
			} else {
				result = src;
			}
		} else {
			is_error = true;
			AMfree(src);
		}
    }
    va_end(args);
	if (is_error) {
		AMfree(result);
		result = NULL;
	}
	return result;
}
