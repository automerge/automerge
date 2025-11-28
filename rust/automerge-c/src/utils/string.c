#include <stdlib.h>
#include <string.h>

#include <automerge-c/utils/string.h>

char* AMstrdup(AMbyteSpan const str, char const* nul) {
    if (!str.src) {
        return NULL;
    } else if (!str.count) {
        return strdup("");
    }
    nul = (nul) ? nul : "\\0";
    size_t const nul_len = strlen(nul);
    char* dup = NULL;
    size_t dup_len = 0;
    char const* begin = (char const*) str.src;
    char const* end = begin;
    for (size_t i = 0; i != str.count; ++i, ++end) {
        if (!*end) {
            size_t const len = end - begin;
            size_t const alloc_len = dup_len + len + nul_len;
            if (dup) {
                dup = realloc(dup, alloc_len + 1);
            } else {
                dup = malloc(alloc_len + 1);
            }
            memcpy(dup + dup_len, begin, len);
            memcpy(dup + dup_len + len, nul, nul_len);
            dup[alloc_len] = '\0';
            begin = end + 1;
            dup_len = alloc_len;
        }
    }
    if (begin != end) {
        size_t const len = end - begin;
        size_t const alloc_len = dup_len + len;
        if (dup) {
            dup = realloc(dup, alloc_len + 1);
        } else {
            dup = malloc(alloc_len + 1);
        }
        memcpy(dup + dup_len, begin, len);
        dup[alloc_len] = '\0';
    }
    return dup;
}
