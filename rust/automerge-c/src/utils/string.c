#include <stdlib.h>
#include <string.h>

#include <automerge-c/utils/string.h>

int AMstrcmp(AMbyteSpan const lhs, AMbyteSpan const rhs) {
    if (lhs.count < rhs.count) {
        return -1;
    } else if (lhs.count == rhs.count) {
        return memcmp(lhs.src, rhs.src, lhs.count);
    } else {
        return 1;
    }
}

char* AMstrdup(AMbyteSpan const str, char const* nul) {
    if (!(str.src && str.count)) {
        return NULL;
    }
    nul = (nul) ? nul : "\\0";
    size_t const nul_len = strlen(nul);
    char* dup = NULL;
    size_t dup_len = 0;
    char const* begin = str.src;
    char const* end = begin;
    for (size_t i = 0; i != str.count; ++i, ++end) {
        if (!*end) {
            size_t const len = end - begin;
            dup_len += len + nul_len;
            if (dup) {
                dup = realloc(dup, dup_len + 1);
            } else {
                dup = memcpy(malloc(dup_len + 1), begin, len);
            }
            memcpy(dup + len, nul, nul_len);
            dup[dup_len] = '\0';
            begin = end + 1;
        }
    }
    if (begin != end) {
        size_t const len = end - begin;
        dup_len += len;
        if (dup) {
            dup = realloc(dup, dup_len + 1);
        } else {
            dup = memcpy(malloc(dup_len + 1), begin, len);
        }
        dup[dup_len] = '\0';
    }
    return dup;
}
