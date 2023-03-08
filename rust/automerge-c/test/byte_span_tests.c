#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* third-party */
#include <cmocka.h>

/* local */
#include <automerge-c/automerge.h>
#include <automerge-c/utils/string.h>

static void test_AMbytes(void** state) {
    static char const DATA[] = {0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf};

    AMbyteSpan bytes = AMbytes(DATA, sizeof(DATA));
    assert_int_equal(bytes.count, sizeof(DATA));
    assert_memory_equal(bytes.src, DATA, bytes.count);
    assert_ptr_equal(bytes.src, DATA);
    /* Empty view */
    bytes = AMbytes(DATA, 0);
    assert_int_equal(bytes.count, 0);
    assert_ptr_equal(bytes.src, DATA);
    /* Invalid array */
    bytes = AMbytes(NULL, SIZE_MAX);
    assert_int_not_equal(bytes.count, SIZE_MAX);
    assert_int_equal(bytes.count, 0);
    assert_ptr_equal(bytes.src, NULL);
}

static void test_AMstr(void** state) {
    AMbyteSpan str = AMstr("abcdefghijkl");
    assert_int_equal(str.count, strlen("abcdefghijkl"));
    assert_memory_equal(str.src, "abcdefghijkl", str.count);
    /* Empty string */
    static char const* const EMPTY = "";

    str = AMstr(EMPTY);
    assert_int_equal(str.count, 0);
    assert_ptr_equal(str.src, EMPTY);
    /* Invalid string */
    str = AMstr(NULL);
    assert_int_equal(str.count, 0);
    assert_ptr_equal(str.src, NULL);
}

static void test_AMstrCmp(void** state) {
    /* Length ordering */
    assert_int_equal(AMstrCmp(AMstr("abcdef"), AMstr("abcdefghijkl")), -1);
    assert_int_equal(AMstrCmp(AMstr("abcdefghijkl"), AMstr("abcdefghijkl")), 0);
    assert_int_equal(AMstrCmp(AMstr("abcdefghijkl"), AMstr("abcdef")), 1);
    /* Lexicographical ordering */
    assert_int_equal(AMstrCmp(AMstr("abcdef"), AMstr("ghijkl")), -1);
    assert_int_equal(AMstrCmp(AMstr("ghijkl"), AMstr("abcdef")), 1);
    /* Case ordering */
    assert_int_equal(AMstrCmp(AMstr("ABCDEFGHIJKL"), AMstr("abcdefghijkl")), -1);
    assert_int_equal(AMstrCmp(AMstr("ABCDEFGHIJKL"), AMstr("ABCDEFGHIJKL")), 0);
    assert_int_equal(AMstrCmp(AMstr("abcdefghijkl"), AMstr("ABCDEFGHIJKL")), 1);
    assert_int_equal(AMstrCmp(AMstr("ABCDEFGHIJKL"), AMstr("abcdef")), -1);
    assert_int_equal(AMstrCmp(AMstr("abcdef"), AMstr("ABCDEFGHIJKL")), 1);
    assert_int_equal(AMstrCmp(AMstr("GHIJKL"), AMstr("abcdef")), -1);
    assert_int_equal(AMstrCmp(AMstr("abcdef"), AMstr("GHIJKL")), 1);
    /* NUL character inclusion */
    static char const SRC[] = {'a', 'b', 'c', 'd', 'e', 'f', '\0', 'g', 'h', 'i', 'j', 'k', 'l'};
    static AMbyteSpan const NUL_STR = {.src = SRC, .count = 13};

    assert_int_equal(AMstrCmp(AMstr("abcdef"), NUL_STR), -1);
    assert_int_equal(AMstrCmp(NUL_STR, NUL_STR), 0);
    assert_int_equal(AMstrCmp(NUL_STR, AMstr("abcdef")), 1);
    /* Empty string */
    assert_int_equal(AMstrCmp(AMstr(""), AMstr("abcdefghijkl")), -1);
    assert_int_equal(AMstrCmp(AMstr(""), AMstr("")), 0);
    assert_int_equal(AMstrCmp(AMstr("abcdefghijkl"), AMstr("")), 1);
    /* Invalid string */
    assert_int_equal(AMstrCmp(AMstr(NULL), AMstr("abcdefghijkl")), -1);
    assert_int_equal(AMstrCmp(AMstr(NULL), AMstr(NULL)), 0);
    assert_int_equal(AMstrCmp(AMstr("abcdefghijkl"), AMstr(NULL)), 1);
}

static void test_AMstrdup(void** state) {
    static char const SRC[] = {'a', 'b', 'c', '\0', 'd', 'e', 'f', '\0', 'g', 'h', 'i', '\0', 'j', 'k', 'l'};
    static AMbyteSpan const NUL_STR = {.src = SRC, .count = 15};

    /* Default substitution ("\\0") for NUL */
    char* dup = AMstrdup(NUL_STR, NULL);
    assert_int_equal(strlen(dup), 18);
    assert_string_equal(dup, "abc\\0def\\0ghi\\0jkl");
    free(dup);
    /* Arbitrary substitution for NUL */
    dup = AMstrdup(NUL_STR, ":-O");
    assert_int_equal(strlen(dup), 21);
    assert_string_equal(dup, "abc:-Odef:-Oghi:-Ojkl");
    free(dup);
    /* Empty substitution for NUL */
    dup = AMstrdup(NUL_STR, "");
    assert_int_equal(strlen(dup), 12);
    assert_string_equal(dup, "abcdefghijkl");
    free(dup);
    /* Empty string */
    dup = AMstrdup(AMstr(""), NULL);
    assert_int_equal(strlen(dup), 0);
    assert_string_equal(dup, "");
    free(dup);
    /* Invalid string */
    assert_null(AMstrdup(AMstr(NULL), NULL));
}

int run_byte_span_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMbytes),
        cmocka_unit_test(test_AMstr),
        cmocka_unit_test(test_AMstrCmp),
        cmocka_unit_test(test_AMstrdup),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
