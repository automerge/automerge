#include <float.h>
#include <limits.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* third-party */
#include <cmocka.h>

/* local */
#include "group_state.h"
#include "macro_utils.h"

#define test_AMmapPut(suffix) test_AMmapPut ## suffix

#define static_void_test_AMmapPut(suffix, member, scalar_value)               \
static void test_AMmapPut ## suffix(void **state) {                           \
    GroupState* group_state = *state;                                         \
    AMresult* res = AMmapPut ## suffix(                                       \
        group_state->doc,                                                     \
        AM_ROOT,                                                              \
        #suffix,                                                              \
        scalar_value                                                          \
    );                                                                        \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 0);                                   \
    AMvalue value = AMresultValue(res, 0);                                    \
    assert_int_equal(value.tag, AM_VALUE_VOID);                               \
    AMfree(res);                                                        \
    res = AMmapGet(group_state->doc, AM_ROOT, #suffix);                       \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 1);                                   \
    value = AMresultValue(res, 0);                                            \
    assert_int_equal(value.tag, AMvalue_discriminant(#suffix));               \
    assert_true(value.member == scalar_value);                                \
    AMfree(res);                                                        \
}

#define test_AMmapPutObject(label) test_AMmapPutObject_ ## label

#define static_void_test_AMmapPutObject(label)                                \
static void test_AMmapPutObject_ ## label(void **state) {                     \
    GroupState* group_state = *state;                                         \
    AMresult* res = AMmapPutObject(                                           \
        group_state->doc,                                                     \
        AM_ROOT,                                                              \
        #label,                                                               \
        AMobjType_tag(#label)                                                 \
    );                                                                        \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 1);                                   \
    AMvalue value = AMresultValue(res, 0);                                    \
    assert_int_equal(value.tag, AM_VALUE_OBJ_ID);                             \
    assert_non_null(value.obj_id);                                            \
    assert_int_equal(AMobjSize(group_state->doc, value.obj_id), 0);           \
    AMfree(res);                                                        \
}

static_void_test_AMmapPut(Bool, boolean, true)

static void test_AMmapPutBytes(void **state) {
    static char const* const KEY = "Bytes";
    static uint8_t const BYTES_VALUE[] = {INT8_MIN, INT8_MAX / 2, INT8_MAX};
    static size_t const BYTES_SIZE = sizeof(BYTES_VALUE) / sizeof(uint8_t);

    GroupState* group_state = *state;
    AMresult* res = AMmapPutBytes(
        group_state->doc,
        AM_ROOT,
        KEY,
        BYTES_VALUE,
        BYTES_SIZE
    );
    if (AMresultStatus(res) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
    assert_int_equal(AMresultSize(res), 0);
    AMvalue value = AMresultValue(res, 0);
    assert_int_equal(value.tag, AM_VALUE_VOID);
    AMfree(res);
    res = AMmapGet(group_state->doc, AM_ROOT, KEY);
    if (AMresultStatus(res) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
    assert_int_equal(AMresultSize(res), 1);
    value = AMresultValue(res, 0);
    assert_int_equal(value.tag, AM_VALUE_BYTES);
    assert_int_equal(value.bytes.count, BYTES_SIZE);
    assert_memory_equal(value.bytes.src, BYTES_VALUE, BYTES_SIZE);
    AMfree(res);
}

static_void_test_AMmapPut(Counter, counter, INT64_MAX)

static_void_test_AMmapPut(F64, f64, DBL_MAX)

static_void_test_AMmapPut(Int, int_, INT64_MAX)

static void test_AMmapPutNull(void **state) {
    static char const* const KEY = "Null";

    GroupState* group_state = *state;
    AMresult* res = AMmapPutNull(group_state->doc, AM_ROOT, KEY);
    if (AMresultStatus(res) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
    assert_int_equal(AMresultSize(res), 0);
    AMvalue value = AMresultValue(res, 0);
    assert_int_equal(value.tag, AM_VALUE_VOID);
    AMfree(res);
    res = AMmapGet(group_state->doc, AM_ROOT, KEY);
    if (AMresultStatus(res) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
    assert_int_equal(AMresultSize(res), 1);
    value = AMresultValue(res, 0);
    assert_int_equal(value.tag, AM_VALUE_NULL);
    AMfree(res);
}

static_void_test_AMmapPutObject(List)

static_void_test_AMmapPutObject(Map)

static_void_test_AMmapPutObject(Text)

static void test_AMmapPutStr(void **state) {
    static char const* const KEY = "Str";
    static char const* const STR_VALUE = "Hello, world!";
    size_t const STR_LEN = strlen(STR_VALUE);

    GroupState* group_state = *state;
    AMresult* res = AMmapPutStr(
        group_state->doc,
        AM_ROOT,
        KEY,
        STR_VALUE
    );
    if (AMresultStatus(res) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
    assert_int_equal(AMresultSize(res), 0);
    AMvalue value = AMresultValue(res, 0);
    assert_int_equal(value.tag, AM_VALUE_VOID);
    AMfree(res);
    res = AMmapGet(group_state->doc, AM_ROOT, KEY);
    if (AMresultStatus(res) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
    assert_int_equal(AMresultSize(res), 1);
    value = AMresultValue(res, 0);
    assert_int_equal(value.tag, AM_VALUE_STR);
    assert_int_equal(strlen(value.str), STR_LEN);
    assert_memory_equal(value.str, STR_VALUE, STR_LEN + 1);
    AMfree(res);
}

static_void_test_AMmapPut(Timestamp, timestamp, INT64_MAX)

static_void_test_AMmapPut(Uint, uint, UINT64_MAX)

int run_AMmapPut_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMmapPut(Bool)),
        cmocka_unit_test(test_AMmapPutBytes),
        cmocka_unit_test(test_AMmapPut(Counter)),
        cmocka_unit_test(test_AMmapPut(F64)),
        cmocka_unit_test(test_AMmapPut(Int)),
        cmocka_unit_test(test_AMmapPutNull),
        cmocka_unit_test(test_AMmapPutObject(List)),
        cmocka_unit_test(test_AMmapPutObject(Map)),
        cmocka_unit_test(test_AMmapPutObject(Text)),
        cmocka_unit_test(test_AMmapPutStr),
        cmocka_unit_test(test_AMmapPut(Timestamp)),
        cmocka_unit_test(test_AMmapPut(Uint)),
    };

    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
