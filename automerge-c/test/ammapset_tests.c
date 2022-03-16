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

#define test_AMmapSet(suffix) test_AMmapSet ## suffix

#define static_void_test_AMmapSet(suffix, member, scalar_value)               \
static void test_AMmapSet ## suffix(void **state) {                           \
    GroupState* group_state = *state;                                         \
    AMresult* res = AMmapSet ## suffix(                                       \
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
    assert_int_equal(value.tag, AM_VALUE_NOTHING);                            \
    AMclear(res);                                                             \
    res = AMmapGet(group_state->doc, AM_ROOT, #suffix);                       \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 1);                                   \
    value = AMresultValue(res, 0);                                            \
    assert_int_equal(value.tag, AMvalue_discriminant(#suffix));               \
    assert_true(value.member == scalar_value);                                \
    AMclear(res);                                                             \
}

#define test_AMmapSetObject(label) test_AMmapSetObject_ ## label

#define static_void_test_AMmapSetObject(label)                                \
static void test_AMmapSetObject_ ## label(void **state) {                     \
    GroupState* group_state = *state;                                         \
    AMresult* res = AMmapSetObject(                                           \
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
    assert_int_equal(value.tag, AM_VALUE_OBJ);                                \
    assert_int_equal(value.obj.tag, AM_OBJ_ID);                               \
    assert_int_equal(AMobjSize(group_state->doc, &value.obj), 0);             \
    AMclear(res);                                                             \
}

static void test_AMmapSetBytes(void **state) {
    static char const* const KEY = "Bytes";
    static uint8_t const BYTES_VALUE[] = {INT8_MIN, INT8_MAX / 2, INT8_MAX};
    static size_t const BYTES_SIZE = sizeof(BYTES_VALUE) / sizeof(uint8_t);

    GroupState* group_state = *state;
    AMresult* res = AMmapSetBytes(
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
    assert_int_equal(value.tag, AM_VALUE_NOTHING);
    AMclear(res);
    res = AMmapGet(group_state->doc, AM_ROOT, KEY);
    if (AMresultStatus(res) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
    assert_int_equal(AMresultSize(res), 1);
    value = AMresultValue(res, 0);
    assert_int_equal(value.tag, AM_VALUE_BYTES);
    assert_int_equal(value.bytes.count, BYTES_SIZE);
    assert_memory_equal(value.bytes.src, BYTES_VALUE, BYTES_SIZE);
    AMclear(res);
}

static_void_test_AMmapSet(Counter, counter, INT64_MAX)

static_void_test_AMmapSet(F64, f64, DBL_MAX)

static_void_test_AMmapSet(Int, int_, INT64_MAX)

static void test_AMmapSetNull(void **state) {
    static char const* const KEY = "Null";

    GroupState* group_state = *state;
    AMresult* res = AMmapSetNull(group_state->doc, AM_ROOT, KEY);
    if (AMresultStatus(res) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
    assert_int_equal(AMresultSize(res), 0);
    AMvalue value = AMresultValue(res, 0);
    assert_int_equal(value.tag, AM_VALUE_NOTHING);
    AMclear(res);
    res = AMmapGet(group_state->doc, AM_ROOT, KEY);
    if (AMresultStatus(res) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
    assert_int_equal(AMresultSize(res), 1);
    value = AMresultValue(res, 0);
    assert_int_equal(value.tag, AM_VALUE_NULL);
    AMclear(res);
}

static_void_test_AMmapSetObject(List)

static_void_test_AMmapSetObject(Map)

static_void_test_AMmapSetObject(Text)

static void test_AMmapSetStr(void **state) {
    static char const* const KEY = "Str";
    static char const* const STR_VALUE = "Hello, world!";
    size_t const STR_LEN = strlen(STR_VALUE);

    GroupState* group_state = *state;
    AMresult* res = AMmapSetStr(
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
    assert_int_equal(value.tag, AM_VALUE_NOTHING);
    AMclear(res);
    res = AMmapGet(group_state->doc, AM_ROOT, KEY);
    if (AMresultStatus(res) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
    assert_int_equal(AMresultSize(res), 1);
    value = AMresultValue(res, 0);
    assert_int_equal(value.tag, AM_VALUE_STR);
    assert_int_equal(strlen(value.str), STR_LEN);
    assert_memory_equal(value.str, STR_VALUE, STR_LEN + 1);
    AMclear(res);
}

static_void_test_AMmapSet(Timestamp, timestamp, INT64_MAX)

static_void_test_AMmapSet(Uint, uint, UINT64_MAX)

int run_AMmapSet_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMmapSetBytes),
        cmocka_unit_test(test_AMmapSet(Counter)),
        cmocka_unit_test(test_AMmapSet(F64)),
        cmocka_unit_test(test_AMmapSet(Int)),
        cmocka_unit_test(test_AMmapSetNull),
        cmocka_unit_test(test_AMmapSetObject(List)),
        cmocka_unit_test(test_AMmapSetObject(Map)),
        cmocka_unit_test(test_AMmapSetObject(Text)),
        cmocka_unit_test(test_AMmapSetStr),
        cmocka_unit_test(test_AMmapSet(Timestamp)),
        cmocka_unit_test(test_AMmapSet(Uint)),
    };

    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
