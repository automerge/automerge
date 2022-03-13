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

#define test_AMlistSet(suffix, mode) test_AMlistSet ## suffix ## _ ## mode

#define static_void_test_AMlistSet(suffix, mode, member, scalar_value)        \
static void test_AMlistSet ## suffix ## _ ## mode(void **state) {             \
    GroupState* group_state = *state;                                         \
    AMresult* res = AMlistSet ## suffix(                                      \
        group_state->doc, AM_ROOT, 0, !strcmp(#mode, "insert"), scalar_value  \
    );                                                                        \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 0);                                   \
    AMvalue value = AMresultValue(res, 0);                                    \
    assert_int_equal(value.tag, AM_VALUE_NOTHING);                            \
    AMclear(res);                                                             \
    res = AMlistGet(group_state->doc, AM_ROOT, 0);                            \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 1);                                   \
    value = AMresultValue(res, 0);                                            \
    assert_int_equal(value.tag, AMvalue_discriminant(#suffix));               \
    assert_true(value.member == scalar_value);                                \
    AMclear(res);                                                             \
}

#define test_AMlistSetBytes(mode) test_AMlistSetBytes ## _ ## mode

#define static_void_test_AMlistSetBytes(mode, bytes_value)                    \
static void test_AMlistSetBytes_ ## mode(void **state) {                      \
    static size_t const BYTES_SIZE = sizeof(bytes_value) / sizeof(uint8_t);   \
                                                                              \
    GroupState* group_state = *state;                                         \
    AMresult* res = AMlistSetBytes(                                           \
        group_state->doc,                                                     \
        AM_ROOT,                                                              \
        0,                                                                    \
        !strcmp(#mode, "insert"),                                             \
        bytes_value,                                                          \
        BYTES_SIZE                                                            \
    );                                                                        \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 0);                                   \
    AMvalue value = AMresultValue(res, 0);                                    \
    assert_int_equal(value.tag, AM_VALUE_NOTHING);                            \
    AMclear(res);                                                             \
    res = AMlistGet(group_state->doc, AM_ROOT, 0);                            \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 1);                                   \
    value = AMresultValue(res, 0);                                            \
    assert_int_equal(value.tag, AM_VALUE_BYTES);                              \
    assert_int_equal(value.bytes.count, BYTES_SIZE);                          \
    assert_memory_equal(value.bytes.src, bytes_value, BYTES_SIZE);            \
    AMclear(res);                                                             \
}

#define test_AMlistSetNull(mode) test_AMlistSetNull_ ## mode

#define static_void_test_AMlistSetNull(mode)                                  \
static void test_AMlistSetNull_ ## mode(void **state) {                       \
    GroupState* group_state = *state;                                         \
    AMresult* res = AMlistSetNull(                                            \
        group_state->doc, AM_ROOT, 0, !strcmp(#mode, "insert"));              \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 0);                                   \
    AMvalue value = AMresultValue(res, 0);                                    \
    assert_int_equal(value.tag, AM_VALUE_NOTHING);                            \
    AMclear(res);                                                             \
    res = AMlistGet(group_state->doc, AM_ROOT, 0);                            \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 1);                                   \
    value = AMresultValue(res, 0);                                            \
    assert_int_equal(value.tag, AM_VALUE_NULL);                               \
    AMclear(res);                                                             \
}

#define test_AMlistSetObject(label, mode) test_AMlistSetObject_ ## label ## _ ## mode

#define static_void_test_AMlistSetObject(label, mode)                         \
static void test_AMlistSetObject_ ## label ## _ ## mode(void **state) {       \
    GroupState* group_state = *state;                                         \
    AMresult* res = AMlistSetObject(                                          \
        group_state->doc,                                                     \
        AM_ROOT,                                                              \
        0,                                                                    \
        !strcmp(#mode, "insert"),                                             \
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

#define test_AMlistSetStr(mode) test_AMlistSetStr ## _ ## mode

#define static_void_test_AMlistSetStr(mode, str_value)                        \
static void test_AMlistSetStr_ ## mode(void **state) {                        \
    static size_t const STR_LEN = strlen(str_value);                          \
                                                                              \
    GroupState* group_state = *state;                                         \
    AMresult* res = AMlistSetStr(                                             \
        group_state->doc,                                                     \
        AM_ROOT,                                                              \
        0,                                                                    \
        !strcmp(#mode, "insert"),                                             \
        str_value                                                             \
    );                                                                        \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 0);                                   \
    AMvalue value = AMresultValue(res, 0);                                    \
    assert_int_equal(value.tag, AM_VALUE_NOTHING);                            \
    AMclear(res);                                                             \
    res = AMlistGet(group_state->doc, AM_ROOT, 0);                            \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 1);                                   \
    value = AMresultValue(res, 0);                                            \
    assert_int_equal(value.tag, AM_VALUE_STR);                                \
    assert_int_equal(strlen(value.str), STR_LEN);                             \
    assert_memory_equal(value.str, str_value, STR_LEN + 1);                   \
    AMclear(res);                                                             \
}

static uint8_t const BYTES_VALUE[] = {INT8_MIN, INT8_MAX / 2, INT8_MAX};

static_void_test_AMlistSetBytes(insert, BYTES_VALUE)

static_void_test_AMlistSetBytes(update, BYTES_VALUE)

static_void_test_AMlistSet(Counter, insert, counter, INT64_MAX)

static_void_test_AMlistSet(Counter, update, counter, INT64_MAX)

static_void_test_AMlistSet(F64, insert, f64, DBL_MAX)

static_void_test_AMlistSet(F64, update, f64, DBL_MAX)

static_void_test_AMlistSet(Int, insert, int_, INT64_MAX)

static_void_test_AMlistSet(Int, update, int_, INT64_MAX)

static_void_test_AMlistSetNull(insert)

static_void_test_AMlistSetNull(update)

static_void_test_AMlistSetObject(List, insert)

static_void_test_AMlistSetObject(List, update)

static_void_test_AMlistSetObject(Map, insert)

static_void_test_AMlistSetObject(Map, update)

static_void_test_AMlistSetObject(Text, insert)

static_void_test_AMlistSetObject(Text, update)

static_void_test_AMlistSetStr(insert, "Hello, world!")

static_void_test_AMlistSetStr(update, "Hello, world!")

static_void_test_AMlistSet(Timestamp, insert, timestamp, INT64_MAX)

static_void_test_AMlistSet(Timestamp, update, timestamp, INT64_MAX)

static_void_test_AMlistSet(Uint, insert, uint, UINT64_MAX)

static_void_test_AMlistSet(Uint, update, uint, UINT64_MAX)

int run_AMlistSet_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMlistSetBytes(insert)),
        cmocka_unit_test(test_AMlistSetBytes(update)),
        cmocka_unit_test(test_AMlistSet(Counter, insert)),
        cmocka_unit_test(test_AMlistSet(Counter, update)),
        cmocka_unit_test(test_AMlistSet(F64, insert)),
        cmocka_unit_test(test_AMlistSet(F64, update)),
        cmocka_unit_test(test_AMlistSet(Int, insert)),
        cmocka_unit_test(test_AMlistSet(Int, update)),
        cmocka_unit_test(test_AMlistSetNull(insert)),
        cmocka_unit_test(test_AMlistSetNull(update)),
        cmocka_unit_test(test_AMlistSetObject(List, insert)),
        cmocka_unit_test(test_AMlistSetObject(List, update)),
        cmocka_unit_test(test_AMlistSetObject(Map, insert)),
        cmocka_unit_test(test_AMlistSetObject(Map, update)),
        cmocka_unit_test(test_AMlistSetObject(Text, insert)),
        cmocka_unit_test(test_AMlistSetObject(Text, update)),
        cmocka_unit_test(test_AMlistSetStr(insert)),
        cmocka_unit_test(test_AMlistSetStr(update)),
        cmocka_unit_test(test_AMlistSet(Timestamp, insert)),
        cmocka_unit_test(test_AMlistSet(Timestamp, update)),
        cmocka_unit_test(test_AMlistSet(Uint, insert)),
        cmocka_unit_test(test_AMlistSet(Uint, update)),
    };

    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
