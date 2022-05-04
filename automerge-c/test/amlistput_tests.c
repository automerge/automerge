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

#define test_AMlistPut(suffix, mode) test_AMlistPut ## suffix ## _ ## mode

#define static_void_test_AMlistPut(suffix, mode, member, scalar_value)        \
static void test_AMlistPut ## suffix ## _ ## mode(void **state) {             \
    GroupState* group_state = *state;                                         \
    AMresult* res = AMlistPut ## suffix(                                      \
        group_state->doc, AM_ROOT, 0, !strcmp(#mode, "insert"), scalar_value  \
    );                                                                        \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 0);                                   \
    AMvalue value = AMresultValue(res, 0);                                    \
    assert_int_equal(value.tag, AM_VALUE_VOID);                            \
    AMfreeResult(res);                                                        \
    res = AMlistGet(group_state->doc, AM_ROOT, 0);                            \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 1);                                   \
    value = AMresultValue(res, 0);                                            \
    assert_int_equal(value.tag, AMvalue_discriminant(#suffix));               \
    assert_true(value.member == scalar_value);                                \
    AMfreeResult(res);                                                        \
}

#define test_AMlistPutBytes(mode) test_AMlistPutBytes ## _ ## mode

#define static_void_test_AMlistPutBytes(mode, bytes_value)                    \
static void test_AMlistPutBytes_ ## mode(void **state) {                      \
    static size_t const BYTES_SIZE = sizeof(bytes_value) / sizeof(uint8_t);   \
                                                                              \
    GroupState* group_state = *state;                                         \
    AMresult* res = AMlistPutBytes(                                           \
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
    assert_int_equal(value.tag, AM_VALUE_VOID);                            \
    AMfreeResult(res);                                                        \
    res = AMlistGet(group_state->doc, AM_ROOT, 0);                            \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 1);                                   \
    value = AMresultValue(res, 0);                                            \
    assert_int_equal(value.tag, AM_VALUE_BYTES);                              \
    assert_int_equal(value.bytes.count, BYTES_SIZE);                          \
    assert_memory_equal(value.bytes.src, bytes_value, BYTES_SIZE);            \
    AMfreeResult(res);                                                        \
}

#define test_AMlistPutNull(mode) test_AMlistPutNull_ ## mode

#define static_void_test_AMlistPutNull(mode)                                  \
static void test_AMlistPutNull_ ## mode(void **state) {                       \
    GroupState* group_state = *state;                                         \
    AMresult* res = AMlistPutNull(                                            \
        group_state->doc, AM_ROOT, 0, !strcmp(#mode, "insert"));              \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 0);                                   \
    AMvalue value = AMresultValue(res, 0);                                    \
    assert_int_equal(value.tag, AM_VALUE_VOID);                            \
    AMfreeResult(res);                                                        \
    res = AMlistGet(group_state->doc, AM_ROOT, 0);                            \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 1);                                   \
    value = AMresultValue(res, 0);                                            \
    assert_int_equal(value.tag, AM_VALUE_NULL);                               \
    AMfreeResult(res);                                                        \
}

#define test_AMlistPutObject(label, mode) test_AMlistPutObject_ ## label ## _ ## mode

#define static_void_test_AMlistPutObject(label, mode)                         \
static void test_AMlistPutObject_ ## label ## _ ## mode(void **state) {       \
    GroupState* group_state = *state;                                         \
    AMresult* res = AMlistPutObject(                                          \
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
    assert_int_equal(value.tag, AM_VALUE_OBJ_ID);                             \
    assert_non_null(value.obj_id);                                            \
    assert_int_equal(AMobjSize(group_state->doc, value.obj_id), 0);           \
    AMfreeResult(res);                                                        \
}

#define test_AMlistPutStr(mode) test_AMlistPutStr ## _ ## mode

#define static_void_test_AMlistPutStr(mode, str_value)                        \
static void test_AMlistPutStr_ ## mode(void **state) {                        \
    static size_t const STR_LEN = strlen(str_value);                          \
                                                                              \
    GroupState* group_state = *state;                                         \
    AMresult* res = AMlistPutStr(                                             \
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
    assert_int_equal(value.tag, AM_VALUE_VOID);                            \
    AMfreeResult(res);                                                        \
    res = AMlistGet(group_state->doc, AM_ROOT, 0);                            \
    if (AMresultStatus(res) != AM_STATUS_OK) {                                \
        fail_msg("%s", AMerrorMessage(res));                                  \
    }                                                                         \
    assert_int_equal(AMresultSize(res), 1);                                   \
    value = AMresultValue(res, 0);                                            \
    assert_int_equal(value.tag, AM_VALUE_STR);                                \
    assert_int_equal(strlen(value.str), STR_LEN);                             \
    assert_memory_equal(value.str, str_value, STR_LEN + 1);                   \
    AMfreeResult(res);                                                        \
}

static_void_test_AMlistPut(Bool, insert, boolean, true)

static_void_test_AMlistPut(Bool, update, boolean, true)

static uint8_t const BYTES_VALUE[] = {INT8_MIN, INT8_MAX / 2, INT8_MAX};

static_void_test_AMlistPutBytes(insert, BYTES_VALUE)

static_void_test_AMlistPutBytes(update, BYTES_VALUE)

static_void_test_AMlistPut(Counter, insert, counter, INT64_MAX)

static_void_test_AMlistPut(Counter, update, counter, INT64_MAX)

static_void_test_AMlistPut(F64, insert, f64, DBL_MAX)

static_void_test_AMlistPut(F64, update, f64, DBL_MAX)

static_void_test_AMlistPut(Int, insert, int_, INT64_MAX)

static_void_test_AMlistPut(Int, update, int_, INT64_MAX)

static_void_test_AMlistPutNull(insert)

static_void_test_AMlistPutNull(update)

static_void_test_AMlistPutObject(List, insert)

static_void_test_AMlistPutObject(List, update)

static_void_test_AMlistPutObject(Map, insert)

static_void_test_AMlistPutObject(Map, update)

static_void_test_AMlistPutObject(Text, insert)

static_void_test_AMlistPutObject(Text, update)

static_void_test_AMlistPutStr(insert, "Hello, world!")

static_void_test_AMlistPutStr(update, "Hello, world!")

static_void_test_AMlistPut(Timestamp, insert, timestamp, INT64_MAX)

static_void_test_AMlistPut(Timestamp, update, timestamp, INT64_MAX)

static_void_test_AMlistPut(Uint, insert, uint, UINT64_MAX)

static_void_test_AMlistPut(Uint, update, uint, UINT64_MAX)

int run_AMlistPut_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMlistPut(Bool, insert)),
        cmocka_unit_test(test_AMlistPut(Bool, update)),
        cmocka_unit_test(test_AMlistPutBytes(insert)),
        cmocka_unit_test(test_AMlistPutBytes(update)),
        cmocka_unit_test(test_AMlistPut(Counter, insert)),
        cmocka_unit_test(test_AMlistPut(Counter, update)),
        cmocka_unit_test(test_AMlistPut(F64, insert)),
        cmocka_unit_test(test_AMlistPut(F64, update)),
        cmocka_unit_test(test_AMlistPut(Int, insert)),
        cmocka_unit_test(test_AMlistPut(Int, update)),
        cmocka_unit_test(test_AMlistPutNull(insert)),
        cmocka_unit_test(test_AMlistPutNull(update)),
        cmocka_unit_test(test_AMlistPutObject(List, insert)),
        cmocka_unit_test(test_AMlistPutObject(List, update)),
        cmocka_unit_test(test_AMlistPutObject(Map, insert)),
        cmocka_unit_test(test_AMlistPutObject(Map, update)),
        cmocka_unit_test(test_AMlistPutObject(Text, insert)),
        cmocka_unit_test(test_AMlistPutObject(Text, update)),
        cmocka_unit_test(test_AMlistPutStr(insert)),
        cmocka_unit_test(test_AMlistPutStr(update)),
        cmocka_unit_test(test_AMlistPut(Timestamp, insert)),
        cmocka_unit_test(test_AMlistPut(Timestamp, update)),
        cmocka_unit_test(test_AMlistPut(Uint, insert)),
        cmocka_unit_test(test_AMlistPut(Uint, update)),
    };

    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
