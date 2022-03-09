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

#define test_AMlistSet(label, mode) test_AMlistSet ## label ## _ ## mode

#define static_void_test_AMlistSet(label, mode, value) \
static void test_AMlistSet ## label ## _ ## mode(void **state) { \
    GroupState* group_state = *state; \
    AMresult* res = AMlistSet ## label(group_state->doc, AM_ROOT, 0, !strcmp(#mode, "insert"), value); \
    if (AMresultStatus(res) != AM_STATUS_COMMAND_OK) { \
        fail_msg("%s", AMerrorMessage(res)); \
    } \
}

static_void_test_AMlistSet(Counter, insert, INT64_MAX)

static_void_test_AMlistSet(Counter, update, INT64_MAX)

static_void_test_AMlistSet(F64, insert, DBL_MAX)

static_void_test_AMlistSet(F64, update, DBL_MAX)

static_void_test_AMlistSet(Int, insert, INT64_MAX)

static_void_test_AMlistSet(Int, update, INT64_MAX)

static_void_test_AMlistSet(Str, insert, "Hello, world!")

static_void_test_AMlistSet(Str, update, "Hello, world!")

static_void_test_AMlistSet(Timestamp, insert, INT64_MAX)

static_void_test_AMlistSet(Timestamp, update, INT64_MAX)

static_void_test_AMlistSet(Uint, insert, UINT64_MAX)

static_void_test_AMlistSet(Uint, update, UINT64_MAX)

static void test_AMlistSetBytes_insert(void **state) {
    static uint8_t const BYTES_VALUE[] = {INT8_MIN, INT8_MAX / 2, INT8_MAX};

    GroupState* group_state = *state;
    AMresult* res = AMlistSetBytes(
        group_state->doc,
        AM_ROOT,
        0,
        true,
        BYTES_VALUE,
        sizeof(BYTES_VALUE) / sizeof(uint8_t)
    );
    if (AMresultStatus(res) != AM_STATUS_COMMAND_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
}

static void test_AMlistSetBytes_update(void **state) {
    static uint8_t const BYTES_VALUE[] = {INT8_MIN, INT8_MAX / 2, INT8_MAX};

    GroupState* group_state = *state;
    AMresult* res = AMlistSetBytes(
        group_state->doc,
        AM_ROOT,
        0,
        false,
        BYTES_VALUE,
        sizeof(BYTES_VALUE) / sizeof(uint8_t)
    );
    if (AMresultStatus(res) != AM_STATUS_COMMAND_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
}


static void test_AMlistSetNull_insert(void **state) {
    GroupState* group_state = *state;
    AMresult* res = AMlistSetNull(group_state->doc, AM_ROOT, 0, true);
    if (AMresultStatus(res) != AM_STATUS_COMMAND_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
}

static void test_AMlistSetNull_update(void **state) {
    GroupState* group_state = *state;
    AMresult* res = AMlistSetNull(group_state->doc, AM_ROOT, 0, false);
    if (AMresultStatus(res) != AM_STATUS_COMMAND_OK) {
        fail_msg("would be consolidated into%s", AMerrorMessage(res));
    }
}

static void test_AMlistSetObject_insert(void **state) {
    static AmObjType const OBJ_TYPES[] = {
        AM_OBJ_TYPE_LIST,
        AM_OBJ_TYPE_MAP,
        AM_OBJ_TYPE_TEXT,
    };
    static AmObjType const* const end = OBJ_TYPES + sizeof(OBJ_TYPES) / sizeof(AmObjType);

    GroupState* group_state = *state;
    for (AmObjType const* next = OBJ_TYPES; next != end; ++next) {
        AMresult* res = AMlistSetObject(
            group_state->doc,
            AM_ROOT,
            0,
            true,
            *next
        );
        if (AMresultStatus(res) != AM_STATUS_OBJ_OK) {
            fail_msg("%s", AMerrorMessage(res));
        }
    }
}

static void test_AMlistSetObject_update(void **state) {
    static AmObjType const OBJ_TYPES[] = {
        AM_OBJ_TYPE_LIST,
        AM_OBJ_TYPE_MAP,
        AM_OBJ_TYPE_TEXT,
    };
    static AmObjType const* const end = OBJ_TYPES + sizeof(OBJ_TYPES) / sizeof(AmObjType);

    GroupState* group_state = *state;
    for (AmObjType const* next = OBJ_TYPES; next != end; ++next) {
        AMresult* res = AMlistSetObject(
            group_state->doc,
            AM_ROOT,
            0,
            false,
            *next
        );
        if (AMresultStatus(res) != AM_STATUS_OBJ_OK) {
            fail_msg("%s", AMerrorMessage(res));
        }
    }
}

int run_AMlistSet_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMlistSetBytes_insert),
        cmocka_unit_test(test_AMlistSetBytes_update),
        cmocka_unit_test(test_AMlistSet(Counter, insert)),
        cmocka_unit_test(test_AMlistSet(Counter, update)),
        cmocka_unit_test(test_AMlistSet(F64, insert)),
        cmocka_unit_test(test_AMlistSet(F64, update)),
        cmocka_unit_test(test_AMlistSet(Int, insert)),
        cmocka_unit_test(test_AMlistSet(Int, update)),
        cmocka_unit_test(test_AMlistSetNull_insert),
        cmocka_unit_test(test_AMlistSetNull_update),
        cmocka_unit_test(test_AMlistSetObject_insert),
        cmocka_unit_test(test_AMlistSetObject_update),
        cmocka_unit_test(test_AMlistSet(Str, insert)),
        cmocka_unit_test(test_AMlistSet(Str, update)),
        cmocka_unit_test(test_AMlistSet(Timestamp, insert)),
        cmocka_unit_test(test_AMlistSet(Timestamp, update)),
        cmocka_unit_test(test_AMlistSet(Uint, insert)),
        cmocka_unit_test(test_AMlistSet(Uint, update)),
    };

    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
