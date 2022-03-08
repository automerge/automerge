#include <float.h>
#include <limits.h>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>

/* third-party */
#include <cmocka.h>

/* local */
#include "group_state.h"

#define test_AMmapSet(label) test_AMmapSet ## label

#define static_void_test_AMmapSet(label, value) \
static void test_AMmapSet ## label(void **state) { \
    GroupState* group_state = *state; \
    AMresult* res = AMmapSet ## label(group_state->doc, AM_ROOT, #label, value); \
    if (AMresultStatus(res) != AM_STATUS_COMMAND_OK) { \
        fail_msg("%s", AMerrorMessage(res)); \
    } \
}

static_void_test_AMmapSet(Int, INT64_MAX)

static_void_test_AMmapSet(Uint, UINT64_MAX)

static_void_test_AMmapSet(Str, "Hello, world!")

static_void_test_AMmapSet(F64, DBL_MAX)

static_void_test_AMmapSet(Counter, INT64_MAX)

static_void_test_AMmapSet(Timestamp, INT64_MAX)

static void test_AMmapSetBytes(void **state) {
    static uint8_t const BYTES_VALUE[] = {INT8_MIN, INT8_MAX / 2, INT8_MAX};

    GroupState* group_state = *state;
    AMresult* res = AMmapSetBytes(
        group_state->doc,
        AM_ROOT,
        "Bytes",
        BYTES_VALUE,
        sizeof(BYTES_VALUE) / sizeof(uint8_t)
    );
    if (AMresultStatus(res) != AM_STATUS_COMMAND_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
}

static void test_AMmapSetNull(void **state) {
    GroupState* group_state = *state;
    AMresult* res = AMmapSetNull(group_state->doc, AM_ROOT, "Null");
    if (AMresultStatus(res) != AM_STATUS_COMMAND_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
}

static void test_AMmapSetObject(void **state) {
    static AmObjType const OBJ_TYPES[] = {
        AM_OBJ_TYPE_LIST,
        AM_OBJ_TYPE_TEXT,
        AM_OBJ_TYPE_MAP,
    };
    static AmObjType const* const end = OBJ_TYPES + sizeof(OBJ_TYPES) / sizeof(AmObjType);

    GroupState* group_state = *state;
    for (AmObjType const* next = OBJ_TYPES; next != end; ++next) {
        AMresult* res = AMmapSetObject(
            group_state->doc,
            AM_ROOT,
            "Object",
            *next
        );
        if (AMresultStatus(res) != AM_STATUS_OBJ_OK) {
            fail_msg("%s", AMerrorMessage(res));
        }
    }
}

int run_AMmapSet_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMmapSet(Int)),
        cmocka_unit_test(test_AMmapSet(Uint)),
        cmocka_unit_test(test_AMmapSet(Str)),
        cmocka_unit_test(test_AMmapSet(F64)),
        cmocka_unit_test(test_AMmapSet(Counter)),
        cmocka_unit_test(test_AMmapSet(Timestamp)),
        cmocka_unit_test(test_AMmapSetBytes),
        cmocka_unit_test(test_AMmapSetNull),
        cmocka_unit_test(test_AMmapSetObject),
    };

    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
