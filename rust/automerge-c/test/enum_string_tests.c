#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

/* third-party */
#include <cmocka.h>

/* local */
#include <automerge-c/automerge.h>
#include <automerge-c/utils/enum_string.h>

#define assert_to_string(function, tag) assert_string_equal(function(tag), #tag)

#define assert_from_string(function, type, tag) \
    do {                                        \
        type out;                               \
        assert_true(function(&out, #tag));      \
        assert_int_equal(out, tag);             \
    } while (0)

static void test_AMidxTypeToString(void** state) {
    assert_to_string(AMidxTypeToString, AM_IDX_TYPE_DEFAULT);
    assert_to_string(AMidxTypeToString, AM_IDX_TYPE_KEY);
    assert_to_string(AMidxTypeToString, AM_IDX_TYPE_POS);
    /* Zero tag */
    assert_string_equal(AMidxTypeToString(0), "AM_IDX_TYPE_DEFAULT");
    /* Invalid tag */
    assert_string_equal(AMidxTypeToString(-1), "???");
}

static void test_AMidxTypeFromString(void** state) {
    assert_from_string(AMidxTypeFromString, AMidxType, AM_IDX_TYPE_DEFAULT);
    assert_from_string(AMidxTypeFromString, AMidxType, AM_IDX_TYPE_KEY);
    assert_from_string(AMidxTypeFromString, AMidxType, AM_IDX_TYPE_POS);
    /* Invalid tag */
    AMidxType out = -1;
    assert_false(AMidxTypeFromString(&out, "???"));
    assert_int_equal(out, (AMidxType)-1);
}

static void test_AMobjTypeToString(void** state) {
    assert_to_string(AMobjTypeToString, AM_OBJ_TYPE_DEFAULT);
    assert_to_string(AMobjTypeToString, AM_OBJ_TYPE_LIST);
    assert_to_string(AMobjTypeToString, AM_OBJ_TYPE_MAP);
    assert_to_string(AMobjTypeToString, AM_OBJ_TYPE_TEXT);
    /* Zero tag */
    assert_string_equal(AMobjTypeToString(0), "AM_OBJ_TYPE_DEFAULT");
    /* Invalid tag */
    assert_string_equal(AMobjTypeToString(-1), "???");
}

static void test_AMobjTypeFromString(void** state) {
    assert_from_string(AMobjTypeFromString, AMobjType, AM_OBJ_TYPE_DEFAULT);
    assert_from_string(AMobjTypeFromString, AMobjType, AM_OBJ_TYPE_LIST);
    assert_from_string(AMobjTypeFromString, AMobjType, AM_OBJ_TYPE_MAP);
    assert_from_string(AMobjTypeFromString, AMobjType, AM_OBJ_TYPE_TEXT);
    /* Invalid tag */
    AMobjType out = -1;
    assert_false(AMobjTypeFromString(&out, "???"));
    assert_int_equal(out, (AMobjType)-1);
}

static void test_AMstatusToString(void** state) {
    assert_to_string(AMstatusToString, AM_STATUS_ERROR);
    assert_to_string(AMstatusToString, AM_STATUS_INVALID_RESULT);
    assert_to_string(AMstatusToString, AM_STATUS_OK);
    /* Zero tag */
    assert_string_equal(AMstatusToString(0), "AM_STATUS_OK");
    /* Invalid tag */
    assert_string_equal(AMstatusToString(-1), "???");
}

static void test_AMstatusFromString(void** state) {
    assert_from_string(AMstatusFromString, AMstatus, AM_STATUS_ERROR);
    assert_from_string(AMstatusFromString, AMstatus, AM_STATUS_INVALID_RESULT);
    assert_from_string(AMstatusFromString, AMstatus, AM_STATUS_OK);
    /* Invalid tag */
    AMstatus out = -1;
    assert_false(AMstatusFromString(&out, "???"));
    assert_int_equal(out, (AMstatus)-1);
}

static void test_AMvalTypeToString(void** state) {
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_ACTOR_ID);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_BOOL);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_BYTES);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_CHANGE);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_CHANGE_HASH);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_COUNTER);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_DEFAULT);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_DOC);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_F64);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_INT);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_NULL);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_OBJ_TYPE);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_STR);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_SYNC_HAVE);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_SYNC_MESSAGE);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_SYNC_STATE);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_TIMESTAMP);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_UINT);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_UNKNOWN);
    assert_to_string(AMvalTypeToString, AM_VAL_TYPE_VOID);
    /* Zero tag */
    assert_string_equal(AMvalTypeToString(0), "AM_VAL_TYPE_DEFAULT");
    /* Invalid tag */
    assert_string_equal(AMvalTypeToString(-1), "???");
}

static void test_AMvalTypeFromString(void** state) {
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_ACTOR_ID);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_BOOL);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_BYTES);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_CHANGE);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_CHANGE_HASH);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_COUNTER);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_DEFAULT);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_DOC);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_F64);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_INT);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_NULL);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_OBJ_TYPE);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_STR);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_SYNC_HAVE);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_SYNC_MESSAGE);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_SYNC_STATE);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_TIMESTAMP);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_UINT);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_UNKNOWN);
    assert_from_string(AMvalTypeFromString, AMvalType, AM_VAL_TYPE_VOID);
    /* Invalid tag */
    AMvalType out = -1;
    assert_false(AMvalTypeFromString(&out, "???"));
    assert_int_equal(out, (AMvalType)-1);
}

int run_enum_string_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMidxTypeToString), cmocka_unit_test(test_AMidxTypeFromString),
        cmocka_unit_test(test_AMobjTypeToString), cmocka_unit_test(test_AMobjTypeFromString),
        cmocka_unit_test(test_AMstatusToString),  cmocka_unit_test(test_AMstatusFromString),
        cmocka_unit_test(test_AMvalTypeToString), cmocka_unit_test(test_AMvalTypeFromString),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
