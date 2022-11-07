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
#include <automerge-c/automerge.h>
#include "group_state.h"
#include "macro_utils.h"
#include "stack_utils.h"

static void test_AMlistIncrement(void** state) {
    GroupState* group_state = *state;
    AMfree(AMlistPutCounter(group_state->doc, AM_ROOT, 0, true, 0));
    assert_int_equal(AMpush(&group_state->stack,
                            AMlistGet(group_state->doc, AM_ROOT, 0, NULL),
                            AM_VALUE_COUNTER,
                            cmocka_cb).counter, 0);
    AMfree(AMpop(&group_state->stack));
    AMfree(AMlistIncrement(group_state->doc, AM_ROOT, 0, 3));
    assert_int_equal(AMpush(&group_state->stack,
                            AMlistGet(group_state->doc, AM_ROOT, 0, NULL),
                            AM_VALUE_COUNTER,
                            cmocka_cb).counter, 3);
    AMfree(AMpop(&group_state->stack));
}

#define test_AMlistPut(suffix, mode) test_AMlistPut ## suffix ## _ ## mode

#define static_void_test_AMlistPut(suffix, mode, member, scalar_value)        \
static void test_AMlistPut ## suffix ## _ ## mode(void **state) {             \
    GroupState* group_state = *state;                                         \
    AMfree(AMlistPut ## suffix(group_state->doc,                              \
                               AM_ROOT,                                       \
                               0,                                             \
                               !strcmp(#mode, "insert"),                      \
                               scalar_value));                                \
    assert_true(AMpush(                                                       \
        &group_state->stack,                                                  \
        AMlistGet(group_state->doc, AM_ROOT, 0, NULL),                        \
        AMvalue_discriminant(#suffix),                                        \
        cmocka_cb).member == scalar_value);                                   \
    AMfree(AMpop(&group_state->stack));                                       \
}

#define test_AMlistPutBytes(mode) test_AMlistPutBytes ## _ ## mode

#define static_void_test_AMlistPutBytes(mode, bytes_value)                    \
static void test_AMlistPutBytes_ ## mode(void **state) {                      \
    static size_t const BYTES_SIZE = sizeof(bytes_value) / sizeof(uint8_t);   \
                                                                              \
    GroupState* group_state = *state;                                         \
    AMfree(AMlistPutBytes(group_state->doc,                                   \
                          AM_ROOT,                                            \
                          0,                                                  \
                          !strcmp(#mode, "insert"),                           \
                          bytes_value,                                        \
                          BYTES_SIZE));                                       \
    AMbyteSpan const bytes = AMpush(                                          \
        &group_state->stack,                                                  \
        AMlistGet(group_state->doc, AM_ROOT, 0, NULL),                        \
        AM_VALUE_BYTES,                                                       \
        cmocka_cb).bytes;                                                     \
    assert_int_equal(bytes.count, BYTES_SIZE);                                \
    assert_memory_equal(bytes.src, bytes_value, BYTES_SIZE);                  \
    AMfree(AMpop(&group_state->stack));                                       \
}

#define test_AMlistPutNull(mode) test_AMlistPutNull_ ## mode

#define static_void_test_AMlistPutNull(mode)                                  \
static void test_AMlistPutNull_ ## mode(void **state) {                       \
    GroupState* group_state = *state;                                         \
    AMfree(AMlistPutNull(group_state->doc,                                    \
                         AM_ROOT,                                             \
                         0,                                                   \
                         !strcmp(#mode, "insert")));                          \
    AMresult* const result = AMlistGet(group_state->doc, AM_ROOT, 0, NULL);   \
    if (AMresultStatus(result) != AM_STATUS_OK) {                             \
        fail_msg("%s", AMerrorMessage(result));                               \
    }                                                                         \
    assert_int_equal(AMresultSize(result), 1);                                \
    assert_int_equal(AMresultValue(result).tag, AM_VALUE_NULL);               \
    AMfree(result);                                                           \
}

#define test_AMlistPutObject(label, mode) test_AMlistPutObject_ ## label ## _ ## mode

#define static_void_test_AMlistPutObject(label, mode)                         \
static void test_AMlistPutObject_ ## label ## _ ## mode(void **state) {       \
    GroupState* group_state = *state;                                         \
    AMobjId const* const obj_id = AMpush(                                     \
        &group_state->stack,                                                  \
        AMlistPutObject(group_state->doc,                                     \
                        AM_ROOT,                                              \
                        0,                                                    \
                        !strcmp(#mode, "insert"),                             \
                        AMobjType_tag(#label)),                               \
        AM_VALUE_OBJ_ID,                                                      \
        cmocka_cb).obj_id;                                                    \
    assert_non_null(obj_id);                                                  \
    assert_int_equal(AMobjSize(group_state->doc, obj_id, NULL), 0);           \
    AMfree(AMpop(&group_state->stack));                                       \
}

#define test_AMlistPutStr(mode) test_AMlistPutStr ## _ ## mode

#define static_void_test_AMlistPutStr(mode, str_value)                        \
static void test_AMlistPutStr_ ## mode(void **state) {                        \
    GroupState* group_state = *state;                                         \
    AMfree(AMlistPutStr(group_state->doc,                                     \
                        AM_ROOT,                                              \
                        0,                                                    \
                        !strcmp(#mode, "insert"),                             \
                        str_value));                                          \
    assert_string_equal(AMpush(                                               \
        &group_state->stack,                                                  \
        AMlistGet(group_state->doc, AM_ROOT, 0, NULL),                        \
        AM_VALUE_STR,                                                         \
        cmocka_cb).str, str_value);                                           \
    AMfree(AMpop(&group_state->stack));                                       \
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

static void test_insert_at_index(void** state) {
    AMresultStack* stack = *state;
    AMdoc* const doc = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;

    AMobjId const* const list = AMpush(
        &stack,
        AMlistPutObject(doc, AM_ROOT, 0, true, AM_OBJ_TYPE_LIST),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    assert_int_equal(AMobjObjType(doc, list), AM_OBJ_TYPE_LIST);
    /* Insert both at the same index. */
    AMfree(AMlistPutUint(doc, list, 0, true, 0));
    AMfree(AMlistPutUint(doc, list, 0, true, 1));

    assert_int_equal(AMobjSize(doc, list, NULL), 2);
    AMstrs const keys = AMpush(&stack,
                               AMkeys(doc, list, NULL),
                               AM_VALUE_STRS,
                               cmocka_cb).strs;
    assert_int_equal(AMstrsSize(&keys), 2);
    AMlistItems const range = AMpush(&stack,
                                     AMlistRange(doc, list, 0, SIZE_MAX, NULL),
                                     AM_VALUE_LIST_ITEMS,
                                     cmocka_cb).list_items;
    assert_int_equal(AMlistItemsSize(&range), 2);
}

static void test_get_list_values(void** state) {
    AMresultStack* stack = *state;
    AMdoc* const doc1 = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMobjId const* const list = AMpush(
        &stack,
        AMmapPutObject(doc1, AM_ROOT, "list", AM_OBJ_TYPE_LIST),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;

    /* Insert elements. */
    AMfree(AMlistPutStr(doc1, list, 0, true, "First"));
    AMfree(AMlistPutStr(doc1, list, 0, true, "Second"));
    AMfree(AMlistPutStr(doc1, list, 0, true, "Third"));
    AMfree(AMlistPutStr(doc1, list, 0, true, "Fourth"));
    AMfree(AMlistPutStr(doc1, list, 0, true, "Fifth"));
    AMfree(AMlistPutStr(doc1, list, 0, true, "Sixth"));
    AMfree(AMlistPutStr(doc1, list, 0, true, "Seventh"));
    AMfree(AMlistPutStr(doc1, list, 0, true, "Eighth"));
    AMfree(AMcommit(doc1, NULL, NULL));

    AMchangeHashes const v1 = AMpush(&stack,
                                     AMgetHeads(doc1),
                                     AM_VALUE_CHANGE_HASHES,
                                     cmocka_cb).change_hashes;
    AMdoc* const doc2 = AMpush(&stack,
                               AMfork(doc1, NULL),
                               AM_VALUE_DOC,
                               cmocka_cb).doc;

    AMfree(AMlistPutStr(doc1, list, 2, false, "Third V2"));
    AMfree(AMcommit(doc1, NULL, NULL));

    AMfree(AMlistPutStr(doc2, list, 2, false, "Third V3"));
    AMfree(AMcommit(doc2, NULL, NULL));

    AMfree(AMmerge(doc1, doc2));

    AMlistItems range = AMpush(&stack,
                               AMlistRange(doc1, list, 0, SIZE_MAX, NULL),
                                                                                                                                                                                                    AM_VALUE_LIST_ITEMS,
        cmocka_cb).list_items;
    assert_int_equal(AMlistItemsSize(&range), 8);

    AMlistItem const* list_item = NULL;
    while ((list_item = AMlistItemsNext(&range, 1)) != NULL) {
        AMvalue const val1 = AMlistItemValue(list_item);
        AMresult* result = AMlistGet(doc1, list, AMlistItemIndex(list_item), NULL);
        AMvalue const val2 = AMresultValue(result);
        assert_true(AMvalueEqual(&val1, &val2));
        assert_non_null(AMlistItemObjId(list_item));
        AMfree(result);
    }

    range = AMpush(&stack,
                   AMlistRange(doc1, list, 3, 6, NULL),
                   AM_VALUE_LIST_ITEMS,
                   cmocka_cb).list_items;
    AMlistItems range_back = AMlistItemsReversed(&range);
    assert_int_equal(AMlistItemsSize(&range), 3);
    assert_int_equal(AMlistItemIndex(AMlistItemsNext(&range, 1)), 3);
    assert_int_equal(AMlistItemIndex(AMlistItemsNext(&range_back, 1)), 5);

    range = AMlistItemsRewound(&range);
    while ((list_item = AMlistItemsNext(&range, 1)) != NULL) {
        AMvalue const val1 = AMlistItemValue(list_item);
        AMresult* result = AMlistGet(doc1, list, AMlistItemIndex(list_item), NULL);
        AMvalue const val2 = AMresultValue(result);
        assert_true(AMvalueEqual(&val1, &val2));
        assert_non_null(AMlistItemObjId(list_item));
        AMfree(result);
    }

    range = AMpush(&stack,
                   AMlistRange(doc1, list, 0, SIZE_MAX, &v1),
                   AM_VALUE_LIST_ITEMS,
                   cmocka_cb).list_items;
    assert_int_equal(AMlistItemsSize(&range), 8);
    while ((list_item = AMlistItemsNext(&range, 1)) != NULL) {
        AMvalue const val1 = AMlistItemValue(list_item);
        AMresult* result = AMlistGet(doc1, list, AMlistItemIndex(list_item), &v1);
        AMvalue const val2 = AMresultValue(result);
        assert_true(AMvalueEqual(&val1, &val2));
        assert_non_null(AMlistItemObjId(list_item));
        AMfree(result);
    }

    range = AMpush(&stack,
                   AMlistRange(doc1, list, 3, 6, &v1),
        AM_VALUE_LIST_ITEMS,
        cmocka_cb).list_items;
    range_back = AMlistItemsReversed(&range);
    assert_int_equal(AMlistItemsSize(&range), 3);
    assert_int_equal(AMlistItemIndex(AMlistItemsNext(&range, 1)), 3);
    assert_int_equal(AMlistItemIndex(AMlistItemsNext(&range_back, 1)), 5);

    range = AMlistItemsRewound(&range);
    while ((list_item = AMlistItemsNext(&range, 1)) != NULL) {
        AMvalue const val1 = AMlistItemValue(list_item);
        AMresult* result = AMlistGet(doc1, list, AMlistItemIndex(list_item), &v1);
        AMvalue const val2 = AMresultValue(result);
        assert_true(AMvalueEqual(&val1, &val2));
        assert_non_null(AMlistItemObjId(list_item));
        AMfree(result);
    }

    range = AMpush(&stack,
                   AMlistRange(doc1, list, 0, SIZE_MAX, NULL),
                   AM_VALUE_LIST_ITEMS,
                   cmocka_cb).list_items;
    AMobjItems values = AMpush(&stack,
                               AMobjValues(doc1, list, NULL),
                               AM_VALUE_OBJ_ITEMS,
                               cmocka_cb).obj_items;
    assert_int_equal(AMlistItemsSize(&range), AMobjItemsSize(&values));
    AMobjItem const* value = NULL;
    while ((list_item = AMlistItemsNext(&range, 1)) != NULL &&
           (value = AMobjItemsNext(&values, 1)) != NULL) {
        AMvalue const val1 = AMlistItemValue(list_item);
        AMvalue const val2 = AMobjItemValue(value);
        assert_true(AMvalueEqual(&val1, &val2));
        assert_true(AMobjIdEqual(AMlistItemObjId(list_item), AMobjItemObjId(value)));
    }

    range = AMpush(&stack,
                   AMlistRange(doc1, list, 0, SIZE_MAX, &v1),
                   AM_VALUE_LIST_ITEMS,
                   cmocka_cb).list_items;
    values = AMpush(&stack,
                    AMobjValues(doc1, list, &v1),
                    AM_VALUE_OBJ_ITEMS,
                    cmocka_cb).obj_items;
    assert_int_equal(AMlistItemsSize(&range), AMobjItemsSize(&values));
    while ((list_item = AMlistItemsNext(&range, 1)) != NULL &&
           (value = AMobjItemsNext(&values, 1)) != NULL) {
        AMvalue const val1 = AMlistItemValue(list_item);
        AMvalue const val2 = AMobjItemValue(value);
        assert_true(AMvalueEqual(&val1, &val2));
        assert_true(AMobjIdEqual(AMlistItemObjId(list_item), AMobjItemObjId(value)));
    }
}

int run_list_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMlistIncrement),
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
        cmocka_unit_test_setup_teardown(test_insert_at_index, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_get_list_values, setup_stack, teardown_stack),
    };

    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
