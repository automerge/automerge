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
#include "cmocka_utils.h"
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
                          AMbytes(bytes_value, BYTES_SIZE}));                 \
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
        fail_msg_view("%s", AMerrorMessage(result));                               \
    }                                                                         \
    assert_int_equal(AMresultSize(result), 1);                                \
    assert_int_equal(AMresultValue(result).tag, AM_VALUE_NULL);               \
    AMfree(result);                                                           \
}

#define test_AMlistPutObject(label, mode) test_AMlistPutObject_ ## label ## _ ## mode

#define static_void_test_AMlistPutObject(label, mode)                         \
static void test_AMlistPutObject_ ## label ## _ ## mode(void **state) {       \
    GroupState* group_state = *state;                                         \
    AMobjType const obj_type = AMobjType_tag(#label);                         \
    if (obj_type != AM_OBJ_TYPE_VOID) {                                       \
        AMobjId const* const obj_id = AMpush(                                 \
            &group_state->stack,                                              \
            AMlistPutObject(group_state->doc,                                 \
                            AM_ROOT,                                          \
                            0,                                                \
                            !strcmp(#mode, "insert"),                         \
                            obj_type),                                        \
            AM_VALUE_OBJ_ID,                                                  \
            cmocka_cb).obj_id;                                                \
        assert_non_null(obj_id);                                              \
        assert_int_equal(AMobjObjType(group_state->doc, obj_id), obj_type);   \
        assert_int_equal(AMobjSize(group_state->doc, obj_id, NULL), 0);       \
    }                                                                         \
    else {                                                                    \
        AMpush(&group_state->stack,                                           \
               AMlistPutObject(group_state->doc,                              \
                               AM_ROOT,                                       \
                               0,                                             \
                               !strcmp(#mode, "insert"),                      \
                               obj_type),                                     \
               AM_VALUE_VOID,                                                 \
               NULL);                                                         \
        assert_int_not_equal(AMresultStatus(group_state->stack->result),      \
                                            AM_STATUS_OK);                    \
    }                                                                         \
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
                        AMstr(str_value)));                                   \
    AMbyteSpan const str = AMpush(                                            \
        &group_state->stack,                                                  \
        AMlistGet(group_state->doc, AM_ROOT, 0, NULL),                        \
        AM_VALUE_STR,                                                         \
        cmocka_cb).str;                                                       \
    char* const c_str = test_calloc(1, str.count + 1);                        \
    strncpy(c_str, str.src, str.count);                                       \
    print_message("str -> \"%s\"\n", c_str);                                  \
    test_free(c_str);                                                         \
    assert_int_equal(str.count, strlen(str_value));                           \
    assert_memory_equal(str.src, str_value, str.count);                       \
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

static_void_test_AMlistPutObject(Void, insert)

static_void_test_AMlistPutObject(Void, update)

static_void_test_AMlistPutStr(insert, "Hello, world!")

static_void_test_AMlistPutStr(update, "Hello, world!")

static_void_test_AMlistPut(Timestamp, insert, timestamp, INT64_MAX)

static_void_test_AMlistPut(Timestamp, update, timestamp, INT64_MAX)

static_void_test_AMlistPut(Uint, insert, uint, UINT64_MAX)

static_void_test_AMlistPut(Uint, update, uint, UINT64_MAX)

static void test_get_list_values(void** state) {
    AMresultStack* stack = *state;
    AMdoc* const doc1 = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMobjId const* const list = AMpush(
        &stack,
        AMmapPutObject(doc1, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;

    /* Insert elements. */
    AMfree(AMlistPutStr(doc1, list, 0, true, AMstr("First")));
    AMfree(AMlistPutStr(doc1, list, 0, true, AMstr("Second")));
    AMfree(AMlistPutStr(doc1, list, 0, true, AMstr("Third")));
    AMfree(AMlistPutStr(doc1, list, 0, true, AMstr("Fourth")));
    AMfree(AMlistPutStr(doc1, list, 0, true, AMstr("Fifth")));
    AMfree(AMlistPutStr(doc1, list, 0, true, AMstr("Sixth")));
    AMfree(AMlistPutStr(doc1, list, 0, true, AMstr("Seventh")));
    AMfree(AMlistPutStr(doc1, list, 0, true, AMstr("Eighth")));
    AMfree(AMcommit(doc1, AMstr(NULL), NULL));

    AMchangeHashes const v1 = AMpush(&stack,
                                     AMgetHeads(doc1),
                                     AM_VALUE_CHANGE_HASHES,
                                     cmocka_cb).change_hashes;
    AMdoc* const doc2 = AMpush(&stack,
                               AMfork(doc1, NULL),
                               AM_VALUE_DOC,
                               cmocka_cb).doc;

    AMfree(AMlistPutStr(doc1, list, 2, false, AMstr("Third V2")));
    AMfree(AMcommit(doc1, AMstr(NULL), NULL));

    AMfree(AMlistPutStr(doc2, list, 2, false, AMstr("Third V3")));
    AMfree(AMcommit(doc2, AMstr(NULL), NULL));

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

/** \brief A JavaScript application can introduce NUL (`\0`) characters into a
 *         list object's string value which will truncate it in a C application.
 */
static void test_get_NUL_string_value(void** state) {
    /*
    import * as Automerge from "@automerge/automerge";
    let doc = Automerge.init();
    doc = Automerge.change(doc, doc => {
        doc[0] = 'o\0ps';
    });
    const bytes = Automerge.save(doc);
    console.log("static uint8_t const SAVED_DOC[] = {" + Array.apply([], bytes).join(", ") + "};");
    */
    static uint8_t const OOPS_VALUE[] = {'o', '\0', 'p', 's'};
    static size_t const OOPS_SIZE = sizeof(OOPS_VALUE) / sizeof(uint8_t);

    static uint8_t const SAVED_DOC[] = {
        133, 111, 74, 131, 224, 28, 197, 17, 0, 113, 1, 16, 246, 137, 63, 193,
        255, 181, 76, 79, 129, 213, 133, 29, 214, 158, 164, 15, 1, 207, 184,
        14, 57, 1, 194, 79, 247, 82, 160, 134, 227, 144, 5, 241, 136, 205,
        238, 250, 251, 54, 34, 250, 210, 96, 204, 132, 153, 203, 110, 109, 6,
        6, 1, 2, 3, 2, 19, 2, 35, 2, 64, 2, 86, 2, 8, 21, 3, 33, 2, 35, 2, 52,
        1, 66, 2, 86, 2, 87, 4, 128, 1, 2, 127, 0, 127, 1, 127, 1, 127, 0,
        127, 0, 127, 7, 127, 1, 48, 127, 0, 127, 1, 1, 127, 1, 127, 70, 111,
        0, 112, 115, 127, 0, 0};
    static size_t const SAVED_DOC_SIZE = sizeof(SAVED_DOC) / sizeof(uint8_t);

    AMresultStack* stack = *state;
    AMdoc* const doc = AMpush(&stack,
                              AMload(SAVED_DOC, SAVED_DOC_SIZE),
                              AM_VALUE_DOC,
                              cmocka_cb).doc;
    AMbyteSpan const str = AMpush(&stack,
                                  AMlistGet(doc, AM_ROOT, 0, NULL),
                                  AM_VALUE_STR,
                                  cmocka_cb).str;
    assert_int_not_equal(str.count, strlen(OOPS_VALUE));
    assert_int_equal(str.count, OOPS_SIZE);
    assert_memory_equal(str.src, OOPS_VALUE, str.count);
}

static void test_insert_at_index(void** state) {
    AMresultStack* stack = *state;
    AMdoc* const doc = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;

    AMobjId const* const list = AMpush(
        &stack,
        AMlistPutObject(doc, AM_ROOT, 0, true, AM_OBJ_TYPE_LIST),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
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
        cmocka_unit_test(test_AMlistPutObject(Void, insert)),
        cmocka_unit_test(test_AMlistPutObject(Void, update)),
        cmocka_unit_test(test_AMlistPutStr(insert)),
        cmocka_unit_test(test_AMlistPutStr(update)),
        cmocka_unit_test(test_AMlistPut(Timestamp, insert)),
        cmocka_unit_test(test_AMlistPut(Timestamp, update)),
        cmocka_unit_test(test_AMlistPut(Uint, insert)),
        cmocka_unit_test(test_AMlistPut(Uint, update)),
        cmocka_unit_test_setup_teardown(test_get_list_values, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_get_NUL_string_value, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_insert_at_index, setup_stack, teardown_stack),
    };

    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
