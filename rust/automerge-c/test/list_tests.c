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
#include <automerge-c/utils/stack_callback_data.h>
#include "base_state.h"
#include "cmocka_utils.h"
#include "doc_state.h"
#include "macro_utils.h"

static void test_AMlistIncrement(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;
    AMobjId const* const list =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc_state->doc, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST),
                                cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    AMstackItem(NULL, AMlistPutCounter(doc_state->doc, list, 0, true, 0), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMitem const* const item =
        AMstackItem(stack_ptr, AMlistGet(doc_state->doc, list, 0, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_COUNTER));
    assert_int_equal(AMitemIdxType(item), AM_IDX_TYPE_POS);
    uint64_t pos;
    assert_true(AMitemPos(item, &pos));
    assert_int_equal(pos, 0);
    int64_t counter;
    assert_true(AMitemToCounter(item, &counter));
    assert_int_equal(counter, 0);
    AMresultFree(AMstackPop(stack_ptr, NULL));
    AMstackItem(NULL, AMlistIncrement(doc_state->doc, list, 0, 3), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    assert_true(AMitemToCounter(
        AMstackItem(stack_ptr, AMlistGet(doc_state->doc, list, 0, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_COUNTER)),
        &counter));
    assert_int_equal(counter, 3);
    AMresultFree(AMstackPop(stack_ptr, NULL));
}

#define test_AMlistPut(suffix, mode) test_AMlistPut##suffix##_##mode

#define static_void_test_AMlistPut(suffix, mode, type, scalar_value)                                           \
    static void test_AMlistPut##suffix##_##mode(void** state) {                                                \
        DocState* doc_state = *state;                                                                          \
        AMstack** stack_ptr = &doc_state->base_state->stack;                                                   \
        AMobjId const* const list = AMitemObjId(                                                               \
            AMstackItem(stack_ptr, AMmapPutObject(doc_state->doc, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST),   \
                        cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));                                           \
        AMstackItem(NULL, AMlistPut##suffix(doc_state->doc, list, 0, !strcmp(#mode, "insert"), scalar_value),  \
                    cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));                                                    \
        AMitem const* const item = AMstackItem(stack_ptr, AMlistGet(doc_state->doc, list, 0, NULL), cmocka_cb, \
                                               AMexpect(suffix_to_val_type(#suffix)));                         \
        assert_int_equal(AMitemIdxType(item), AM_IDX_TYPE_POS);                                                \
        uint64_t pos;                                                                                          \
        assert_true(AMitemPos(item, &pos));                                                                    \
        assert_int_equal(pos, 0);                                                                              \
        type value;                                                                                            \
        assert_true(AMitemTo##suffix(item, &value));                                                           \
        assert_true(value == scalar_value);                                                                    \
        AMresultFree(AMstackPop(stack_ptr, NULL));                                                             \
    }

#define test_AMlistPutBytes(mode) test_AMlistPutBytes##_##mode

#define static_void_test_AMlistPutBytes(mode, bytes_value)                                                             \
    static void test_AMlistPutBytes_##mode(void** state) {                                                             \
        static size_t const BYTES_SIZE = sizeof(bytes_value) / sizeof(uint8_t);                                        \
                                                                                                                       \
        DocState* doc_state = *state;                                                                                  \
        AMstack** stack_ptr = &doc_state->base_state->stack;                                                           \
        AMobjId const* const list = AMitemObjId(                                                                       \
            AMstackItem(stack_ptr, AMmapPutObject(doc_state->doc, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST),           \
                        cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));                                                   \
        AMstackItem(                                                                                                   \
            NULL, AMlistPutBytes(doc_state->doc, list, 0, !strcmp(#mode, "insert"), AMbytes(bytes_value, BYTES_SIZE)), \
            cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));                                                                    \
        AMitem const* const item =                                                                                     \
            AMstackItem(stack_ptr, AMlistGet(doc_state->doc, list, 0, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES));  \
        assert_int_equal(AMitemIdxType(item), AM_IDX_TYPE_POS);                                                        \
        uint64_t pos;                                                                                                  \
        assert_true(AMitemPos(item, &pos));                                                                            \
        assert_int_equal(pos, 0);                                                                                      \
        AMbyteSpan bytes;                                                                                              \
        assert_true(AMitemToBytes(item, &bytes));                                                                      \
        assert_int_equal(bytes.count, BYTES_SIZE);                                                                     \
        assert_memory_equal(bytes.src, bytes_value, BYTES_SIZE);                                                       \
        AMresultFree(AMstackPop(stack_ptr, NULL));                                                                     \
    }

#define test_AMlistPutNull(mode) test_AMlistPutNull_##mode

#define static_void_test_AMlistPutNull(mode)                                                                 \
    static void test_AMlistPutNull_##mode(void** state) {                                                    \
        DocState* doc_state = *state;                                                                        \
        AMstack** stack_ptr = &doc_state->base_state->stack;                                                 \
        AMobjId const* const list = AMitemObjId(                                                             \
            AMstackItem(stack_ptr, AMmapPutObject(doc_state->doc, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST), \
                        cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));                                         \
        AMstackItem(NULL, AMlistPutNull(doc_state->doc, list, 0, !strcmp(#mode, "insert")), cmocka_cb,       \
                    AMexpect(AM_VAL_TYPE_VOID));                                                             \
        AMresult* result = AMstackResult(stack_ptr, AMlistGet(doc_state->doc, list, 0, NULL), NULL, NULL);   \
        if (AMresultStatus(result) != AM_STATUS_OK) {                                                        \
            fail_msg_view("%s", AMresultError(result));                                                      \
        }                                                                                                    \
        assert_int_equal(AMresultSize(result), 1);                                                           \
        AMitem const* const item = AMresultItem(result);                                                     \
        assert_int_equal(AMitemIdxType(item), AM_IDX_TYPE_POS);                                              \
        uint64_t pos;                                                                                        \
        assert_true(AMitemPos(item, &pos));                                                                  \
        assert_int_equal(pos, 0);                                                                            \
        assert_int_equal(AMitemValType(item), AM_VAL_TYPE_NULL);                                             \
        AMresultFree(AMstackPop(stack_ptr, NULL));                                                           \
    }

#define test_AMlistPutObject(label, mode) test_AMlistPutObject_##label##_##mode

#define static_void_test_AMlistPutObject(label, mode)                                                            \
    static void test_AMlistPutObject_##label##_##mode(void** state) {                                            \
        DocState* doc_state = *state;                                                                            \
        AMstack** stack_ptr = &doc_state->base_state->stack;                                                     \
        AMobjId const* const list = AMitemObjId(                                                                 \
            AMstackItem(stack_ptr, AMmapPutObject(doc_state->doc, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST),     \
                        cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));                                             \
        AMobjType const obj_type = suffix_to_obj_type(#label);                                                   \
        AMitem const* const item =                                                                               \
            AMstackItem(stack_ptr, AMlistPutObject(doc_state->doc, list, 0, !strcmp(#mode, "insert"), obj_type), \
                        cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE));                                              \
        assert_int_equal(AMitemIdxType(item), AM_IDX_TYPE_POS);                                                  \
        uint64_t pos;                                                                                            \
        assert_true(AMitemPos(item, &pos));                                                                      \
        assert_int_equal(pos, 0);                                                                                \
        AMobjId const* const obj_id = AMitemObjId(item);                                                         \
        assert_non_null(obj_id);                                                                                 \
        assert_int_equal(AMobjObjType(doc_state->doc, obj_id), obj_type);                                        \
        assert_int_equal(AMobjSize(doc_state->doc, obj_id, NULL), 0);                                            \
        AMresultFree(AMstackPop(stack_ptr, NULL));                                                               \
    }

#define test_AMlistPutStr(mode) test_AMlistPutStr##_##mode

#define static_void_test_AMlistPutStr(mode, str_value)                                                              \
    static void test_AMlistPutStr_##mode(void** state) {                                                            \
        DocState* doc_state = *state;                                                                               \
        AMstack** stack_ptr = &doc_state->base_state->stack;                                                        \
        AMobjId const* const list = AMitemObjId(                                                                    \
            AMstackItem(stack_ptr, AMmapPutObject(doc_state->doc, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST),        \
                        cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));                                                \
        AMstackItem(NULL, AMlistPutStr(doc_state->doc, list, 0, !strcmp(#mode, "insert"), AMstr(str_value)),        \
                    cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));                                                         \
        AMitem const* const item =                                                                                  \
            AMstackItem(stack_ptr, AMlistGet(doc_state->doc, list, 0, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)); \
        assert_int_equal(AMitemIdxType(item), AM_IDX_TYPE_POS);                                                     \
        uint64_t pos;                                                                                               \
        assert_true(AMitemPos(item, &pos));                                                                         \
        assert_int_equal(pos, 0);                                                                                   \
        AMbyteSpan str;                                                                                             \
        assert_true(AMitemToStr(item, &str));                                                                       \
        assert_int_equal(str.count, strlen(str_value));                                                             \
        assert_memory_equal(str.src, str_value, str.count);                                                         \
        AMresultFree(AMstackPop(stack_ptr, NULL));                                                                  \
    }

static_void_test_AMlistPut(Bool, insert, bool, true);

static_void_test_AMlistPut(Bool, update, bool, true);

static uint8_t const BYTES_VALUE[] = {INT8_MIN, INT8_MAX / 2, INT8_MAX};

static_void_test_AMlistPutBytes(insert, BYTES_VALUE);

static_void_test_AMlistPutBytes(update, BYTES_VALUE);

static_void_test_AMlistPut(Counter, insert, int64_t, INT64_MAX);

static_void_test_AMlistPut(Counter, update, int64_t, INT64_MAX);

static_void_test_AMlistPut(F64, insert, double, DBL_MAX);

static_void_test_AMlistPut(F64, update, double, DBL_MAX);

static_void_test_AMlistPut(Int, insert, int64_t, INT64_MAX);

static_void_test_AMlistPut(Int, update, int64_t, INT64_MAX);

static_void_test_AMlistPutNull(insert);

static_void_test_AMlistPutNull(update);

static_void_test_AMlistPutObject(List, insert);

static_void_test_AMlistPutObject(List, update);

static_void_test_AMlistPutObject(Map, insert);

static_void_test_AMlistPutObject(Map, update);

static_void_test_AMlistPutObject(Text, insert);

static_void_test_AMlistPutObject(Text, update);

static_void_test_AMlistPutStr(insert,
                              "Hello, "
                              "world!");

static_void_test_AMlistPutStr(update,
                              "Hello,"
                              " world"
                              "!");

static_void_test_AMlistPut(Timestamp, insert, int64_t, INT64_MAX);

static_void_test_AMlistPut(Timestamp, update, int64_t, INT64_MAX);

static_void_test_AMlistPut(Uint, insert, uint64_t, UINT64_MAX);

static_void_test_AMlistPut(Uint, update, uint64_t, UINT64_MAX);

static void test_get_range_values(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    AMdoc* doc1;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc1));
    AMobjId const* const list =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc1, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));

    /* Insert elements. */
    AMstackItem(NULL, AMlistPutStr(doc1, list, 0, true, AMstr("First")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMlistPutStr(doc1, list, 0, true, AMstr("Second")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMlistPutStr(doc1, list, 0, true, AMstr("Third")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMlistPutStr(doc1, list, 0, true, AMstr("Fourth")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMlistPutStr(doc1, list, 0, true, AMstr("Fifth")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMlistPutStr(doc1, list, 0, true, AMstr("Sixth")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMlistPutStr(doc1, list, 0, true, AMstr("Seventh")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMlistPutStr(doc1, list, 0, true, AMstr("Eighth")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(doc1, AMstr(NULL), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    AMitems const v1 = AMstackItems(stack_ptr, AMgetHeads(doc1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMdoc* doc2;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMfork(doc1, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc2));

    AMstackItem(NULL, AMlistPutStr(doc1, list, 2, false, AMstr("Third V2")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(doc1, AMstr(NULL), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    AMstackItem(NULL, AMlistPutStr(doc2, list, 2, false, AMstr("Third V3")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(doc2, AMstr(NULL), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    AMstackItem(NULL, AMmerge(doc1, doc2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    /* Forward vs. reverse: complete current list range. */
    AMitems range =
        AMstackItems(stack_ptr, AMlistRange(doc1, list, 0, SIZE_MAX, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    size_t size = AMitemsSize(&range);
    assert_int_equal(size, 8);
    AMitems range_back = AMitemsReversed(&range);
    assert_int_equal(AMitemsSize(&range_back), size);
    size_t pos;
    assert_true(AMitemPos(AMitemsNext(&range, 1), &pos));
    assert_int_equal(pos, 0);
    assert_true(AMitemPos(AMitemsNext(&range_back, 1), &pos));
    assert_int_equal(pos, 7);

    AMitem *item1, *item_back1;
    size_t count, middle = size / 2;
    range = AMitemsRewound(&range);
    range_back = AMitemsRewound(&range_back);
    for (item1 = NULL, item_back1 = NULL, count = 0; item1 && item_back1;
         item1 = AMitemsNext(&range, 1), item_back1 = AMitemsNext(&range_back, 1), ++count) {
        size_t pos1, pos_back1;
        assert_true(AMitemPos(item1, &pos1));
        assert_true(AMitemPos(item_back1, &pos_back1));
        if ((count == middle) && (middle & 1)) {
            /* The iterators are crossing in the middle. */
            assert_int_equal(pos1, pos_back1);
            assert_true(AMitemEqual(item1, item_back1));
            assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item_back1)));
        } else {
            assert_int_not_equal(pos1, pos_back1);
        }
        AMitem* item2 = AMstackItem(stack_ptr, AMlistGet(doc1, list, pos1, NULL), NULL, NULL);
        AMitem* item_back2 = AMstackItem(stack_ptr, AMlistGet(doc1, list, pos_back1, NULL), NULL, NULL);
        /** \note An item returned from an `AM...Get()` call doesn't include the
                  index used to retrieve it. */
        assert_false(AMitemIdxType(item2));
        assert_false(AMitemIdxType(item_back2));
        assert_true(AMitemEqual(item1, item2));
        assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item2)));
        assert_true(AMitemEqual(item_back1, item_back2));
        assert_true(AMobjIdEqual(AMitemObjId(item_back1), AMitemObjId(item_back2)));
        AMresultFree(AMstackPop(stack_ptr, NULL));
    }

    /* Forward vs. reverse: partial current list range. */
    range = AMstackItems(stack_ptr, AMlistRange(doc1, list, 1, 6, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    size = AMitemsSize(&range);
    assert_int_equal(size, 5);
    range_back = AMitemsReversed(&range);
    assert_int_equal(AMitemsSize(&range_back), size);
    assert_true(AMitemPos(AMitemsNext(&range, 1), &pos));
    assert_int_equal(pos, 1);
    assert_true(AMitemPos(AMitemsNext(&range_back, 1), &pos));
    assert_int_equal(pos, 5);

    middle = size / 2;
    range = AMitemsRewound(&range);
    range_back = AMitemsRewound(&range_back);
    for (item1 = NULL, item_back1 = NULL, count = 0; item1 && item_back1;
         item1 = AMitemsNext(&range, 1), item_back1 = AMitemsNext(&range_back, 1), ++count) {
        size_t pos1, pos_back1;
        assert_true(AMitemPos(item1, &pos1));
        assert_true(AMitemPos(item_back1, &pos_back1));
        if ((count == middle) && (middle & 1)) {
            /* The iterators are crossing in the middle. */
            assert_int_equal(pos1, pos_back1);
            assert_true(AMitemEqual(item1, item_back1));
            assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item_back1)));
        } else {
            assert_int_not_equal(pos1, pos_back1);
        }
        AMitem* item2 = AMstackItem(stack_ptr, AMlistGet(doc1, list, pos1, NULL), NULL, NULL);
        AMitem* item_back2 = AMstackItem(stack_ptr, AMlistGet(doc1, list, pos_back1, NULL), NULL, NULL);
        /** \note An item returned from an `AMlistGet()` call doesn't include
                  the index used to retrieve it. */
        assert_int_equal(AMitemIdxType(item2), 0);
        assert_int_equal(AMitemIdxType(item_back2), 0);
        assert_true(AMitemEqual(item1, item2));
        assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item2)));
        assert_true(AMitemEqual(item_back1, item_back2));
        assert_true(AMobjIdEqual(AMitemObjId(item_back1), AMitemObjId(item_back2)));
        AMresultFree(AMstackPop(stack_ptr, NULL));
    }

    /* Forward vs. reverse: complete historical map range. */
    range = AMstackItems(stack_ptr, AMlistRange(doc1, list, 0, SIZE_MAX, &v1), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    size = AMitemsSize(&range);
    assert_int_equal(size, 8);
    range_back = AMitemsReversed(&range);
    assert_int_equal(AMitemsSize(&range_back), size);
    assert_true(AMitemPos(AMitemsNext(&range, 1), &pos));
    assert_int_equal(pos, 0);
    assert_true(AMitemPos(AMitemsNext(&range_back, 1), &pos));
    assert_int_equal(pos, 7);

    middle = size / 2;
    range = AMitemsRewound(&range);
    range_back = AMitemsRewound(&range_back);
    for (item1 = NULL, item_back1 = NULL, count = 0; item1 && item_back1;
         item1 = AMitemsNext(&range, 1), item_back1 = AMitemsNext(&range_back, 1), ++count) {
        size_t pos1, pos_back1;
        assert_true(AMitemPos(item1, &pos1));
        assert_true(AMitemPos(item_back1, &pos_back1));
        if ((count == middle) && (middle & 1)) {
            /* The iterators are crossing in the middle. */
            assert_int_equal(pos1, pos_back1);
            assert_true(AMitemEqual(item1, item_back1));
            assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item_back1)));
        } else {
            assert_int_not_equal(pos1, pos_back1);
        }
        AMitem* item2 = AMstackItem(stack_ptr, AMlistGet(doc1, list, pos1, &v1), NULL, NULL);
        AMitem* item_back2 = AMstackItem(stack_ptr, AMlistGet(doc1, list, pos_back1, &v1), NULL, NULL);
        /** \note An item returned from an `AM...Get()` call doesn't include the
                  index used to retrieve it. */
        assert_false(AMitemIdxType(item2));
        assert_false(AMitemIdxType(item_back2));
        assert_true(AMitemEqual(item1, item2));
        assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item2)));
        assert_true(AMitemEqual(item_back1, item_back2));
        assert_true(AMobjIdEqual(AMitemObjId(item_back1), AMitemObjId(item_back2)));
        AMresultFree(AMstackPop(stack_ptr, NULL));
    }

    /* Forward vs. reverse: partial historical map range. */
    range = AMstackItems(stack_ptr, AMlistRange(doc1, list, 2, 7, &v1), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    size = AMitemsSize(&range);
    assert_int_equal(size, 5);
    range_back = AMitemsReversed(&range);
    assert_int_equal(AMitemsSize(&range_back), size);
    assert_true(AMitemPos(AMitemsNext(&range, 1), &pos));
    assert_int_equal(pos, 2);
    assert_true(AMitemPos(AMitemsNext(&range_back, 1), &pos));
    assert_int_equal(pos, 6);

    middle = size / 2;
    range = AMitemsRewound(&range);
    range_back = AMitemsRewound(&range_back);
    for (item1 = NULL, item_back1 = NULL, count = 0; item1 && item_back1;
         item1 = AMitemsNext(&range, 1), item_back1 = AMitemsNext(&range_back, 1), ++count) {
        size_t pos1, pos_back1;
        assert_true(AMitemPos(item1, &pos1));
        assert_true(AMitemPos(item_back1, &pos_back1));
        if ((count == middle) && (middle & 1)) {
            /* The iterators are crossing in the middle. */
            assert_int_equal(pos1, pos_back1);
            assert_true(AMitemEqual(item1, item_back1));
            assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item_back1)));
        } else {
            assert_int_not_equal(pos1, pos_back1);
        }
        AMitem* item2 = AMstackItem(stack_ptr, AMlistGet(doc1, list, pos1, &v1), NULL, NULL);
        AMitem* item_back2 = AMstackItem(stack_ptr, AMlistGet(doc1, list, pos_back1, &v1), NULL, NULL);
        /** \note An item returned from an `AM...Get()` call doesn't include the
                  index used to retrieve it. */
        assert_false(AMitemIdxType(item2));
        assert_false(AMitemIdxType(item_back2));
        assert_true(AMitemEqual(item1, item2));
        assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item2)));
        assert_true(AMitemEqual(item_back1, item_back2));
        assert_true(AMobjIdEqual(AMitemObjId(item_back1), AMitemObjId(item_back2)));
        AMresultFree(AMstackPop(stack_ptr, NULL));
    }

    /* List range vs. object range: complete current. */
    range = AMstackItems(stack_ptr, AMlistRange(doc1, list, 0, SIZE_MAX, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    AMitems obj_items = AMstackItems(stack_ptr, AMobjItems(doc1, list, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_int_equal(AMitemsSize(&range), AMitemsSize(&obj_items));

    AMitem *item, *obj_item;
    for (item = NULL, obj_item = NULL; item && obj_item;
         item = AMitemsNext(&range, 1), obj_item = AMitemsNext(&obj_items, 1)) {
        /** \note Object iteration doesn't yield any item indices. */
        assert_true(AMitemIdxType(item));
        assert_false(AMitemIdxType(obj_item));
        assert_true(AMitemEqual(item, obj_item));
        assert_true(AMobjIdEqual(AMitemObjId(item), AMitemObjId(obj_item)));
    }

    /* List range vs. object range: complete historical. */
    range = AMstackItems(stack_ptr, AMlistRange(doc1, list, 0, SIZE_MAX, &v1), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    obj_items = AMstackItems(stack_ptr, AMobjItems(doc1, list, &v1), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_int_equal(AMitemsSize(&range), AMitemsSize(&obj_items));

    for (item = NULL, obj_item = NULL; item && obj_item;
         item = AMitemsNext(&range, 1), obj_item = AMitemsNext(&obj_items, 1)) {
        /** \note Object iteration doesn't yield any item indices. */
        assert_true(AMitemIdxType(item));
        assert_false(AMitemIdxType(obj_item));
        assert_true(AMitemEqual(item, obj_item));
        assert_true(AMobjIdEqual(AMitemObjId(item), AMitemObjId(obj_item)));
    }
}

/**
 * \brief A JavaScript application can introduce NUL (`\0`) characters into a
 *        list object's string value which will truncate it in a C application.
 */
static void test_get_NUL_string_value(void** state) {
    /*
    import * as Automerge from "@automerge/automerge";
    let doc = Automerge.init();
    doc = Automerge.change(doc, doc => {
        doc.list = [new Automerge.ImmutableString("o\0ps")];
    });
    const bytes = Automerge.save(doc);
    console.log("static uint8_t const SAVED_DOC[] = {" + Array.apply([],
    bytes).join(", ") + "};");
    */
    static uint8_t const OOPS_VALUE[] = {'o', '\0', 'p', 's'};
    static size_t const OOPS_SIZE = sizeof(OOPS_VALUE) / sizeof(uint8_t);

    static uint8_t const SAVED_DOC[] = {
        133, 111, 74, 131, 234, 84,  17,  185, 0,   143, 1,   1,   16,  210, 154, 229, 127, 245, 12,  121, 118, 197,
        1,   61,  77, 57,  197, 134, 224, 1,   137, 202, 7,   230, 81,  66,  74,  151, 240, 74,  118, 112, 116, 63,
        87,  118, 0,  50,  14,  111, 33,  70,  123, 152, 248, 67,  235, 239, 164, 41,  255, 42,  6,   1,   2,   3,
        2,   19,  2,  35,  6,   64,  2,   86,  2,   11,  1,   4,   2,   4,   19,  4,   21,  8,   33,  2,   35,  2,
        52,  2,   66, 3,   86,  3,   87,  4,   128, 1,   2,   127, 0,   127, 1,   127, 2,   127, 159, 212, 158, 202,
        6,   127, 0,  127, 7,   0,   1,   127, 0,   0,   1,   127, 1,   0,   1,   127, 0,   127, 4,   108, 105, 115,
        116, 0,   1,  2,   0,   2,   1,   1,   1,   126, 2,   1,   126, 0,   70,  111, 0,   112, 115, 2,   0,   0};
    static size_t const SAVED_DOC_SIZE = sizeof(SAVED_DOC) / sizeof(uint8_t);

    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    AMdoc* doc;
    assert_true(AMitemToDoc(
        AMstackItem(stack_ptr, AMload(SAVED_DOC, SAVED_DOC_SIZE), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    AMobjId const* const list = AMitemObjId(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("list"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    AMbyteSpan str;
    assert_true(
        AMitemToStr(AMstackItem(stack_ptr, AMlistGet(doc, list, 0, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_not_equal(str.count, strlen(OOPS_VALUE));
    assert_int_equal(str.count, OOPS_SIZE);
    assert_memory_equal(str.src, OOPS_VALUE, str.count);
}

static void test_insert_at_index(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    AMobjId const* const list =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* Insert both at the same index. */
    AMstackItem(NULL, AMlistPutUint(doc, list, 0, true, 0), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMlistPutUint(doc, list, 0, true, 1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    assert_int_equal(AMobjSize(doc, list, NULL), 2);
    AMitems const keys = AMstackItems(stack_ptr, AMkeys(doc, list, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_int_equal(AMitemsSize(&keys), 2);
    AMitems const range =
        AMstackItems(stack_ptr, AMlistRange(doc, list, 0, SIZE_MAX, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_UINT));
    assert_int_equal(AMitemsSize(&range), 2);
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
        cmocka_unit_test_setup_teardown(test_get_range_values, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_get_NUL_string_value, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_insert_at_index, setup_base, teardown_base),
    };

    return cmocka_run_group_tests(tests, setup_doc, teardown_doc);
}
