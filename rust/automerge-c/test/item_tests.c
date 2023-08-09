#include <float.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

/* third-party */
#include <cmocka.h>

/* local */
#include <automerge-c/automerge.h>
#include <automerge-c/utils/stack_callback_data.h>
#include "cmocka_utils.h"
#include "doc_state.h"

static void test_AMitemResult(void** state) {
    enum { ITEM_COUNT = 1000 };

    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;
    /* Append the strings to a list so that they'll be in numerical order. */
    AMobjId const* const list =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc_state->doc, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST),
                                cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    for (size_t pos = 0; pos != ITEM_COUNT; ++pos) {
        size_t const count = snprintf(NULL, 0, "%zu", pos);
        char* const src = test_calloc(count + 1, sizeof(char));
        assert_int_equal(sprintf(src, "%zu", pos), count);
        AMstackItem(NULL, AMlistPutStr(doc_state->doc, list, pos, true, AMbytes(src, count)), cmocka_cb,
                    AMexpect(AM_VAL_TYPE_VOID));
        test_free(src);
    }
    /* Get an item iterator. */
    AMitems items = AMstackItems(stack_ptr, AMlistRange(doc_state->doc, list, 0, SIZE_MAX, NULL), cmocka_cb,
                                 AMexpect(AM_VAL_TYPE_STR));
    /* Get the item iterator's result so that it can be freed later. */
    AMresult const* const items_result = (*stack_ptr)->result;
    /* Iterate over all of the items and copy their pointers into an array. */
    AMitem* item_ptrs[ITEM_COUNT] = {NULL};
    AMitem* item = NULL;
    for (size_t pos = 0; (item = AMitemsNext(&items, 1)) != NULL; ++pos) {
        /* The item's reference count should be 1. */
        assert_int_equal(AMitemRefCount(item), 1);
        if (pos & 1) {
            /* Create a redundant result for an odd item. */
            AMitem* const new_item = AMstackItem(stack_ptr, AMitemResult(item), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
            /* The item's old and new pointers will never match. */
            assert_ptr_not_equal(new_item, item);
            /* The item's reference count will have been incremented. */
            assert_int_equal(AMitemRefCount(item), 2);
            assert_int_equal(AMitemRefCount(new_item), 2);
            /* The item's old and new indices should match. */
            assert_int_equal(AMitemIdxType(item), AMitemIdxType(new_item));
            assert_int_equal(AMitemIdxType(item), AM_IDX_TYPE_POS);
            size_t pos, new_pos;
            assert_true(AMitemPos(item, &pos));
            assert_true(AMitemPos(new_item, &new_pos));
            assert_int_equal(pos, new_pos);
            /* The item's old and new object IDs should match. */
            AMobjId const* const obj_id = AMitemObjId(item);
            AMobjId const* const new_obj_id = AMitemObjId(new_item);
            assert_true(AMobjIdEqual(obj_id, new_obj_id));
            /* The item's old and new value types should match. */
            assert_int_equal(AMitemValType(item), AMitemValType(new_item));
            /* The item's old and new string values should match. */
            AMbyteSpan str;
            assert_true(AMitemToStr(item, &str));
            AMbyteSpan new_str;
            assert_true(AMitemToStr(new_item, &new_str));
            assert_int_equal(str.count, new_str.count);
            assert_memory_equal(str.src, new_str.src, new_str.count);
            /* The item's old and new object IDs are one and the same. */
            assert_ptr_equal(obj_id, new_obj_id);
            /* The item's old and new string values are one and the same. */
            assert_ptr_equal(str.src, new_str.src);
            /* Save the item's new pointer. */
            item_ptrs[pos] = new_item;
        }
    }
    /* Free the item iterator's result. */
    AMresultFree(AMstackPop(stack_ptr, items_result));
    /* An odd item's reference count should be 1 again. */
    for (size_t pos = 1; pos < ITEM_COUNT; pos += 2) {
        assert_int_equal(AMitemRefCount(item_ptrs[pos]), 1);
    }
}

static void test_AMitemToActorId_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMbyteSpan const str = AMstr("000102030405060708090a0b0c0d0e0f");
    AMitem* item = AMstackItem(stack_ptr, AMactorIdFromStr(str), NULL, NULL);
    assert_non_null(item);
    assert_false(AMitemToActorId(item, NULL));
}

static void test_AMitemToBool_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMstackItem(NULL, AMmapPutBool(doc_state->doc, AM_ROOT, AMstr("bool"), true), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMitem* const item = AMstackItem(stack_ptr, AMmapGet(doc_state->doc, AM_ROOT, AMstr("bool"), NULL), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_BOOL));
    assert_non_null(item);
    assert_false(AMitemToBool(item, NULL));
}

static void test_AMitemToBytes_null_out_arg(void** state) {
    static uint8_t const BYTES_VALUE[] = {INT8_MIN, INT8_MAX / 2, INT8_MAX};

    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMstackItem(NULL,
                AMmapPutBytes(doc_state->doc, AM_ROOT, AMstr("bytes"),
                              AMbytes(BYTES_VALUE, sizeof(BYTES_VALUE) / sizeof(uint8_t))),
                cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMitem* const item = AMstackItem(stack_ptr, AMmapGet(doc_state->doc, AM_ROOT, AMstr("bytes"), NULL), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_BYTES));
    assert_non_null(item);
    assert_false(AMitemToBytes(item, NULL));
}

static void test_AMitemToChange_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMstackItem(NULL, AMmapPutBool(doc_state->doc, AM_ROOT, AMstr("bool"), true), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMitem* const item =
        AMstackItem(stack_ptr, AMgetChanges(doc_state->doc, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE));
    assert_non_null(item);
    assert_false(AMitemToChange(item, NULL));
}

static void test_AMitemToChangeHash_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMstackItem(NULL, AMmapPutBool(doc_state->doc, AM_ROOT, AMstr("bool"), true), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMitem* const item =
        AMstackItem(stack_ptr, AMgetHeads(doc_state->doc), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_non_null(item);
    assert_false(AMitemToChangeHash(item, NULL));
}

static void test_AMitemToCounter_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMstackItem(NULL, AMmapPutCounter(doc_state->doc, AM_ROOT, AMstr("counter"), 3), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMitem* const item = AMstackItem(stack_ptr, AMmapGet(doc_state->doc, AM_ROOT, AMstr("counter"), NULL), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_COUNTER));
    assert_non_null(item);
    assert_false(AMitemToCounter(item, NULL));
}

static void test_AMitemToDoc_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMitem* const item = AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC));
    assert_non_null(item);
    assert_false(AMitemToDoc(item, NULL));
}

static void test_AMitemToF64_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMstackItem(NULL, AMmapPutF64(doc_state->doc, AM_ROOT, AMstr("f64"), DBL_MAX), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMitem* const item = AMstackItem(stack_ptr, AMmapGet(doc_state->doc, AM_ROOT, AMstr("f64"), NULL), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_F64));
    assert_non_null(item);
    assert_false(AMitemToF64(item, NULL));
}

static void test_AMitemToInt_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMstackItem(NULL, AMmapPutInt(doc_state->doc, AM_ROOT, AMstr("int"), INT64_MAX), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMitem* const item = AMstackItem(stack_ptr, AMmapGet(doc_state->doc, AM_ROOT, AMstr("int"), NULL), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_INT));
    assert_non_null(item);
    assert_false(AMitemToInt(item, NULL));
}

static void test_AMitemToMark_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMobjId const* const text =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc_state->doc, AM_ROOT, AMstr("text"), AM_OBJ_TYPE_TEXT),
                                cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    AMstackItem(stack_ptr, AMspliceText(doc_state->doc, text, 0, 0, AMstr("hello world")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(stack_ptr,
                AMmarkCreate(doc_state->doc, text, 0, 5, AM_MARK_EXPAND_BOTH, AMstr("bold"),
                             AMstackItem(stack_ptr, AMitemFromBool(true), cmocka_cb, AMexpect(AM_VAL_TYPE_BOOL))),
                cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMitem* const item =
        AMstackItem(stack_ptr, AMmarks(doc_state->doc, text, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_MARK));
    assert_non_null(item);
    assert_false(AMitemToMark(item, NULL));
}

static void test_AMitemToStr_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMstackItem(NULL, AMmapPutStr(doc_state->doc, AM_ROOT, AMstr("str"), AMstr("Hello, world!")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMitem* const item = AMstackItem(stack_ptr, AMmapGet(doc_state->doc, AM_ROOT, AMstr("str"), NULL), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_STR));
    assert_non_null(item);
    assert_false(AMitemToStr(item, NULL));
}

static void test_AMitemToSyncHave_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMsyncState* sync_state;
    assert_true(AMitemToSyncState(
        AMstackItem(stack_ptr, AMsyncStateInit(), cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_STATE)), &sync_state));
    AMsyncMessage const* sync_message;
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(doc_state->doc, sync_state), cmocka_cb,
                                                AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &sync_message));
    AMitem* const item =
        AMstackItem(stack_ptr, AMsyncMessageHaves(sync_message), cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_HAVE));
    assert_non_null(item);
    assert_false(AMitemToSyncHave(item, NULL));
}

static void test_AMitemToSyncMessage_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMsyncState* sync_state;
    assert_true(AMitemToSyncState(
        AMstackItem(stack_ptr, AMsyncStateInit(), cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_STATE)), &sync_state));
    AMitem* const item = AMstackItem(stack_ptr, AMgenerateSyncMessage(doc_state->doc, sync_state), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_SYNC_MESSAGE));
    assert_non_null(item);
    assert_false(AMitemToSyncMessage(item, NULL));
}

static void test_AMitemToSyncState_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMitem* const item = AMstackItem(stack_ptr, AMsyncStateInit(), cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_STATE));
    assert_non_null(item);
    assert_false(AMitemToSyncState(item, NULL));
}

static void test_AMitemToTimestamp_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMstackItem(NULL, AMmapPutTimestamp(doc_state->doc, AM_ROOT, AMstr("timestamp"), 1000), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMitem* const item = AMstackItem(stack_ptr, AMmapGet(doc_state->doc, AM_ROOT, AMstr("timestamp"), NULL), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_TIMESTAMP));
    assert_non_null(item);
    assert_false(AMitemToTimestamp(item, NULL));
}

static void test_AMitemToUint_null_out_arg(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMstackItem(NULL, AMmapPutUint(doc_state->doc, AM_ROOT, AMstr("uint"), UINT64_MAX), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMitem* const item = AMstackItem(stack_ptr, AMmapGet(doc_state->doc, AM_ROOT, AMstr("uint"), NULL), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_UINT));
    assert_non_null(item);
    assert_false(AMitemToUint(item, NULL));
}

int run_item_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMitemResult),
        cmocka_unit_test(test_AMitemToActorId_null_out_arg),
        cmocka_unit_test(test_AMitemToBool_null_out_arg),
        cmocka_unit_test(test_AMitemToBytes_null_out_arg),
        cmocka_unit_test(test_AMitemToChange_null_out_arg),
        cmocka_unit_test(test_AMitemToChangeHash_null_out_arg),
        cmocka_unit_test(test_AMitemToCounter_null_out_arg),
        cmocka_unit_test(test_AMitemToDoc_null_out_arg),
        cmocka_unit_test(test_AMitemToF64_null_out_arg),
        cmocka_unit_test(test_AMitemToInt_null_out_arg),
        cmocka_unit_test(test_AMitemToMark_null_out_arg),
        cmocka_unit_test(test_AMitemToStr_null_out_arg),
        cmocka_unit_test(test_AMitemToSyncHave_null_out_arg),
        cmocka_unit_test(test_AMitemToSyncMessage_null_out_arg),
        cmocka_unit_test(test_AMitemToSyncState_null_out_arg),
        cmocka_unit_test(test_AMitemToTimestamp_null_out_arg),
        cmocka_unit_test(test_AMitemToUint_null_out_arg),
    };

    return cmocka_run_group_tests(tests, setup_doc, teardown_doc);
}
