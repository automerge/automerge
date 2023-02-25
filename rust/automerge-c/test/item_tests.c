#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
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

int run_item_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMitemResult),
    };

    return cmocka_run_group_tests(tests, setup_doc, teardown_doc);
}
