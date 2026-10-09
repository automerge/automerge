#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* third-party */
#include <cmocka.h>

/* local */
#include <automerge-c/automerge.h>
#include <automerge-c/utils/stack_callback_data.h>
#include "../base_state.h"
#include "../cmocka_utils.h"

/**
 * \brief should be able to make a cursor from a position in a text document, then use it
 */
static void test_make_cursor_from_position_and_use_it(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* let doc1 = create();                                                                                           */
    AMdoc* doc1;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc1));
    /* doc1.putObject("/", "text", "the sly fox jumped over the lazy dog");                                           */
    AMobjId const* const text =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc1, AM_ROOT, AMstr("text"), AM_OBJ_TYPE_TEXT), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    AMstackItem(NULL, AMspliceText(doc1, text, 0, 0, AMstr("the sly fox jumped over the lazy dog")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* let heads1 = doc1.getHeads();                                                                                  */
    AMitems const heads1 = AMstackItems(stack_ptr, AMgetHeads(doc1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /*
       // get a cursor at a position
       let cursor = doc1.getCursor("/text", 12);                                                                      */
    AMcursor const* cursor;
    assert_true(AMitemToCursor(
        AMstackItem(stack_ptr, AMgetCursor(doc1, text, 12, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CURSOR)), &cursor));
    /* let index1 = doc1.getCursorPosition("/text", cursor);                                                          */
    size_t index1;
    assert_true(AMitemToUint(
        AMstackItem(stack_ptr, AMgetCursorPosition(doc1, text, cursor, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_UINT)),
        &index1));
    /* assert.deepStrictEqual(index1, 12);                                                                            */
    assert_int_equal(index1, 12);
    /*
       // modifying the text changes the cursor position
       doc1.splice("/text",0,3,"Has the");                                                                            */
    AMstackItem(NULL, AMspliceText(doc1, text, 0, 3, AMstr("Has the")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepStrictEqual(doc1.text("/text"), "Has the sly fox jumped over the lazy dog");                        */
    AMbyteSpan str;
    assert_true(
        AMitemToStr(AMstackItem(stack_ptr, AMtext(doc1, text, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, strlen("Has the sly fox jumped over the lazy dog"));
    assert_memory_equal(str.src, "Has the sly fox jumped over the lazy dog", str.count);
    /* let index2 = doc1.getCursorPosition("/text", cursor);                                                          */
    size_t index2;
    assert_true(AMitemToUint(
        AMstackItem(stack_ptr, AMgetCursorPosition(doc1, text, cursor, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_UINT)),
        &index2));
    /* assert.deepStrictEqual(index2, 16);                                                                            */
    assert_int_equal(index2, 16);
    /*
       // get the cursor position at heads
       let index3 = doc1.getCursorPosition("/text", cursor, heads1);                                                  */
    size_t index3;
    assert_true(AMitemToUint(
        AMstackItem(stack_ptr, AMgetCursorPosition(doc1, text, cursor, &heads1), cmocka_cb, AMexpect(AM_VAL_TYPE_UINT)),
        &index3));
    /* assert.deepStrictEqual(index1, index3);                                                                        */
    assert_int_equal(index1, index3);
    /*
       // get a cursor at heads
       let cursor2 = doc1.getCursor("/text", 12, heads1);                                                             */
    AMcursor const* cursor2;
    assert_true(AMitemToCursor(
        AMstackItem(stack_ptr, AMgetCursor(doc1, text, 12, &heads1), cmocka_cb, AMexpect(AM_VAL_TYPE_CURSOR)),
        &cursor2));
    /* let cursor3 = doc1.getCursor("/text", 16);                                                                     */
    AMcursor const* cursor3;
    assert_true(AMitemToCursor(
        AMstackItem(stack_ptr, AMgetCursor(doc1, text, 16, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CURSOR)), &cursor3));
    /* assert.deepStrictEqual(cursor, cursor2);                                                                       */
    assert_true(AMcursorEqual(cursor, cursor2));
    /* assert.deepStrictEqual(cursor, cursor3);                                                                       */
    assert_true(AMcursorEqual(cursor, cursor3));
    /*
       // cursor works at the heads
       let cursor4 = doc1.getCursor("/text", 0);                                                                      */
    AMcursor const* cursor4;
    assert_true(AMitemToCursor(
        AMstackItem(stack_ptr, AMgetCursor(doc1, text, 0, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CURSOR)), &cursor4));
    /* let index4 = doc1.getCursorPosition("/text", cursor4);                                                         */
    size_t index4;
    assert_true(AMitemToUint(
        AMstackItem(stack_ptr, AMgetCursorPosition(doc1, text, cursor4, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_UINT)),
        &index4));
    /* assert.deepStrictEqual(index4, 0);                                                                             */
    assert_int_equal(index4, 0);
}

int run_ported_wasm_cursor_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_make_cursor_from_position_and_use_it, setup_base, teardown_base)};

    return cmocka_run_group_tests(tests, NULL, NULL);
}
