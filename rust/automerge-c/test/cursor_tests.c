#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

/* third-party */
#include <cmocka.h>

/* local */
#include <automerge-c/automerge.h>
#include <automerge-c/utils/stack_callback_data.h>
#include "cmocka_utils.h"
#include "doc_state.h"

typedef struct {
    DocState* doc_state;
    AMobjId const* text;
} TestState;

static int setup(void** state) {
    /* let mut doc = Automerge::new();                                                                                */
    TestState* test_state = test_calloc(1, sizeof(TestState));
    setup_doc((void**)&test_state->doc_state);
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    /* let mut tx = doc.transaction();
       let text = tx.put_object(ROOT, "text", ObjType::Text)?;                                                        */
    test_state->text = AMitemObjId(
        AMstackItem(stack_ptr, AMmapPutObject(test_state->doc_state->doc, AM_ROOT, AMstr("text"), AM_OBJ_TYPE_TEXT),
                    cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* tx.commit();                                                                                                   */
    AMstackItem(NULL, AMcommit(test_state->doc_state->doc, AMstr(NULL), NULL), cmocka_cb,
                AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* let mut tx = doc.transaction();
       tx.splice_text(&text, 0, 0, "hello world")?;                                                                   */
    AMstackItem(NULL, AMspliceText(test_state->doc_state->doc, test_state->text, 0, 0, AMstr("hello world")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* tx.commit();                                                                                                   */
    AMstackItem(NULL, AMcommit(test_state->doc_state->doc, AMstr(NULL), NULL), cmocka_cb,
                AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* let mut tx = doc.transaction();
       tx.splice_text(&text, 6, 0, "big bad ")?;                                                                      */
    AMstackItem(NULL, AMspliceText(test_state->doc_state->doc, test_state->text, 6, 0, AMstr("big bad ")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* tx.commit();                                                                                                   */
    AMstackItem(NULL, AMcommit(test_state->doc_state->doc, AMstr(NULL), NULL), cmocka_cb,
                AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    *state = test_state;
    return 0;
}

static int teardown(void** state) {
    TestState* test_state = *state;
    teardown_doc((void**)&test_state->doc_state);
    test_free(test_state);
    return 0;
}

static void test_AMgetCursor(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    /* // simple cursor test + serialization
       let cursor0 = doc.get_cursor(&text, 0, None).unwrap();                                                         */
    AMcursor const* cursor0;
    assert_true(
        AMitemToCursor(AMstackItem(stack_ptr, AMgetCursor(test_state->doc_state->doc, test_state->text, 0, NULL),
                                   cmocka_cb, AMexpect(AM_VAL_TYPE_CURSOR)),
                       &cursor0));
    /* let cursor0_str = cursor0.to_string();                                                                         */
    AMbyteSpan const cursor0_str = AMcursorStr(cursor0);
    assert_int_not_equal(cursor0_str.count, 0);
    assert_non_null(cursor0_str.src);
    /* let cursor0_bytes = cursor0.to_bytes();                                                                        */
    AMbyteSpan const cursor0_bytes = AMcursorBytes(cursor0);
    assert_int_not_equal(cursor0_bytes.count, 0);
    assert_non_null(cursor0_bytes.src);
    /* let pos0 = doc.get_cursor_position(&text, &cursor0, None).unwrap();                                            */
    size_t pos0;
    assert_true(AMitemToUint(
        AMstackItem(stack_ptr, AMgetCursorPosition(test_state->doc_state->doc, test_state->text, cursor0, NULL),
                    cmocka_cb, AMexpect(AM_VAL_TYPE_UINT)),
        &pos0));
    /* assert_eq!(pos0, 0);                                                                                           */
    assert_int_equal(pos0, 0);
    /* assert_eq!(Cursor::try_from(cursor0_str).unwrap(), cursor0);                                                   */
    AMcursor const* cursor0_deserialized;
    assert_true(
        AMitemToCursor(AMstackItem(stack_ptr, AMcursorFromStr(cursor0_str), cmocka_cb, AMexpect(AM_VAL_TYPE_CURSOR)),
                       &cursor0_deserialized));
    assert_true(AMcursorEqual(cursor0_deserialized, cursor0));
    /* assert_eq!(Cursor::try_from(cursor0_bytes).unwrap(), cursor0);                                                 */
    assert_true(AMitemToCursor(AMstackItem(stack_ptr, AMcursorFromBytes(cursor0_bytes.src, cursor0_bytes.count),
                                           cmocka_cb, AMexpect(AM_VAL_TYPE_CURSOR)),
                               &cursor0_deserialized));
    assert_true(AMcursorEqual(cursor0_deserialized, cursor0));
}

static void test_AMgetCursorPosition(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    /* // simple cursor test + serialization
       let cursor1 = doc.get_cursor(&text, 6, None).unwrap();                                                         */
    AMcursor const* cursor1;
    assert_true(
        AMitemToCursor(AMstackItem(stack_ptr, AMgetCursor(test_state->doc_state->doc, test_state->text, 6, NULL),
                                   cmocka_cb, AMexpect(AM_VAL_TYPE_CURSOR)),
                       &cursor1));
    /* let cursor1_str = cursor1.to_string();                                                                         */
    AMbyteSpan const cursor1_str = AMcursorStr(cursor1);
    assert_int_not_equal(cursor1_str.count, 0);
    assert_non_null(cursor1_str.src);
    /* let cursor1_bytes = cursor1.to_bytes();                                                                        */
    AMbyteSpan const cursor1_bytes = AMcursorBytes(cursor1);
    assert_int_not_equal(cursor1_bytes.count, 0);
    assert_non_null(cursor1_bytes.src);
    /* let pos1 = doc.get_cursor_position(&text, &cursor1, None).unwrap();                                            */
    size_t pos1;
    assert_true(AMitemToUint(
        AMstackItem(stack_ptr, AMgetCursorPosition(test_state->doc_state->doc, test_state->text, cursor1, NULL),
                    cmocka_cb, AMexpect(AM_VAL_TYPE_UINT)),
        &pos1));
    /* assert_eq!(pos1, 6);                                                                                           */
    assert_int_equal(pos1, 6);
    /* assert_eq!(Cursor::try_from(cursor1_str).unwrap(), cursor1);                                                   */
    AMcursor const* cursor1_deserialized;
    assert_true(
        AMitemToCursor(AMstackItem(stack_ptr, AMcursorFromStr(cursor1_str), cmocka_cb, AMexpect(AM_VAL_TYPE_CURSOR)),
                       &cursor1_deserialized));
    assert_true(AMcursorEqual(cursor1_deserialized, cursor1));
    /* assert_eq!(Cursor::try_from(cursor1_bytes).unwrap(), cursor1);                                                 */
    assert_true(AMitemToCursor(AMstackItem(stack_ptr, AMcursorFromBytes(cursor1_bytes.src, cursor1_bytes.count),
                                           cmocka_cb, AMexpect(AM_VAL_TYPE_CURSOR)),
                               &cursor1_deserialized));
    assert_true(AMcursorEqual(cursor1_deserialized, cursor1));
    /*
       let heads0 = doc.get_heads();                                                                                  */
    AMitems const heads0 =
        AMstackItems(stack_ptr, AMgetHeads(test_state->doc_state->doc), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /*
       let mut tx = doc.transaction();
       tx.splice_text(&text, 3, 6, " new text ")?;                                                                    */
    AMstackItem(NULL, AMspliceText(test_state->doc_state->doc, test_state->text, 3, 6, AMstr(" new text ")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* tx.commit();                                                                                                   */
    AMstackItem(NULL, AMcommit(test_state->doc_state->doc, AMstr(NULL), NULL), cmocka_cb,
                AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /*
       // confirm the cursor changed position after an edit
       let pos2 = doc.get_cursor_position(&text, &cursor1, None).unwrap();                                            */
    size_t pos2;
    assert_true(AMitemToUint(
        AMstackItem(stack_ptr, AMgetCursorPosition(test_state->doc_state->doc, test_state->text, cursor1, NULL),
                    cmocka_cb, AMexpect(AM_VAL_TYPE_UINT)),
        &pos2));
    /* assert_eq !(pos2, 13);  // -3 deleted & +10 inserted before cursor                                             */
    assert_int_equal(pos2, 13);
    /*
       // confirm the cursor can still be read at the old position
       let pos3 = doc.get_cursor_position(&text, &cursor1, Some(&heads0)).unwrap();                                   */
    size_t pos3;
    assert_true(AMitemToUint(
        AMstackItem(stack_ptr, AMgetCursorPosition(test_state->doc_state->doc, test_state->text, cursor1, &heads0),
                    cmocka_cb, AMexpect(AM_VAL_TYPE_UINT)),
        &pos3));
    /* assert_eq !(pos3, 6);  // back to the old heads                                                                */
    assert_int_equal(pos3, 6);
}

static void test_AMcursorFromBytes_failure(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    /* // confirm cursor load errors
       assert_eq!(
        Cursor::try_from(vec![0u8, 3u8, 10u8].as_slice()),
        Err(AutomergeError::InvalidCursorFormat)
    );                                                                                                                */
    uint8_t const BYTES[3] = {0, 3, 10};
    AMresult* result = AMstackResult(stack_ptr, AMcursorFromBytes(BYTES, 3), NULL, NULL);
    if (AMresultStatus(result) == AM_STATUS_OK) {
        fail_msg("`AMcursor` from invalid array of bytes");
    }
}

static void test_AMcursorFromStr_failure(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    /* assert_eq!(
        Cursor::try_from("notacursor"),
        Err(AutomergeError::InvalidCursorFormat)
    );                                                                                                                */
    AMresult* result = AMstackResult(stack_ptr, AMcursorFromStr(AMstr("notacursor")), NULL, NULL);
    if (AMresultStatus(result) == AM_STATUS_OK) {
        fail_msg("`AMcursor` from invalid UTF-8 string");
    }
}

static void test_AMgetCursorPosition_failure(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    /* // confirm behavior of a invalid cursor
       let bad_cursor = Cursor::try_from("10@aabbcc00").unwrap();                                                     */
    AMcursor const* bad_cursor;
    assert_true(AMitemToCursor(
        AMstackItem(stack_ptr, AMcursorFromStr(AMstr("10@aabbcc00")), cmocka_cb, AMexpect(AM_VAL_TYPE_CURSOR)),
        &bad_cursor));
    /* assert_eq!(
           doc.get_cursor_position(&text, &bad_cursor, None),
           Err(AutomergeError::InvalidCursor(bad_cursor))
       );                                                                                                             */
    AMresult* result = AMstackResult(
        stack_ptr, AMgetCursorPosition(test_state->doc_state->doc, test_state->text, bad_cursor, NULL), NULL, NULL);
    if (AMresultStatus(result) == AM_STATUS_OK) {
        fail_msg("position from invalid `AMcursor`");
    }
}

static void test_AMgetCursor_failure(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    AMitems const heads0 =
        AMstackItems(stack_ptr, AMgetHeads(test_state->doc_state->doc), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMstackItem(NULL, AMspliceText(test_state->doc_state->doc, test_state->text, 3, 6, AMstr(" new text ")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(test_state->doc_state->doc, AMstr(NULL), NULL), cmocka_cb,
                AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* // cursors created after heads are invalid
       let cursor3 = doc.get_cursor(&text, 6, None).unwrap();                                                         */
    AMcursor const* cursor3;
    assert_true(
        AMitemToCursor(AMstackItem(stack_ptr, AMgetCursor(test_state->doc_state->doc, test_state->text, 6, NULL),
                                   cmocka_cb, AMexpect(AM_VAL_TYPE_CURSOR)),
                       &cursor3));
    /* let pos4 = doc.get_cursor_position(&text, &cursor3, None).unwrap();                                            */
    size_t pos4;
    assert_true(AMitemToUint(
        AMstackItem(stack_ptr, AMgetCursorPosition(test_state->doc_state->doc, test_state->text, cursor3, NULL),
                    cmocka_cb, AMexpect(AM_VAL_TYPE_UINT)),
        &pos4));
    /* assert_eq!(pos4, 6);                                                                                           */
    assert_int_equal(pos4, 6);
    /* assert_eq!(
           doc.get_cursor_position(&text, &cursor3, Some(&heads0)),
           Err(AutomergeError::InvalidCursor(cursor3))
    );                                                                                                                */
    AMresult* result = AMstackResult(
        stack_ptr, AMgetCursorPosition(test_state->doc_state->doc, test_state->text, cursor3, &heads0), NULL, NULL);
    if (AMresultStatus(result) == AM_STATUS_OK) {
        fail_msg("`AMcursor` from invalid heads");
    }
}

int run_cursor_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMgetCursor),
        cmocka_unit_test(test_AMgetCursorPosition),
        cmocka_unit_test(test_AMcursorFromBytes_failure),
        cmocka_unit_test(test_AMcursorFromStr_failure),
        cmocka_unit_test(test_AMgetCursorPosition_failure),
        cmocka_unit_test(test_AMgetCursor_failure),
    };

    return cmocka_run_group_tests(tests, setup, teardown);
}
