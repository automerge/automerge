#include <float.h>
#include <limits.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

/* third-party */
#include <cmocka.h>

/* local */
#include <automerge-c/automerge.h>
#include <automerge-c/config.h>
#include <automerge-c/utils/stack_callback_data.h>
#include <automerge-c/utils/string.h>
#include "base_state.h"
#include "cmocka_utils.h"
#include "doc_state.h"
#include "macro_utils.h"

static void test_AMmark_round_trip(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMobjId const* const obj_id =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc_state->doc, AM_ROOT, AMstr("text"), AM_OBJ_TYPE_TEXT),
                                cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));

    AMstackItem(stack_ptr, AMspliceText(doc_state->doc, obj_id, 0, 0, AMstr("hello world")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));

    AMitem* val = AMstackItem(stack_ptr, AMitemFromBool(true), cmocka_cb, AMexpect(AM_VAL_TYPE_BOOL));

    AMstackItem(stack_ptr, AMmarkCreate(doc_state->doc, obj_id, 0, 5, AM_MARK_EXPAND_BOTH, AMstr("bold"), val),
                cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    AMstackItem(stack_ptr, AMspliceText(doc_state->doc, obj_id, 5, 0, AMstr("cool ")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));

    AMstackItem(stack_ptr, AMmarkClear(doc_state->doc, obj_id, 1, 6, AM_MARK_EXPAND_BOTH, AMstr("bold")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));

    AMitems marks =
        AMstackItems(stack_ptr, AMmarks(doc_state->doc, obj_id, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_MARK));

    assert_int_equal(2, AMitemsSize(&marks));
    AMmark const* mark;

    assert_true(AMitemToMark(AMitemsNext(&marks, 1), &mark));
    assert_int_equal(0, AMmarkStart(mark));
    assert_int_equal(1, AMmarkEnd(mark));

    assert_true(AMitemToMark(AMitemsNext(&marks, 1), &mark));
    assert_int_equal(6, AMmarkStart(mark));
    assert_int_equal(10, AMmarkEnd(mark));
    AMbyteSpan name = AMmarkName(mark);
    assert_int_equal(name.count, strlen("bold"));
    assert_memory_equal(name.src, "bold", name.count);

    bool b;
    assert_true(AMitemToBool(AMstackItem(stack_ptr, AMmarkValue(mark), cmocka_cb, AMexpect(AM_VAL_TYPE_BOOL)), &b));

    assert_true(b);
}

static void test_AMmark_unicode_indexing(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;

    AMobjId const* const obj_id =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc_state->doc, AM_ROOT, AMstr("text"), AM_OBJ_TYPE_TEXT),
                                cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));

    AMstackItem(stack_ptr, AMspliceText(doc_state->doc, obj_id, 0, 0, AMstr("🇬🇧🇩🇪")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMitem* val = AMstackItem(stack_ptr, AMitemFromBool(true), cmocka_cb, AMexpect(AM_VAL_TYPE_BOOL));

#ifdef AUTOMERGE_C_UTF8
    AMstackItem(stack_ptr, AMmarkCreate(doc_state->doc, obj_id, 8, 16, AM_MARK_EXPAND_NONE, AMstr("bold"), val),
                cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    AMstackItem(stack_ptr, AMspliceText(doc_state->doc, obj_id, 0, 8, AMstr("hi ")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
#else
    AMstackItem(stack_ptr, AMmarkCreate(doc_state->doc, obj_id, 2, 4, AM_MARK_EXPAND_NONE, AMstr("bold"), val),
                cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(stack_ptr, AMspliceText(doc_state->doc, obj_id, 0, 2, AMstr("hi ")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
#endif

    AMbyteSpan str;
    assert_true(
        AMitemToStr(AMstackItem(stack_ptr, AMtext(doc_state->doc, obj_id, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));

    assert_int_equal(str.count, strlen("hi 🇩🇪"));
    assert_memory_equal(str.src, "hi 🇩🇪", str.count);

    AMitems marks =
        AMstackItems(stack_ptr, AMmarks(doc_state->doc, obj_id, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_MARK));

    AMmark const* mark;
    assert_int_equal(1, AMitemsSize(&marks));
    assert_true(AMitemToMark(AMitemsNext(&marks, 1), &mark));

    assert_int_equal(3, AMmarkStart(mark));

#ifdef AUTOMERGE_C_UTF8
    assert_int_equal(11, AMmarkEnd(mark));
#else
    assert_int_equal(5, AMmarkEnd(mark));
#endif

}

int run_mark_tests(void) {
    struct CMUnitTest const tests[] = {
        cmocka_unit_test(test_AMmark_round_trip),
        cmocka_unit_test(test_AMmark_unicode_indexing),
    };

    return cmocka_run_group_tests(tests, setup_doc, teardown_doc);
}
