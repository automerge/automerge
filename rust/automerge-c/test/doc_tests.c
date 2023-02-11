#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

/* third-party */
#include <cmocka.h>

/* local */
#include <automerge-c/automerge.h>
#include <automerge-c/utils/stack_callback_data.h>
#include "base_state.h"
#include "cmocka_utils.h"
#include "doc_state.h"
#include "str_utils.h"

typedef struct {
    DocState* doc_state;
    AMbyteSpan actor_id_str;
    uint8_t* actor_id_bytes;
    size_t actor_id_size;
} TestState;

static int setup(void** state) {
    TestState* test_state = test_calloc(1, sizeof(TestState));
    setup_doc((void**)&test_state->doc_state);
    test_state->actor_id_str.src = "000102030405060708090a0b0c0d0e0f";
    test_state->actor_id_str.count = strlen(test_state->actor_id_str.src);
    test_state->actor_id_size = test_state->actor_id_str.count / 2;
    test_state->actor_id_bytes = test_malloc(test_state->actor_id_size);
    hex_to_bytes(test_state->actor_id_str.src, test_state->actor_id_bytes, test_state->actor_id_size);
    *state = test_state;
    return 0;
}

static int teardown(void** state) {
    TestState* test_state = *state;
    teardown_doc((void**)&test_state->doc_state);
    test_free(test_state->actor_id_bytes);
    test_free(test_state);
    return 0;
}

static void test_AMkeys_empty(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    AMdoc* const doc = AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));
    AMitems forward = AMstackItems(stack_ptr, AMkeys(doc, AM_ROOT, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    assert_int_equal(AMitemsSize(&forward), 0);
    AMitems reverse = AMitemsReversed(&forward);
    assert_int_equal(AMitemsSize(&reverse), 0);
    assert_null(AMitemsNext(&forward, 1));
    assert_null(AMitemsPrev(&forward, 1));
    assert_null(AMitemsNext(&reverse, 1));
    assert_null(AMitemsPrev(&reverse, 1));
}

static void test_AMkeys_list(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    AMdoc* const doc = AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));
    AMobjId const* const list =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_VOID)));
    AMstackItem(NULL, AMlistPutInt(doc, list, 0, true, 0), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMlistPutInt(doc, list, 1, true, 0), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMlistPutInt(doc, list, 2, true, 0), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMitems forward = AMstackItems(stack_ptr, AMkeys(doc, list, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_int_equal(AMitemsSize(&forward), 3);
    AMitems reverse = AMitemsReversed(&forward);
    assert_int_equal(AMitemsSize(&reverse), 3);
    /* Forward iterator forward. */
    AMbyteSpan str = AMitemToStr(AMitemsNext(&forward, 1));
    assert_ptr_equal(strstr(str.src, "2@"), str.src);
    str = AMitemToStr(AMitemsNext(&forward, 1));
    assert_ptr_equal(strstr(str.src, "3@"), str.src);
    str = AMitemToStr(AMitemsNext(&forward, 1));
    assert_ptr_equal(strstr(str.src, "4@"), str.src);
    assert_null(AMitemsNext(&forward, 1));
    // /* Forward iterator reverse. */
    str = AMitemToStr(AMitemsPrev(&forward, 1));
    assert_ptr_equal(strstr(str.src, "4@"), str.src);
    str = AMitemToStr(AMitemsPrev(&forward, 1));
    assert_ptr_equal(strstr(str.src, "3@"), str.src);
    str = AMitemToStr(AMitemsPrev(&forward, 1));
    assert_ptr_equal(strstr(str.src, "2@"), str.src);
    assert_null(AMitemsPrev(&forward, 1));
    /* Reverse iterator forward. */
    str = AMitemToStr(AMitemsNext(&reverse, 1));
    assert_ptr_equal(strstr(str.src, "4@"), str.src);
    str = AMitemToStr(AMitemsNext(&reverse, 1));
    assert_ptr_equal(strstr(str.src, "3@"), str.src);
    str = AMitemToStr(AMitemsNext(&reverse, 1));
    assert_ptr_equal(strstr(str.src, "2@"), str.src);
    assert_null(AMitemsNext(&reverse, 1));
    /* Reverse iterator reverse. */
    str = AMitemToStr(AMitemsPrev(&reverse, 1));
    assert_ptr_equal(strstr(str.src, "2@"), str.src);
    str = AMitemToStr(AMitemsPrev(&reverse, 1));
    assert_ptr_equal(strstr(str.src, "3@"), str.src);
    str = AMitemToStr(AMitemsPrev(&reverse, 1));
    assert_ptr_equal(strstr(str.src, "4@"), str.src);
    assert_null(AMitemsPrev(&reverse, 1));
}

static void test_AMkeys_map(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    AMdoc* const doc = AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));
    AMstackItem(NULL, AMmapPutInt(doc, AM_ROOT, AMstr("one"), 1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutInt(doc, AM_ROOT, AMstr("two"), 2), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutInt(doc, AM_ROOT, AMstr("three"), 3), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMitems forward = AMstackItems(stack_ptr, AMkeys(doc, AM_ROOT, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_int_equal(AMitemsSize(&forward), 3);
    AMitems reverse = AMitemsReversed(&forward);
    assert_int_equal(AMitemsSize(&reverse), 3);
    /* Forward iterator forward. */
    AMbyteSpan str = AMitemToStr(AMitemsNext(&forward, 1));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "one", str.count);
    str = AMitemToStr(AMitemsNext(&forward, 1));
    assert_int_equal(str.count, 5);
    assert_memory_equal(str.src, "three", str.count);
    str = AMitemToStr(AMitemsNext(&forward, 1));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "two", str.count);
    assert_null(AMitemsNext(&forward, 1));
    /* Forward iterator reverse. */
    str = AMitemToStr(AMitemsPrev(&forward, 1));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "two", str.count);
    str = AMitemToStr(AMitemsPrev(&forward, 1));
    assert_int_equal(str.count, 5);
    assert_memory_equal(str.src, "three", str.count);
    str = AMitemToStr(AMitemsPrev(&forward, 1));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "one", str.count);
    assert_null(AMitemsPrev(&forward, 1));
    /* Reverse iterator forward. */
    str = AMitemToStr(AMitemsNext(&reverse, 1));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "two", str.count);
    str = AMitemToStr(AMitemsNext(&reverse, 1));
    assert_int_equal(str.count, 5);
    assert_memory_equal(str.src, "three", str.count);
    str = AMitemToStr(AMitemsNext(&reverse, 1));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "one", str.count);
    assert_null(AMitemsNext(&reverse, 1));
    /* Reverse iterator reverse. */
    str = AMitemToStr(AMitemsPrev(&reverse, 1));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "one", str.count);
    str = AMitemToStr(AMitemsPrev(&reverse, 1));
    assert_int_equal(str.count, 5);
    assert_memory_equal(str.src, "three", str.count);
    str = AMitemToStr(AMitemsPrev(&reverse, 1));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "two", str.count);
    assert_null(AMitemsPrev(&reverse, 1));
}

static void test_AMputActor_bytes(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    AMactorId const* actor_id = AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromBytes(test_state->actor_id_bytes, test_state->actor_id_size), cmocka_cb,
                    AMexpect(AM_VAL_TYPE_ACTOR_ID)));
    AMstackItem(NULL, AMsetActorId(test_state->doc_state->doc, actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    actor_id = AMitemToActorId(
        AMstackItem(stack_ptr, AMgetActorId(test_state->doc_state->doc), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)));
    AMbyteSpan const bytes = AMactorIdBytes(actor_id);
    assert_int_equal(bytes.count, test_state->actor_id_size);
    assert_memory_equal(bytes.src, test_state->actor_id_bytes, bytes.count);
}

static void test_AMputActor_str(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    AMactorId const* actor_id = AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(test_state->actor_id_str), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)));
    AMstackItem(NULL, AMsetActorId(test_state->doc_state->doc, actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    actor_id = AMitemToActorId(
        AMstackItem(stack_ptr, AMgetActorId(test_state->doc_state->doc), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)));
    AMbyteSpan const str = AMactorIdStr(actor_id);
    assert_int_equal(str.count, test_state->actor_id_str.count);
    assert_memory_equal(str.src, test_state->actor_id_str.src, str.count);
}

static void test_AMspliceText(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    AMdoc* const doc = AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));
    AMobjId const* const text =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("text"), AM_OBJ_TYPE_TEXT), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_VOID)));
    AMstackItem(NULL, AMspliceText(doc, text, 0, 0, AMstr("one + ")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMspliceText(doc, text, 4, 2, AMstr("two = ")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMspliceText(doc, text, 8, 2, AMstr("three")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMbyteSpan const str =
        AMitemToStr(AMstackItem(stack_ptr, AMtext(doc, text, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)));
    static char const* const STR_VALUE = "one two three";
    assert_int_equal(str.count, strlen(STR_VALUE));
    assert_memory_equal(str.src, STR_VALUE, str.count);
}

int run_doc_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_AMkeys_empty, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMkeys_list, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMkeys_map, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMputActor_bytes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMputActor_str, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMspliceText, setup, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
