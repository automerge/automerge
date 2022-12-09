#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

/* third-party */
#include <cmocka.h>

/* local */
#include <automerge-c/automerge.h>
#include "group_state.h"
#include "stack_utils.h"
#include "str_utils.h"

typedef struct {
    GroupState* group_state;
    AMbyteSpan actor_id_str;
    uint8_t* actor_id_bytes;
    size_t actor_id_size;
} TestState;

static int setup(void** state) {
    TestState* test_state = test_calloc(1, sizeof(TestState));
    group_setup((void**)&test_state->group_state);
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
    group_teardown((void**)&test_state->group_state);
    test_free(test_state->actor_id_bytes);
    test_free(test_state);
    return 0;
}

static void test_AMkeys_empty() {
    AMresultStack* stack = NULL;
    AMdoc* const doc = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMstrs forward = AMpush(&stack,
                            AMkeys(doc, AM_ROOT, NULL),
                            AM_VALUE_STRS,
                            cmocka_cb).strs;
    assert_int_equal(AMstrsSize(&forward), 0);
    AMstrs reverse = AMstrsReversed(&forward);
    assert_int_equal(AMstrsSize(&reverse), 0);
    assert_null(AMstrsNext(&forward, 1).src);
    assert_null(AMstrsPrev(&forward, 1).src);
    assert_null(AMstrsNext(&reverse, 1).src);
    assert_null(AMstrsPrev(&reverse, 1).src);
    AMfreeStack(&stack);
}

static void test_AMkeys_list() {
    AMresultStack* stack = NULL;
    AMdoc* const doc = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMobjId const* const list = AMpush(
        &stack,
        AMmapPutObject(doc, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    AMfree(AMlistPutInt(doc, list, 0, true, 0));
    AMfree(AMlistPutInt(doc, list, 1, true, 0));
    AMfree(AMlistPutInt(doc, list, 2, true, 0));
    AMstrs forward = AMpush(&stack,
                            AMkeys(doc, list, NULL),
                            AM_VALUE_STRS,
                            cmocka_cb).strs;
    assert_int_equal(AMstrsSize(&forward), 3);
    AMstrs reverse = AMstrsReversed(&forward);
    assert_int_equal(AMstrsSize(&reverse), 3);
    /* Forward iterator forward. */
    AMbyteSpan str = AMstrsNext(&forward, 1);
    assert_ptr_equal(strstr(str.src, "2@"), str.src);
    str = AMstrsNext(&forward, 1);
    assert_ptr_equal(strstr(str.src, "3@"), str.src);
    str = AMstrsNext(&forward, 1);
    assert_ptr_equal(strstr(str.src, "4@"), str.src);
    assert_null(AMstrsNext(&forward, 1).src);
    // /* Forward iterator reverse. */
    str = AMstrsPrev(&forward, 1);
    assert_ptr_equal(strstr(str.src, "4@"), str.src);
    str = AMstrsPrev(&forward, 1);
    assert_ptr_equal(strstr(str.src, "3@"), str.src);
    str = AMstrsPrev(&forward, 1);
    assert_ptr_equal(strstr(str.src, "2@"), str.src);
    assert_null(AMstrsPrev(&forward, 1).src);
    /* Reverse iterator forward. */
    str = AMstrsNext(&reverse, 1);
    assert_ptr_equal(strstr(str.src, "4@"), str.src);
    str = AMstrsNext(&reverse, 1);
    assert_ptr_equal(strstr(str.src, "3@"), str.src);
    str = AMstrsNext(&reverse, 1);
    assert_ptr_equal(strstr(str.src, "2@"), str.src);
    assert_null(AMstrsNext(&reverse, 1).src);
    /* Reverse iterator reverse. */
    str = AMstrsPrev(&reverse, 1);
    assert_ptr_equal(strstr(str.src, "2@"), str.src);
    str = AMstrsPrev(&reverse, 1);
    assert_ptr_equal(strstr(str.src, "3@"), str.src);
    str = AMstrsPrev(&reverse, 1);
    assert_ptr_equal(strstr(str.src, "4@"), str.src);
    assert_null(AMstrsPrev(&reverse, 1).src);
    AMfreeStack(&stack);
}

static void test_AMkeys_map() {
    AMresultStack* stack = NULL;
    AMdoc* const doc = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMmapPutInt(doc, AM_ROOT, AMstr("one"), 1));
    AMfree(AMmapPutInt(doc, AM_ROOT, AMstr("two"), 2));
    AMfree(AMmapPutInt(doc, AM_ROOT, AMstr("three"), 3));
    AMstrs forward = AMpush(&stack,
                            AMkeys(doc, AM_ROOT, NULL),
                            AM_VALUE_STRS,
                            cmocka_cb).strs;
    assert_int_equal(AMstrsSize(&forward), 3);
    AMstrs reverse = AMstrsReversed(&forward);
    assert_int_equal(AMstrsSize(&reverse), 3);
    /* Forward iterator forward. */
    AMbyteSpan str = AMstrsNext(&forward, 1);
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "one", str.count);
    str = AMstrsNext(&forward, 1);
    assert_int_equal(str.count, 5);
    assert_memory_equal(str.src, "three", str.count);
    str = AMstrsNext(&forward, 1);
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "two", str.count);
    assert_null(AMstrsNext(&forward, 1).src);
    /* Forward iterator reverse. */
    str = AMstrsPrev(&forward, 1);
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "two", str.count);
    str = AMstrsPrev(&forward, 1);
    assert_int_equal(str.count, 5);
    assert_memory_equal(str.src, "three", str.count);
    str = AMstrsPrev(&forward, 1);
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "one", str.count);
    assert_null(AMstrsPrev(&forward, 1).src);
    /* Reverse iterator forward. */
    str = AMstrsNext(&reverse, 1);
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "two", str.count);
    str = AMstrsNext(&reverse, 1);
    assert_int_equal(str.count, 5);
    assert_memory_equal(str.src, "three", str.count);
    str = AMstrsNext(&reverse, 1);
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "one", str.count);
    assert_null(AMstrsNext(&reverse, 1).src);
    /* Reverse iterator reverse. */
    str = AMstrsPrev(&reverse, 1);
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "one", str.count);
    str = AMstrsPrev(&reverse, 1);
    assert_int_equal(str.count, 5);
    assert_memory_equal(str.src, "three", str.count);
    str = AMstrsPrev(&reverse, 1);
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "two", str.count);
    assert_null(AMstrsPrev(&reverse, 1).src);
    AMfreeStack(&stack);
}

static void test_AMputActor_bytes(void **state) {
    TestState* test_state = *state;
    AMactorId const* actor_id = AMpush(&test_state->group_state->stack,
                                       AMactorIdInitBytes(
                                           test_state->actor_id_bytes,
                                           test_state->actor_id_size),
                                       AM_VALUE_ACTOR_ID,
                                       cmocka_cb).actor_id;
    AMfree(AMsetActorId(test_state->group_state->doc, actor_id));
    actor_id = AMpush(&test_state->group_state->stack,
                      AMgetActorId(test_state->group_state->doc),
                      AM_VALUE_ACTOR_ID,
                      cmocka_cb).actor_id;
    AMbyteSpan const bytes = AMactorIdBytes(actor_id);
    assert_int_equal(bytes.count, test_state->actor_id_size);
    assert_memory_equal(bytes.src, test_state->actor_id_bytes, bytes.count);
}

static void test_AMputActor_str(void **state) {
    TestState* test_state = *state;
    AMactorId const* actor_id = AMpush(&test_state->group_state->stack,
                                       AMactorIdInitStr(test_state->actor_id_str),
                                       AM_VALUE_ACTOR_ID,
                                       cmocka_cb).actor_id;
    AMfree(AMsetActorId(test_state->group_state->doc, actor_id));
    actor_id = AMpush(&test_state->group_state->stack,
                      AMgetActorId(test_state->group_state->doc),
                      AM_VALUE_ACTOR_ID,
                      cmocka_cb).actor_id;
    AMbyteSpan const str = AMactorIdStr(actor_id);
    assert_int_equal(str.count, test_state->actor_id_str.count);
    assert_memory_equal(str.src, test_state->actor_id_str.src, str.count);
}

static void test_AMspliceText() {
    AMresultStack* stack = NULL;
    AMdoc* const doc = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMobjId const* const text = AMpush(&stack,
                                       AMmapPutObject(doc, AM_ROOT, AMstr("text"), AM_OBJ_TYPE_TEXT),
                                       AM_VALUE_OBJ_ID,
                                       cmocka_cb).obj_id;    
    AMfree(AMspliceText(doc, text, 0, 0, AMstr("one + ")));
    AMfree(AMspliceText(doc, text, 4, 2, AMstr("two = ")));
    AMfree(AMspliceText(doc, text, 8, 2, AMstr("three")));
    AMbyteSpan const str = AMpush(&stack,
                                  AMtext(doc, text, NULL),
                                  AM_VALUE_STR,
                                  cmocka_cb).str;
    static char const* const STR_VALUE = "one two three";
    assert_int_equal(str.count, strlen(STR_VALUE));
    assert_memory_equal(str.src, STR_VALUE, str.count);
    AMfreeStack(&stack);
}

int run_doc_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMkeys_empty),
        cmocka_unit_test(test_AMkeys_list),
        cmocka_unit_test(test_AMkeys_map),
        cmocka_unit_test_setup_teardown(test_AMputActor_bytes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMputActor_str, setup, teardown),
        cmocka_unit_test(test_AMspliceText),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
