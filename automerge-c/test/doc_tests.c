#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

/* third-party */
#include <cmocka.h>

/* local */
#include "automerge.h"
#include "group_state.h"
#include "stack_utils.h"
#include "str_utils.h"

typedef struct {
    GroupState* group_state;
    char const* actor_id_str;
    uint8_t* actor_id_bytes;
    size_t actor_id_size;
} TestState;

static int setup(void** state) {
    TestState* test_state = test_calloc(1, sizeof(TestState));
    group_setup((void**)&test_state->group_state);
    test_state->actor_id_str = "000102030405060708090a0b0c0d0e0f";
    test_state->actor_id_size = strlen(test_state->actor_id_str) / 2;
    test_state->actor_id_bytes = test_malloc(test_state->actor_id_size);
    hex_to_bytes(test_state->actor_id_str, test_state->actor_id_bytes, test_state->actor_id_size);
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
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    AMstrs forward = AMpush(&stack,
                            AMkeys(doc, AM_ROOT, NULL),
                            AM_VALUE_STRS,
                            cmocka_cb).strs;
    assert_int_equal(AMstrsSize(&forward), 0);
    AMstrs reverse = AMstrsReversed(&forward);
    assert_int_equal(AMstrsSize(&reverse), 0);
    assert_null(AMstrsNext(&forward, 1));
    assert_null(AMstrsPrev(&forward, 1));
    assert_null(AMstrsNext(&reverse, 1));
    assert_null(AMstrsPrev(&reverse, 1));
    AMfreeStack(&stack);
}

static void test_AMkeys_list() {
    AMresultStack* stack = NULL;
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMlistPutInt(doc, AM_ROOT, 0, true, 1));
    AMfree(AMlistPutInt(doc, AM_ROOT, 1, true, 2));
    AMfree(AMlistPutInt(doc, AM_ROOT, 2, true, 3));
    AMstrs forward = AMpush(&stack,
                            AMkeys(doc, AM_ROOT, NULL),
                            AM_VALUE_STRS,
                            cmocka_cb).strs;
    assert_int_equal(AMstrsSize(&forward), 3);
    AMstrs reverse = AMstrsReversed(&forward);
    assert_int_equal(AMstrsSize(&reverse), 3);
    /* Forward iterator forward. */
    char const* str = AMstrsNext(&forward, 1);
    assert_ptr_equal(strstr(str, "1@"), str);
    str = AMstrsNext(&forward, 1);
    assert_ptr_equal(strstr(str, "2@"), str);
    str = AMstrsNext(&forward, 1);
    assert_ptr_equal(strstr(str, "3@"), str);
    assert_null(AMstrsNext(&forward, 1));
    /* Forward iterator reverse. */
    str = AMstrsPrev(&forward, 1);
    assert_ptr_equal(strstr(str, "3@"), str);
    str = AMstrsPrev(&forward, 1);
    assert_ptr_equal(strstr(str, "2@"), str);
    str = AMstrsPrev(&forward, 1);
    assert_ptr_equal(strstr(str, "1@"), str);
    assert_null(AMstrsPrev(&forward, 1));
    /* Reverse iterator forward. */
    str = AMstrsNext(&reverse, 1);
    assert_ptr_equal(strstr(str, "3@"), str);
    str = AMstrsNext(&reverse, 1);
    assert_ptr_equal(strstr(str, "2@"), str);
    str = AMstrsNext(&reverse, 1);
    assert_ptr_equal(strstr(str, "1@"), str);
    /* Reverse iterator reverse. */
    assert_null(AMstrsNext(&reverse, 1));
    str = AMstrsPrev(&reverse, 1);
    assert_ptr_equal(strstr(str, "1@"), str);
    str = AMstrsPrev(&reverse, 1);
    assert_ptr_equal(strstr(str, "2@"), str);
    str = AMstrsPrev(&reverse, 1);
    assert_ptr_equal(strstr(str, "3@"), str);
    assert_null(AMstrsPrev(&reverse, 1));
    AMfreeStack(&stack);
}

static void test_AMkeys_map() {
    AMresultStack* stack = NULL;
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMmapPutInt(doc, AM_ROOT, "one", 1));
    AMfree(AMmapPutInt(doc, AM_ROOT, "two", 2));
    AMfree(AMmapPutInt(doc, AM_ROOT, "three", 3));
    AMstrs forward = AMpush(&stack,
                            AMkeys(doc, AM_ROOT, NULL),
                            AM_VALUE_STRS,
                            cmocka_cb).strs;
    assert_int_equal(AMstrsSize(&forward), 3);
    AMstrs reverse = AMstrsReversed(&forward);
    assert_int_equal(AMstrsSize(&reverse), 3);
    /* Forward iterator forward. */
    assert_string_equal(AMstrsNext(&forward, 1), "one");
    assert_string_equal(AMstrsNext(&forward, 1), "three");
    assert_string_equal(AMstrsNext(&forward, 1), "two");
    assert_null(AMstrsNext(&forward, 1));
    /* Forward iterator reverse. */
    assert_string_equal(AMstrsPrev(&forward, 1), "two");
    assert_string_equal(AMstrsPrev(&forward, 1), "three");
    assert_string_equal(AMstrsPrev(&forward, 1), "one");
    assert_null(AMstrsPrev(&forward, 1));
    /* Reverse iterator forward. */
    assert_string_equal(AMstrsNext(&reverse, 1), "two");
    assert_string_equal(AMstrsNext(&reverse, 1), "three");
    assert_string_equal(AMstrsNext(&reverse, 1), "one");
    assert_null(AMstrsNext(&reverse, 1));
    /* Reverse iterator reverse. */
    assert_string_equal(AMstrsPrev(&reverse, 1), "one");
    assert_string_equal(AMstrsPrev(&reverse, 1), "three");
    assert_string_equal(AMstrsPrev(&reverse, 1), "two");
    assert_null(AMstrsPrev(&reverse, 1));
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

static void test_AMputActor_hex(void **state) {
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
    char const* const str = AMactorIdStr(actor_id);
    assert_int_equal(strlen(str), test_state->actor_id_size * 2);
    assert_string_equal(str, test_state->actor_id_str);
}

static void test_AMspliceText() {
    AMresultStack* stack = NULL;
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMspliceText(doc, AM_ROOT, 0, 0, "one + "));
    AMfree(AMspliceText(doc, AM_ROOT, 4, 2, "two = "));
    AMfree(AMspliceText(doc, AM_ROOT, 8, 2, "three"));
    char const* const text = AMpush(&stack,
                                    AMtext(doc, AM_ROOT, NULL),
                                    AM_VALUE_STR,
                                    cmocka_cb).str;
    assert_string_equal(text, "one two three");
    AMfreeStack(&stack);
}

int run_doc_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMkeys_empty),
        cmocka_unit_test(test_AMkeys_list),
        cmocka_unit_test(test_AMkeys_map),
        cmocka_unit_test_setup_teardown(test_AMputActor_bytes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMputActor_hex, setup, teardown),
        cmocka_unit_test(test_AMspliceText),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
