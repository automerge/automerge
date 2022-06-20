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
#include "str_utils.h"

typedef struct {
    GroupState* group_state;
    char const* actor_id_str;
    uint8_t* actor_id_bytes;
    size_t actor_id_size;
} TestState;

static int setup(void** state) {
    TestState* test_state = calloc(1, sizeof(TestState));
    group_setup((void**)&test_state->group_state);
    test_state->actor_id_str = "000102030405060708090a0b0c0d0e0f";
    test_state->actor_id_size = strlen(test_state->actor_id_str) / 2;
    test_state->actor_id_bytes = malloc(test_state->actor_id_size);
    hex_to_bytes(test_state->actor_id_str, test_state->actor_id_bytes, test_state->actor_id_size);
    *state = test_state;
    return 0;
}

static int teardown(void** state) {
    TestState* test_state = *state;
    group_teardown((void**)&test_state->group_state);
    free(test_state->actor_id_bytes);
    free(test_state);
    return 0;
}

static void test_AMkeys_empty() {
    AMresult* const doc_result = AMcreate();
    AMresult* const strings_result = AMkeys(AMresultValue(doc_result).doc, AM_ROOT, NULL);
    if (AMresultStatus(strings_result) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(strings_result));
    }
    assert_int_equal(AMresultSize(strings_result), 0);
    AMvalue value = AMresultValue(strings_result);
    assert_int_equal(value.tag, AM_VALUE_STRINGS);
    assert_int_equal(AMstringsSize(&value.strings), 0);
    AMstrings forward = value.strings;
    assert_null(AMstringsNext(&forward, 1));
    assert_null(AMstringsPrev(&forward, 1));
    AMstrings reverse = AMstringsReversed(&value.strings);
    assert_null(AMstringsNext(&reverse, 1));
    assert_null(AMstringsPrev(&reverse, 1));
    AMfree(strings_result);
    AMfree(doc_result);
}

static void test_AMkeys_list() {
    AMresult* const doc_result = AMcreate();
    AMdoc* const doc = AMresultValue(doc_result).doc;
    AMfree(AMlistPutInt(doc, AM_ROOT, 0, true, 1));
    AMfree(AMlistPutInt(doc, AM_ROOT, 1, true, 2));
    AMfree(AMlistPutInt(doc, AM_ROOT, 2, true, 3));
    AMresult* const strings_result = AMkeys(doc, AM_ROOT, NULL);
    if (AMresultStatus(strings_result) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(strings_result));
    }
    assert_int_equal(AMresultSize(strings_result), 3);
    AMvalue value = AMresultValue(strings_result);
    assert_int_equal(value.tag, AM_VALUE_STRINGS);
    AMstrings forward = value.strings;
    assert_int_equal(AMstringsSize(&forward), 3);
    /* Forward iterator forward. */
    char const* str = AMstringsNext(&forward, 1);
    assert_ptr_equal(strstr(str, "1@"), str);
    str = AMstringsNext(&forward, 1);
    assert_ptr_equal(strstr(str, "2@"), str);
    str = AMstringsNext(&forward, 1);
    assert_ptr_equal(strstr(str, "3@"), str);
    assert_null(AMstringsNext(&forward, 1));
    /* Forward iterator reverse. */
    str = AMstringsPrev(&forward, 1);
    assert_ptr_equal(strstr(str, "3@"), str);
    str = AMstringsPrev(&forward, 1);
    assert_ptr_equal(strstr(str, "2@"), str);
    str = AMstringsPrev(&forward, 1);
    assert_ptr_equal(strstr(str, "1@"), str);
    assert_null(AMstringsPrev(&forward, 1));
    AMstrings reverse = AMstringsReversed(&value.strings);
    assert_int_equal(AMstringsSize(&reverse), 3);
    /* Reverse iterator forward. */
    str = AMstringsNext(&reverse, 1);
    assert_ptr_equal(strstr(str, "3@"), str);
    str = AMstringsNext(&reverse, 1);
    assert_ptr_equal(strstr(str, "2@"), str);
    str = AMstringsNext(&reverse, 1);
    assert_ptr_equal(strstr(str, "1@"), str);
    /* Reverse iterator reverse. */
    assert_null(AMstringsNext(&reverse, 1));
    str = AMstringsPrev(&reverse, 1);
    assert_ptr_equal(strstr(str, "1@"), str);
    str = AMstringsPrev(&reverse, 1);
    assert_ptr_equal(strstr(str, "2@"), str);
    str = AMstringsPrev(&reverse, 1);
    assert_ptr_equal(strstr(str, "3@"), str);
    assert_null(AMstringsPrev(&reverse, 1));
    AMfree(strings_result);
    AMfree(doc_result);
}

static void test_AMkeys_map() {
    AMresult* const doc_result = AMcreate();
    AMdoc* const doc = AMresultValue(doc_result).doc;
    AMfree(AMmapPutInt(doc, AM_ROOT, "one", 1));
    AMfree(AMmapPutInt(doc, AM_ROOT, "two", 2));
    AMfree(AMmapPutInt(doc, AM_ROOT, "three", 3));
    AMresult* const strings_result = AMkeys(doc, AM_ROOT, NULL);
    if (AMresultStatus(strings_result) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(strings_result));
    }
    assert_int_equal(AMresultSize(strings_result), 3);
    AMvalue value = AMresultValue(strings_result);
    assert_int_equal(value.tag, AM_VALUE_STRINGS);
    AMstrings forward = value.strings;
    assert_int_equal(AMstringsSize(&forward), 3);
    /* Forward iterator forward. */
    assert_string_equal(AMstringsNext(&forward, 1), "one");
    assert_string_equal(AMstringsNext(&forward, 1), "three");
    assert_string_equal(AMstringsNext(&forward, 1), "two");
    assert_null(AMstringsNext(&forward, 1));
    /* Forward iterator reverse. */
    assert_string_equal(AMstringsPrev(&forward, 1), "two");
    assert_string_equal(AMstringsPrev(&forward, 1), "three");
    assert_string_equal(AMstringsPrev(&forward, 1), "one");
    assert_null(AMstringsPrev(&forward, 1));
    AMstrings reverse = AMstringsReversed(&value.strings);
    assert_int_equal(AMstringsSize(&reverse), 3);
    /* Reverse iterator forward. */
    assert_string_equal(AMstringsNext(&reverse, 1), "two");
    assert_string_equal(AMstringsNext(&reverse, 1), "three");
    assert_string_equal(AMstringsNext(&reverse, 1), "one");
    assert_null(AMstringsNext(&reverse, 1));
    /* Reverse iterator reverse. */
    assert_string_equal(AMstringsPrev(&reverse, 1), "one");
    assert_string_equal(AMstringsPrev(&reverse, 1), "three");
    assert_string_equal(AMstringsPrev(&reverse, 1), "two");
    assert_null(AMstringsPrev(&reverse, 1));
    AMfree(strings_result);
    AMfree(doc_result);
}

static void test_AMputActor_bytes(void **state) {
    TestState* test_state = *state;
    GroupState* group_state = test_state->group_state;
    AMresult* actor_id_result = AMactorIdInitBytes(test_state->actor_id_bytes,
                                                   test_state->actor_id_size);
    AMvalue value = AMresultValue(actor_id_result);
    AMresult* result = AMsetActor(group_state->doc, value.actor_id);
    AMfree(actor_id_result);
    if (AMresultStatus(result) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(result));
    }
    assert_int_equal(AMresultSize(result), 0);
    value = AMresultValue(result);
    assert_int_equal(value.tag, AM_VALUE_VOID);
    AMfree(result);
    result = AMgetActor(group_state->doc);
    if (AMresultStatus(result) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(result));
    }
    assert_int_equal(AMresultSize(result), 1);
    value = AMresultValue(result);
    assert_int_equal(value.tag, AM_VALUE_ACTOR_ID);
    AMbyteSpan const bytes = AMactorIdBytes(value.actor_id);
    assert_int_equal(bytes.count, test_state->actor_id_size);
    assert_memory_equal(bytes.src, test_state->actor_id_bytes, bytes.count);
    AMfree(result);
}

static void test_AMputActor_hex(void **state) {
    TestState* test_state = *state;
    GroupState* group_state = test_state->group_state;
    AMresult* actor_id_result = AMactorIdInitStr(test_state->actor_id_str);
    AMvalue value = AMresultValue(actor_id_result);
    AMresult* result = AMsetActor(group_state->doc, value.actor_id);
    AMfree(actor_id_result);
    if (AMresultStatus(result) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(result));
    }
    assert_int_equal(AMresultSize(result), 0);
    value = AMresultValue(result);
    assert_int_equal(value.tag, AM_VALUE_VOID);
    AMfree(result);
    result = AMgetActor(group_state->doc);
    if (AMresultStatus(result) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(result));
    }
    assert_int_equal(AMresultSize(result), 1);
    value = AMresultValue(result);
    assert_int_equal(value.tag, AM_VALUE_ACTOR_ID);
    char const* const str = AMactorIdStr(value.actor_id);
    assert_int_equal(strlen(str), test_state->actor_id_size * 2);
    assert_string_equal(str, test_state->actor_id_str);
    AMfree(result);
}

int run_doc_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMkeys_empty),
        cmocka_unit_test(test_AMkeys_list),
        cmocka_unit_test(test_AMkeys_map),
        cmocka_unit_test_setup_teardown(test_AMputActor_bytes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMputActor_hex, setup, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
