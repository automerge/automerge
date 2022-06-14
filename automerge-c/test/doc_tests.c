#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

/* third-party */
#include <cmocka.h>

/* local */
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
        cmocka_unit_test_setup_teardown(test_AMputActor_bytes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMputActor_hex, setup, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
