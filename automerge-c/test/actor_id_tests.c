#include <math.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* third-party */
#include <cmocka.h>

/* local */
#include "automerge.h"
#include "str_utils.h"

typedef struct {
    uint8_t* src;
    char const* str;
    size_t count;
} GroupState;

static int group_setup(void** state) {
    GroupState* group_state = test_calloc(1, sizeof(GroupState));
    group_state->str = "000102030405060708090a0b0c0d0e0f";
    group_state->count = strlen(group_state->str) / 2;
    group_state->src = test_malloc(group_state->count);
    hex_to_bytes(group_state->str, group_state->src, group_state->count);
    *state = group_state;
    return 0;
}

static int group_teardown(void** state) {
    GroupState* group_state = *state;
    test_free(group_state->src);
    test_free(group_state);
    return 0;
}

static void test_AMactorIdInit() {
    AMresult* prior_result = NULL;
    AMbyteSpan prior_bytes;
    char const* prior_str = NULL;
    AMresult* result = NULL;
    for (size_t i = 0; i != 11; ++i) {
        result = AMactorIdInit();
        if (AMresultStatus(result) != AM_STATUS_OK) {
            fail_msg("%s", AMerrorMessage(result));
        }
        assert_int_equal(AMresultSize(result), 1);
        AMvalue const value = AMresultValue(result);
        assert_int_equal(value.tag, AM_VALUE_ACTOR_ID);
        AMbyteSpan const bytes = AMactorIdBytes(value.actor_id);
        char const* const str = AMactorIdStr(value.actor_id);
        if (prior_result) {
            size_t const min_count = fmax(bytes.count, prior_bytes.count);
            assert_memory_not_equal(bytes.src, prior_bytes.src, min_count);
            assert_string_not_equal(str, prior_str);
            AMfree(prior_result);
        }
        prior_result = result;
        prior_bytes = bytes;
        prior_str = str;
    }
    AMfree(result);
}

static void test_AMactorIdInitBytes(void **state) {
    GroupState* group_state = *state;
    AMresult* const result = AMactorIdInitBytes(group_state->src, group_state->count);
    if (AMresultStatus(result) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(result));
    }
    assert_int_equal(AMresultSize(result), 1);
    AMvalue const value = AMresultValue(result);
    assert_int_equal(value.tag, AM_VALUE_ACTOR_ID);
    AMbyteSpan const bytes = AMactorIdBytes(value.actor_id);
    assert_int_equal(bytes.count, group_state->count);
    assert_memory_equal(bytes.src, group_state->src, bytes.count);
    AMfree(result);
}

static void test_AMactorIdInitStr(void **state) {
    GroupState* group_state = *state;
    AMresult* const result = AMactorIdInitStr(group_state->str);
    if (AMresultStatus(result) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(result));
    }
    assert_int_equal(AMresultSize(result), 1);
    AMvalue const value = AMresultValue(result);
    assert_int_equal(value.tag, AM_VALUE_ACTOR_ID);
    char const* const str = AMactorIdStr(value.actor_id);
    assert_int_equal(strlen(str), group_state->count * 2);
    assert_string_equal(str, group_state->str);
    AMfree(result);
}

int run_actor_id_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMactorIdInit),
        cmocka_unit_test(test_AMactorIdInitBytes),
        cmocka_unit_test(test_AMactorIdInitStr),
    };

    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
