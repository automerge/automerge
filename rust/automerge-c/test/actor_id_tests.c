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
#include <automerge-c/automerge.h>
#include "cmocka_utils.h"
#include "str_utils.h"

/**
 * \brief State for a group of cmocka test cases.
 */
typedef struct {
    /** An actor ID as an array of bytes. */
    uint8_t* src;
    /** The count of bytes in \p src. */
    size_t count;
    /** A stack of results. */
    AMstack* stack;
    /** An actor ID as a hexadecimal string. */
    AMbyteSpan str;
} TestState;

static int setup(void** state) {
    TestState* test_state = test_calloc(1, sizeof(TestState));
    test_state->str = AMstr("000102030405060708090a0b0c0d0e0f");
    test_state->count = test_state->str.count / 2;
    test_state->src = test_calloc(test_state->count, sizeof(uint8_t));
    hex_to_bytes(test_state->str.src, test_state->src, test_state->count);
    *state = test_state;
    return 0;
}

static int teardown(void** state) {
    TestState* test_state = *state;
    test_free(test_state->src);
    AMstackFree(&test_state->stack);
    test_free(test_state);
    return 0;
}

static void test_AMactorIdFromBytes(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->stack;
    /* Non-empty string. */
    AMresult* result = AMstackResult(stack_ptr, AMactorIdFromBytes(test_state->src, test_state->count), NULL, NULL);
    if (AMresultStatus(result) != AM_STATUS_OK) {
        fail_msg_view("%s", AMresultError(result));
    }
    assert_int_equal(AMresultSize(result), 1);
    AMitem* const item = AMresultItem(result);
    assert_int_equal(AMitemValType(item), AM_VAL_TYPE_ACTOR_ID);
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(item, &actor_id));
    AMbyteSpan const bytes = AMactorIdBytes(actor_id);
    assert_int_equal(bytes.count, test_state->count);
    assert_memory_equal(bytes.src, test_state->src, bytes.count);
    /* Empty array. */
    /** \todo Find out if this is intentionally allowed. */
    result = AMstackResult(stack_ptr, AMactorIdFromBytes(test_state->src, 0), NULL, NULL);
    if (AMresultStatus(result) != AM_STATUS_OK) {
        fail_msg_view("%s", AMresultError(result));
    }
    /* NULL array. */
    result = AMstackResult(stack_ptr, AMactorIdFromBytes(NULL, test_state->count), NULL, NULL);
    if (AMresultStatus(result) == AM_STATUS_OK) {
        fail_msg("`AMactorId` from `NULL`.");
    }
}

static void test_AMactorIdFromStr(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->stack;
    AMresult* result = AMstackResult(stack_ptr, AMactorIdFromStr(test_state->str), NULL, NULL);
    if (AMresultStatus(result) != AM_STATUS_OK) {
        fail_msg_view("%s", AMresultError(result));
    }
    assert_int_equal(AMresultSize(result), 1);
    AMitem* const item = AMresultItem(result);
    assert_int_equal(AMitemValType(item), AM_VAL_TYPE_ACTOR_ID);
    /* The hexadecimal string should've been decoded as identical bytes. */
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(item, &actor_id));
    AMbyteSpan const bytes = AMactorIdBytes(actor_id);
    assert_int_equal(bytes.count, test_state->count);
    assert_memory_equal(bytes.src, test_state->src, bytes.count);
    /* The bytes should've been encoded as an identical hexadecimal string. */
    assert_true(AMitemToActorId(item, &actor_id));
    AMbyteSpan const str = AMactorIdStr(actor_id);
    assert_int_equal(str.count, test_state->str.count);
    assert_memory_equal(str.src, test_state->str.src, str.count);
}

static void test_AMactorIdInit(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->stack;
    AMresult* prior_result = NULL;
    AMbyteSpan prior_bytes = {NULL, 0};
    AMbyteSpan prior_str = {NULL, 0};
    for (size_t i = 0; i != 11; ++i) {
        AMresult* result = AMstackResult(stack_ptr, AMactorIdInit(), NULL, NULL);
        if (AMresultStatus(result) != AM_STATUS_OK) {
            fail_msg_view("%s", AMresultError(result));
        }
        assert_int_equal(AMresultSize(result), 1);
        AMitem* const item = AMresultItem(result);
        assert_int_equal(AMitemValType(item), AM_VAL_TYPE_ACTOR_ID);
        AMactorId const* actor_id;
        assert_true(AMitemToActorId(item, &actor_id));
        AMbyteSpan const bytes = AMactorIdBytes(actor_id);
        assert_true(AMitemToActorId(item, &actor_id));
        AMbyteSpan const str = AMactorIdStr(actor_id);
        if (prior_result) {
            size_t const max_byte_count = fmax(bytes.count, prior_bytes.count);
            assert_memory_not_equal(bytes.src, prior_bytes.src, max_byte_count);
            size_t const max_char_count = fmax(str.count, prior_str.count);
            assert_memory_not_equal(str.src, prior_str.src, max_char_count);
        }
        prior_result = result;
        prior_bytes = bytes;
        prior_str = str;
    }
}

int run_actor_id_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMactorIdFromBytes),
        cmocka_unit_test(test_AMactorIdFromStr),
        cmocka_unit_test(test_AMactorIdInit),
    };

    return cmocka_run_group_tests(tests, setup, teardown);
}
