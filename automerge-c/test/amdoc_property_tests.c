#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* third-party */
#include <cmocka.h>

/* local */
#include "group_state.h"

typedef struct {
    GroupState* group_state;
    char const* actor_id_str;
    uint8_t* actor_id_bytes;
    size_t actor_id_size;
} TestState;

static void hex_to_bytes(char const* hex_str, uint8_t* bytes, size_t const count) {
    unsigned int byte;
    char const* next = hex_str;
	for (size_t index = 0; *next && index != count; next += 2, ++index) {
		if (sscanf(next, "%02x", &byte) == 1) {
            bytes[index] = (uint8_t)byte;
        }
	}
}

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

static void test_AMputActor(void **state) {
    TestState* test_state = *state;
    GroupState* group_state = test_state->group_state;
    AMresult* res = AMsetActor(
        group_state->doc,
        test_state->actor_id_bytes,
        test_state->actor_id_size
    );
    if (AMresultStatus(res) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
    assert_int_equal(AMresultSize(res), 0);
    AMvalue value = AMresultValue(res, 0);
    assert_int_equal(value.tag, AM_VALUE_VOID);
    AMfree(res);
    res = AMgetActor(group_state->doc);
    if (AMresultStatus(res) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
    assert_int_equal(AMresultSize(res), 1);
    value = AMresultValue(res, 0);
    assert_int_equal(value.tag, AM_VALUE_ACTOR_ID);
    assert_int_equal(value.actor_id.count, test_state->actor_id_size);
    assert_memory_equal(value.actor_id.src, test_state->actor_id_bytes, value.actor_id.count);
    AMfree(res);
}

static void test_AMputActorHex(void **state) {
    TestState* test_state = *state;
    GroupState* group_state = test_state->group_state;
    AMresult* res = AMsetActorHex(
        group_state->doc,
        test_state->actor_id_str
    );
    if (AMresultStatus(res) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
    assert_int_equal(AMresultSize(res), 0);
    AMvalue value = AMresultValue(res, 0);
    assert_int_equal(value.tag, AM_VALUE_VOID);
    AMfree(res);
    res = AMgetActorHex(group_state->doc);
    if (AMresultStatus(res) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
    assert_int_equal(AMresultSize(res), 1);
    value = AMresultValue(res, 0);
    assert_int_equal(value.tag, AM_VALUE_STR);
    assert_int_equal(strlen(value.str), test_state->actor_id_size * 2);
    assert_string_equal(value.str, test_state->actor_id_str);
    AMfree(res);
}

int run_AMdoc_property_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_AMputActor, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMputActorHex, setup, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
