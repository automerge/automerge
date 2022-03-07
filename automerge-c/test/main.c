#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <setjmp.h>
#include <stdint.h>

/* third-party */
#include <cmocka.h>

/* local */
#include "automerge.h"

typedef struct {
    AMdoc* doc;
} GroupState;

static int group_setup(void** state) {
    GroupState* group_state = calloc(1, sizeof(GroupState));
    group_state->doc = AMcreate();
    *state = group_state;
    return 0;
}

static int group_teardown(void** state) {
    GroupState* group_state = *state;
    AMdestroy(group_state->doc);
    free(group_state);
    return 0;
}

static void test_AMconfig(void **state) {
    GroupState* group_state = *state;
    AMconfig(group_state->doc, "actor", "aabbcc");
}

static void test_AMmapSetStr(void **state) {
    GroupState* group_state = *state;
    AMresult* res = AMmapSetStr(group_state->doc, NULL, "string", "hello world");
    if (AMresultStatus(res) != AM_STATUS_COMMAND_OK) {
        fail_msg("%s", AMerrorMessage(res));
    }
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMconfig),
        cmocka_unit_test(test_AMmapSetStr),
    };

    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
