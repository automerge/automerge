#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>

/* third-party */
#include <cmocka.h>

/* local */
#include "group_state.h"

extern int run_AMmapSet_tests(void);

static void test_AMconfig(void **state) {
    GroupState* group_state = *state;
    AMconfig(group_state->doc, "actor", "aabbcc");
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMconfig),
    };

    return (
        run_AMmapSet_tests() +
        cmocka_run_group_tests(tests, group_setup, group_teardown)
    );
}
