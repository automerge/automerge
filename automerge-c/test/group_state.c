#include <setjmp.h>
#include <stdarg.h>
#include <stdlib.h>

/* third-party */
#include <cmocka.h>

/* local */
#include "group_state.h"
#include "stack_utils.h"

int group_setup(void** state) {
    GroupState* group_state = test_calloc(1, sizeof(GroupState));
    group_state->doc = AMpush(&group_state->stack,
                              AMcreate(NULL),
                              AM_VALUE_DOC,
                              cmocka_cb).doc;
    *state = group_state;
    return 0;
}

int group_teardown(void** state) {
    GroupState* group_state = *state;
    AMfreeStack(&group_state->stack);
    test_free(group_state);
    return 0;
}
