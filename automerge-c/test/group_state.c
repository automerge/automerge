#include <stdlib.h>

/* local */
#include "group_state.h"

int group_setup(void** state) {
    GroupState* group_state = calloc(1, sizeof(GroupState));
    group_state->doc_result = AMcreate();
    group_state->doc = AMresultValue(group_state->doc_result, 0).doc;
    *state = group_state;
    return 0;
}

int group_teardown(void** state) {
    GroupState* group_state = *state;
    AMfree(group_state->doc_result);
    free(group_state);
    return 0;
}
