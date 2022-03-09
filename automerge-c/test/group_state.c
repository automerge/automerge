#include <stdlib.h>

/* local */
#include "group_state.h"

int group_setup(void** state) {
    GroupState* group_state = calloc(1, sizeof(GroupState));
    group_state->doc = AMcreate();
    *state = group_state;
    return 0;
}

int group_teardown(void** state) {
    GroupState* group_state = *state;
    AMdestroy(group_state->doc);
    free(group_state);
    return 0;
}
