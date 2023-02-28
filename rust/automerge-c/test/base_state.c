#include <stdlib.h>

/* local */
#include "base_state.h"

int setup_base(void** state) {
    BaseState* base_state = calloc(1, sizeof(BaseState));
    *state = base_state;
    return 0;
}

int teardown_base(void** state) {
    BaseState* base_state = *state;
    AMstackFree(&base_state->stack);
    free(base_state);
    return 0;
}
