#ifndef GROUP_STATE_INCLUDED
#define GROUP_STATE_INCLUDED

/* local */
#include "automerge.h"

typedef struct {
    AMdoc* doc;
} GroupState;

int group_setup(void** state);

int group_teardown(void** state);

#endif