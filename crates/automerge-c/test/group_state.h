#ifndef GROUP_STATE_H
#define GROUP_STATE_H

/* local */
#include <automerge-c/automerge.h>

typedef struct {
    AMresultStack* stack;
    AMdoc* doc;
} GroupState;

int group_setup(void** state);

int group_teardown(void** state);

#endif  /* GROUP_STATE_H */
