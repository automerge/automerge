#ifndef TESTS_DOC_STATE_H
#define TESTS_DOC_STATE_H

/* local */
#include <automerge-c/automerge.h>
#include "base_state.h"

typedef struct {
    BaseState* base_state;
    AMdoc* doc;
} DocState;

int setup_doc(void** state);

int teardown_doc(void** state);

#endif /* TESTS_DOC_STATE_H */
