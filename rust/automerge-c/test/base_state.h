#ifndef TESTS_BASE_STATE_H
#define TESTS_BASE_STATE_H

#include <stdint.h>

/* local */
#include <automerge-c/automerge.h>
#include <automerge-c/utils/stack.h>

/**
 * \struct BaseState
 * \brief The shared state for one or more cmocka test cases.
 */
typedef struct {
    /** A stack of results. */
    AMstack* stack;
} BaseState;

/**
 * \memberof BaseState
 * \brief Sets up the shared state for one or more cmocka test cases.
 *
 * \param[in,out] state A pointer to a pointer to a `BaseState` struct.
 * \pre \p state `!= NULL`.
 * \warning The `BaseState` struct returned through \p state must be
 *          passed to `teardown_base()` in order to avoid a memory leak.
 */
int setup_base(void** state);

/**
 * \memberof BaseState
 * \brief Tears down the shared state for one or more cmocka test cases.
 *
 * \param[in] state A pointer to a pointer to a `BaseState` struct.
 * \pre \p state `!= NULL`.
 */
int teardown_base(void** state);

#endif /* TESTS_BASE_STATE_H */
