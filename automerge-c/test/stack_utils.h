#ifndef STACK_UTILS_H
#define STACK_UTILS_H

#include <stdint.h>

/* local */
#include "automerge.h"

/**
 * \brief Reports an error through a cmocka assertion.
 *
 * \param[in,out] stack A pointer to a pointer to an `AMresultStack` struct.
 * \param[in] discriminant An `AMvalueVariant` enum tag.
 * \pre \p stack` != NULL`.
 */
void cmocka_cb(AMresultStack** stack, uint8_t discriminant);

/**
 * \brief Allocates a result stack for storing the results allocated during one
 *        or more test cases.
 *
 * \param[in,out] state A pointer to a pointer to an `AMresultStack` struct.
 * \pre \p state` != NULL`.
 * \warning The `AMresultStack` struct returned through \p state must be
 *          deallocated with `teardown_stack()` in order to prevent memory leaks.
 */
int setup_stack(void** state);

/**
 * \brief Deallocates a result stack after deallocating any results that were
 *        stored in it by one or more test cases.
 *
 * \param[in] state A pointer to a pointer to an `AMresultStack` struct.
 * \pre \p state` != NULL`.
 */
int teardown_stack(void** state);

#endif  /* STACK_UTILS_H */
