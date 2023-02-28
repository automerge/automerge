#ifndef AUTOMERGE_C_UTILS_RESULT_H
#define AUTOMERGE_C_UTILS_RESULT_H
/**
 * \file
 * \brief Utility functions for use with `AMresult` structs.
 */

#include <stdarg.h>

#include <automerge-c/automerge.h>

/**
 * \brief Transfers the items within an arbitrary list of results into a
 *        new result in their order of specification.
 * \param[in] count The count of subsequent arguments.
 * \param[in] ... A \p count list of arguments, each of which is a pointer to
 *                an `AMresult` struct whose items will be transferred out of it
 *                and which is subsequently freed.
 * \return A pointer to an `AMresult` struct or `NULL`.
 * \pre `∀𝑥 ∈` \p ... `, AMresultStatus(𝑥) == AM_STATUS_OK`
 * \post `(∃𝑥 ∈` \p ... `, AMresultStatus(𝑥) != AM_STATUS_OK) -> NULL`
 * \attention All `AMresult` struct pointer arguments are passed to
 *            `AMresultFree()` regardless of success; use `AMresultCat()`
 *            instead if you wish to pass them to `AMresultFree()` yourself.
 * \warning The returned `AMresult` struct pointer must be passed to
 *          `AMresultFree()` in order to avoid a memory leak.
 */
AMresult* AMresultFrom(int count, ...);

#endif /* AUTOMERGE_C_UTILS_RESULT_H */
