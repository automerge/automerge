#ifndef AUTOMERGE_C_UTILS_RESULT_H
#define AUTOMERGE_C_UTILS_RESULT_H

#include <stdarg.h>

#include <automerge-c/automerge.h>

/** \memberof AMresult
 * \brief Transfers the items within an arbitrary list of results into a
 *        new result in their order of specification.
 * \param[in] count The count of subsequent arguments.
 * \param[in] ... A \p count list of arguments, each of which is a pointer to
 *                an `AMresult` struct whose items will be transferred out of it
 *                and which is subsequently freed.
 * \return A new `AMresult` struct.
 * \warning The returned `AMresult` struct must be passed to `AMfree()`
 *          in order to avoid a memory leak.
*/
AMresult* AMresultFrom(int count, ...);

#endif /* AUTOMERGE_C_UTILS_RESULT_H */
