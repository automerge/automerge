#ifndef AUTOMERGE_C_UTILS_STACK_H
#define AUTOMERGE_C_UTILS_STACK_H
/**
 * \file
 * \brief Utility data structures and functions for hiding `AMresult` structs,
 *        managing their lifetimes, and automatically applying custom
 *        validation logic to the `AMitem` structs that they contain.
 *
 * \note The `AMstack` struct and its related functions drastically reduce the
 *       need for boilerplate code and/or `goto` statement usage within a C
 *       application but a higher-level programming language offers even better
 *       ways to do the same things.
 */

#include <automerge-c/automerge.h>

/**
 * \struct AMstack
 * \brief A node in a singly-linked list of result pointers.
 */
typedef struct AMstack {
    /** A result to be deallocated. */
    AMresult* result;
    /** The previous node in the singly-linked list or `NULL`. */ 
    struct AMstack* prev;
} AMstack;

/**
 * \memberof AMstack
 * \brief The prototype of a function that examines the result at the top of
 *        the given stack in terms of some arbitrary data.
 *
 * \param[in,out] stack A pointer to a pointer to an `AMstack` struct.
 * \param[in] data A pointer to arbitrary data or `NULL`.
 * \return `true` if the top `AMresult` struct in \p stack is valid, `false`
 *         otherwise.
 * \pre \p stack `!= NULL`.
 */
typedef bool (*AMstackCallback)(AMstack** stack, void* data);

/**
 * \memberof AMstack
 * \brief Deallocates the storage for a stack of results.
 *
 * \param[in,out] stack A pointer to a pointer to an `AMstack` struct.
 * \pre \p stack `!= NULL`
 * \post `*stack == NULL`
 */
void AMstackFree(AMstack** stack);

/**
 * \memberof AMstack
 * \brief Gets a result from the stack after removing it.
 *
 * \param[in,out] stack A pointer to a pointer to an `AMstack` struct.
 * \param[in] result A pointer to the `AMresult` to be popped or `NULL` to
 *                   select the top result in \p stack.
 * \return A pointer to an `AMresult` struct or `NULL`.
 * \pre \p stack `!= NULL`
 * \warning The returned `AMresult` struct pointer must be passed to
 *          `AMresultFree()` in order to avoid a memory leak.
 */
AMresult* AMstackPop(AMstack** stack, AMresult const* result);

/**
 * \memberof AMstack
 * \brief Pushes the given result onto the given stack, calls the given
 *        callback with the given data to validate it and then either gets the
 *        result if it's valid or gets `NULL` instead.
 *
 * \param[in,out] stack A pointer to a pointer to an `AMstack` struct.
 * \param[in] result A pointer to an `AMresult` struct.
 * \param[in] callback A pointer to a function with the same signature as
 *                     `AMstackCallback()` or `NULL`.
 * \param[in] data A pointer to arbitrary data or `NULL` which is passed to
 *                 \p callback.
 * \return \p result or `NULL`.
 * \warning If \p stack `== NULL` then \p result is deallocated in order to
 *          avoid a memory leak.
 */
AMresult* AMstackResult(AMstack** stack, AMresult* result, AMstackCallback callback, void* data);

/**
 * \memberof AMstack
 * \brief Pushes the given result onto the given stack, calls the given
 *        callback with the given data to validate it and then either gets the
 *        first item in the sequence of items within that result if it's valid
 *        or gets `NULL` instead.
 *
 * \param[in,out] stack A pointer to a pointer to an `AMstack` struct.
 * \param[in] result A pointer to an `AMresult` struct.
 * \param[in] callback A pointer to a function with the same signature as
 *                     `AMstackCallback()` or `NULL`.
 * \param[in] data A pointer to arbitrary data or `NULL` which is passed to
 *                 \p callback.
 * \return A pointer to an `AMitem` struct or `NULL`.
 * \warning If \p stack `== NULL` then \p result is deallocated in order to
 *          avoid a memory leak.
 */
AMitem* AMstackItem(AMstack** stack, AMresult* result, AMstackCallback callback, void* data);

/**
 * \memberof AMstack
 * \brief Pushes the given result onto the given stack, calls the given
 *        callback with the given data to validate it and then either gets an
 *        `AMitems` struct over the sequence of items within that result if it's
 *        valid or gets an empty `AMitems` instead.
 *
 * \param[in,out] stack A pointer to a pointer to an `AMstack` struct.
 * \param[in] result A pointer to an `AMresult` struct.
 * \param[in] callback A pointer to a function with the same signature as
 *                     `AMstackCallback()` or `NULL`.
 * \param[in] data A pointer to arbitrary data or `NULL` which is passed to
 *                 \p callback.
 * \return An `AMitems` struct.
 * \warning If \p stack `== NULL` then \p result is deallocated immediately
 *          in order to avoid a memory leak.
 */
AMitems AMstackItems(AMstack** stack, AMresult* result, AMstackCallback callback, void* data);

/**
 * \memberof AMstack
 * \brief Gets the count of results that have been pushed onto the stack.
 *
 * \param[in,out] stack A pointer to an `AMstack` struct.
 * \return A 64-bit unsigned integer.
 */
size_t AMstackSize(AMstack const* const stack);

#endif /* AUTOMERGE_C_UTILS_STACK_H */
