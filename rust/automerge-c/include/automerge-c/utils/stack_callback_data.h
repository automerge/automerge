#ifndef AUTOMERGE_C_UTILS_PUSH_CALLBACK_DATA_H
#define AUTOMERGE_C_UTILS_PUSH_CALLBACK_DATA_H
/**
 * \file
 * \brief Utility data structures, functions and macros for supplying
 *        parameters to the custom validation logic applied to `AMitem`
 *        structs.
 */

#include <automerge-c/automerge.h>

/**
 * \struct AMstackCallbackData
 * \brief  A data structure for passing the parameters of an item value test
 *         to an implementation of the `AMstackCallback` function prototype.
 */
typedef struct {
    /** A bitmask of `AMvalType` tags. */
    AMvalType bitmask;
    /** A null-terminated file path string. */
    char const* file;
    /** The ordinal number of a line within a file. */
    int line;
} AMstackCallbackData;

/**
 * \memberof AMstackCallbackData
 * \brief Allocates a new `AMstackCallbackData` struct and initializes its
 *        members from their corresponding arguments.
 *
 * \param[in] bitmask A bitmask of `AMvalType` tags.
 * \param[in] file A null-terminated file path string.
 * \param[in] line The ordinal number of a line within a file.
 * \return A pointer to a disowned `AMstackCallbackData` struct.
 * \warning The returned pointer must be passed to `free()` to avoid a memory
 *          leak.
 */
AMstackCallbackData* AMstackCallbackDataInit(AMvalType const bitmask, char const* const file, int const line);

/**
 * \memberof AMstackCallbackData
 * \def AMexpect
 * \brief Allocates a new `AMstackCallbackData` struct and initializes it from
 *        an `AMvalueType` bitmask.
 *
 * \param[in] bitmask A bitmask of `AMvalType` tags.
 * \return A pointer to a disowned `AMstackCallbackData` struct.
 * \warning The returned pointer must be passed to `free()` to avoid a memory
 *          leak.
 */
#define AMexpect(bitmask) AMstackCallbackDataInit(bitmask, __FILE__, __LINE__)

#endif /* AUTOMERGE_C_UTILS_PUSH_CALLBACK_DATA_H */
