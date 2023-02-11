#ifndef TESTS_MACRO_UTILS_H
#define TESTS_MACRO_UTILS_H

/* local */
#include <automerge-c/automerge.h>

/**
 * \brief Gets the object type tag corresponding to an object type suffix.
 *
 * \param[in] suffix An object type suffix string.
 * \return An `AMobjType` enum tag.
 */
AMobjType suffix_to_obj_type(char const* suffix);

/**
 * \brief Gets the value type tag corresponding to a value type suffix.
 *
 * \param[in] suffix A value type suffix string.
 * \return An `AMvalType` enum tag.
 */
AMvalType suffix_to_val_type(char const* suffix);

#endif /* TESTS_MACRO_UTILS_H */
