#ifndef MACRO_UTILS_H
#define MACRO_UTILS_H

/* local */
#include <automerge-c/automerge.h>

/**
 * \brief Gets the result value discriminant corresponding to a function name
 *        suffix.
 *
 * \param[in] suffix A string.
 * \return An `AMvalue` struct discriminant.
 */
AMvalueVariant AMvalue_discriminant(char const* suffix);

/**
 * \brief Gets the object type tag corresponding to an object type label.
 *
 * \param[in] obj_type_label A string.
 * \return An `AMobjType` enum tag.
 */
AMobjType AMobjType_tag(char const* obj_type_label);

#endif  /* MACRO_UTILS_H */
