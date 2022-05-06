#ifndef MACRO_UTILS_INCLUDED
#define MACRO_UTILS_INCLUDED

/* local */
#include "automerge.h"

/**
 * \brief Gets the `AMvalue` struct discriminant corresponding to a function
 *        name suffix.
 *
 * \param[in] suffix A string.
 * \return An `AMvalue` struct discriminant.
 */
AMvalueVariant AMvalue_discriminant(char const* suffix);

/**
 * \brief Gets the `AMobjType` enum tag corresponding to an object type label.
 *
 * \param[in] obj_type_label A string.
 * \return An `AMobjType` enum tag.
 */
AMobjType AMobjType_tag(char const* obj_type_label);

#endif
