#ifndef MACRO_UTILS_INCLUDED
#define MACRO_UTILS_INCLUDED

/* local */
#include "automerge.h"

/**
 * \brief Get the `AMvalue` discriminant corresponding to a function name suffix.
 *
 * \param[in] suffix A string.
 * \return An `AMvalue` variant discriminant enum tag.
 */
AMvalueVariant AMvalue_discriminant(char const* suffix);

/**
 * \brief Get the `AMobjType` tag corresponding to a object type label.
 *
 * \param[in] obj_type_label A string.
 * \return An `AMobjType` enum tag.
 */
AMobjType AMobjType_tag(char const* obj_type_label);

#endif
