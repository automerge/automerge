#ifndef AUTOMERGE_C_UTILS_STRING_H
#define AUTOMERGE_C_UTILS_STRING_H
/**
 * \file
 * \brief Utility functions for use with `AMbyteSpan` structs that provide
 *        UTF-8 string views.
 */

#include <automerge-c/automerge.h>

/**
 * \memberof AMbyteSpan
 * \brief Returns a pointer to a null-terminated byte string which is a
 *        duplicate of the given UTF-8 string view except for the substitution
 *        of its NUL (0) characters with the specified null-terminated byte
 *        string.
 *        
 * \param[in] str A UTF-8 string view as an `AMbyteSpan` struct.
 * \param[in] nul A null-terminated byte string to substitute for NUL characters
 *                or `NULL` to substitute `"\\0"` for NUL characters.
 * \return A disowned null-terminated byte string.
 * \pre \p str.src `!= NULL`
 * \pre \p str.count `<= sizeof(`\p str.src `)`
 * \warning The returned pointer must be passed to `free()` to avoid a memory
 *          leak. 
 */
char* AMstrdup(AMbyteSpan const str, char const* nul);

#endif /* AUTOMERGE_C_UTILS_STRING_H */
