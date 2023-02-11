#ifndef AUTOMERGE_C_UTILS_STRING_H
#define AUTOMERGE_C_UTILS_STRING_H

#include <automerge-c/automerge.h>

/**
 * \memberof AMbyteSpan
 * \brief Compares two UTF-8 string views lexicographically.
 *        
 * \param[in] lhs A UTF-8 string view as an `AMbyteSpan` struct.
 * \param[in] rhs A UTF-8 string view as an `AMbyteSpan` struct.
 * \return Negative value if \p lhs appears before \p rhs in lexicographical order.
 *         Zero if \p lhs and \p rhs compare equal.
 *         Positive value if \p lhs appears after \p rhs in lexicographical order. 
 * \pre \p lhs.src `!= NULL`
 * \pre \p lhs.count `<= sizeof(`\p lhs.src `)`
 * \pre \p rhs.src `!= NULL`
 * \pre \p rhs.count `<= sizeof(`\p rhs.src `)`
 */
int AMstrcmp(AMbyteSpan const lhs, AMbyteSpan const rhs);

/**
 * \memberof AMbyteSpan
 * \brief Returns a pointer to a null-terminated byte string which is a
 *        duplicate of the given UTF-8 string view except for the substitution
 *        of its NUL (`0`) characters with the specified null-terminated byte
 *        string.
 *        
 * \param[in] str A UTF-8 string view as an `AMbyteSpan` struct.
 * \param[in] nul A null-terminated byte string or `NULL` to indicate `"\0"`.
 * \return A disowned null-terminated byte string.
 * \pre \p str.src `!= NULL`
 * \pre \p str.count `<= sizeof(`\p str.src `)`
 * \warning The returned pointer must be passed to `free()` to avoid a memory
 *          leak. 
 */
char* AMstrdup(AMbyteSpan const str, char const* nul);

#endif /* AUTOMERGE_C_UTILS_STRING_H */
