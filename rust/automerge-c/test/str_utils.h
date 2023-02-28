#ifndef TESTS_STR_UTILS_H
#define TESTS_STR_UTILS_H

/**
 * \brief Converts a hexadecimal string into an array of bytes.
 *
 * \param[in] hex_str A hexadecimal string.
 * \param[in] src A pointer to an array of bytes.
 * \param[in] count The count of bytes to copy into the array pointed to by
 *                  \p src.
 * \pre \p src `!= NULL`
 * \pre `sizeof(`\p src `) > 0`
 * \pre \p count `<= sizeof(`\p src `)`
 */
void hex_to_bytes(char const* hex_str, uint8_t* src, size_t const count);

#endif /* TESTS_STR_UTILS_H */
