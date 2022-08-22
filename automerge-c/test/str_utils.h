#ifndef STR_UTILS_H
#define STR_UTILS_H

/**
 * \brief Converts a hexadecimal string into a sequence of bytes.
 *
 * \param[in] hex_str A string.
 * \param[in] src A pointer to a contiguous sequence of bytes.
 * \param[in] count The number of bytes to copy to \p src.
 * \pre \p count `<=` length of \p src.
 */
void hex_to_bytes(char const* hex_str, uint8_t* src, size_t const count);

#endif  /* STR_UTILS_H */
