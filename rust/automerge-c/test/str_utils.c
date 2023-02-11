#include <stdint.h>
#include <stdio.h>

/* local */
#include "str_utils.h"

void hex_to_bytes(char const* hex_str, uint8_t* src, size_t const count) {
    unsigned int byte;
    char const* next = hex_str;
    for (size_t index = 0; *next && index != count; next += 2, ++index) {
        if (sscanf(next, "%02x", &byte) == 1) {
            src[index] = (uint8_t)byte;
        }
    }
}
