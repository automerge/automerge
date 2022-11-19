#ifndef CMOCKA_UTILS_H
#define CMOCKA_UTILS_H

#include <string.h>

/* third-party */
#include <cmocka.h>

/**
 * \brief Forces the test to fail immediately and quit, printing the reason.
 *
 * \param[in] view A string view as an `AMbyteSpan` struct.
 */
#define fail_msg_view(msg, view) do { \
    char* const c_str = test_calloc(1, view.count + 1); \
    strncpy(c_str, view.src, view.count); \
    print_error(msg, c_str); \
    test_free(c_str); \
    fail(); \
} while (0)

#endif  /* CMOCKA_UTILS_H */
