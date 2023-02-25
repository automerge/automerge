#ifndef TESTS_CMOCKA_UTILS_H
#define TESTS_CMOCKA_UTILS_H

#include <stdlib.h>
#include <string.h>

/* third-party */
#include <automerge-c/utils/string.h>
#include <cmocka.h>

/* local */
#include "base_state.h"

/**
 * \brief Forces the test to fail immediately and quit, printing the reason.
 *
 * \param[in] msg A message string into which \p view.src is interpolated.
 * \param[in] view A UTF-8 string view as an `AMbyteSpan` struct.
 */
#define fail_msg_view(msg, view)                  \
    do {                                          \
        char* const c_str = AMstrdup(view, NULL); \
        print_error("ERROR: " msg "\n", c_str);   \
        free(c_str);                              \
        fail();                                   \
    } while (0)

/**
 * \brief Validates the top result in a stack based upon the parameters
 *        specified within the given data structure and reports violations
 *        using cmocka assertions.
 *
 * \param[in,out] stack A pointer to a pointer to an `AMstack` struct.
 * \param[in] data A pointer to an owned `AMpushData` struct.
 * \return `true` if the top `AMresult` struct in \p stack is valid, `false`
 *         otherwise.
 * \pre \p stack `!= NULL`.
 * \pre \p data `!= NULL`.
 */
bool cmocka_cb(AMstack** stack, void* data);

#endif /* TESTS_CMOCKA_UTILS_H */
