#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>

/* third-party */
#include <automerge-c/utils/enum_string.h>
#include <automerge-c/utils/stack_callback_data.h>
#include <automerge-c/utils/string.h>
#include <cmocka.h>

/* local */
#include "cmocka_utils.h"

/**
 * \brief Assert that the given expression is true and report failure in terms
 *        of a line number within a file.
 *
 * \param[in] c An expression.
 * \param[in] file A file's full path string.
 * \param[in] line A line number.
 */
#define assert_true_where(c, file, line) _assert_true(cast_ptr_to_largest_integral_type(c), #c, file, line)

/**
 * \brief Assert that the given pointer is non-NULL and report failure in terms
 *        of a line number within a file.
 *
 * \param[in] c An expression.
 * \param[in] file A file's full path string.
 * \param[in] line A line number.
 */
#define assert_non_null_where(c, file, line) assert_true_where(c, file, line)

/**
 * \brief Forces the test to fail immediately and quit, printing the reason in
 *        terms of a line number within a file.
 *
 * \param[in] msg A message string into which \p str is interpolated.
 * \param[in] str An owned string.
 * \param[in] file A file's full path string.
 * \param[in] line A line number.
 */
#define fail_msg_where(msg, str, file, line)  \
    do {                                      \
        print_error("ERROR: " msg "\n", str); \
        _fail(file, line);                    \
    } while (0)

/**
 * \brief Forces the test to fail immediately and quit, printing the reason in
 *        terms of a line number within a file.
 *
 * \param[in] msg A message string into which \p view.src is interpolated.
 * \param[in] view A UTF-8 string view as an `AMbyteSpan` struct.
 * \param[in] file A file's full path string.
 * \param[in] line A line number.
 */
#define fail_msg_view_where(msg, view, file, line) \
    do {                                           \
        char* const str = AMstrdup(view, NULL);    \
        print_error("ERROR: " msg "\n", str);      \
        free(str);                                 \
        _fail(file, line);                         \
    } while (0)

bool cmocka_cb(AMstack** stack, void* data) {
    assert_non_null(data);
    AMstackCallbackData* const sc_data = (AMstackCallbackData*)data;
    assert_non_null_where(stack, sc_data->file, sc_data->line);
    assert_non_null_where(*stack, sc_data->file, sc_data->line);
    assert_non_null_where((*stack)->result, sc_data->file, sc_data->line);
    if (AMresultStatus((*stack)->result) != AM_STATUS_OK) {
        fail_msg_view_where("%s", AMresultError((*stack)->result), sc_data->file, sc_data->line);
        free(data);
        return false;
    }
    /* Test that the types of all item values are members of the mask. */
    AMitems items = AMresultItems((*stack)->result);
    AMitem* item = NULL;
    while ((item = AMitemsNext(&items, 1)) != NULL) {
        AMvalType const tag = AMitemValType(item);
        if (!(tag & sc_data->bitmask)) {
            fail_msg_where("Unexpected value type `%s`.", AMvalTypeToString(tag), sc_data->file, sc_data->line);
            free(data);
            return false;
        }
    }
    free(data);
    return true;
}
