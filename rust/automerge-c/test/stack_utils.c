#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>

/* third-party */
#include <cmocka.h>

/* local */
#include "cmocka_utils.h"
#include "stack_utils.h"

void cmocka_cb(AMresultStack** stack, uint8_t discriminant) {
    assert_non_null(stack);
    assert_non_null(*stack);
    assert_non_null((*stack)->result);
    if (AMresultStatus((*stack)->result) != AM_STATUS_OK) {
        fail_msg_view("%s", AMerrorMessage((*stack)->result));
    }
    assert_int_equal(AMresultValue((*stack)->result).tag, discriminant);
}

int setup_stack(void** state) {
    *state = NULL;
    return 0;
}

int teardown_stack(void** state) {
    AMresultStack* stack = *state;
    AMfreeStack(&stack);
    return 0;
}
