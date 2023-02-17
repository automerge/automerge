#include <setjmp.h>
#include <stdarg.h>
#include <stdlib.h>

/* third-party */
#include <cmocka.h>

/* local */
#include <automerge-c/utils/stack_callback_data.h>
#include "cmocka_utils.h"
#include "doc_state.h"

int setup_doc(void** state) {
    DocState* doc_state = test_calloc(1, sizeof(DocState));
    setup_base((void**)&doc_state->base_state);
    AMitemToDoc(AMstackItem(&doc_state->base_state->stack, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)),
                &doc_state->doc);
    *state = doc_state;
    return 0;
}

int teardown_doc(void** state) {
    DocState* doc_state = *state;
    teardown_base((void**)&doc_state->base_state);
    test_free(doc_state);
    return 0;
}
