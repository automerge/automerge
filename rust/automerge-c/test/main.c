#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

/* third-party */
#include <cmocka.h>

extern int run_actor_id_tests(void);

extern int run_byte_span_tests(void);

extern int run_cursor_tests(void);

extern int run_doc_tests(void);

extern int run_enum_string_tests(void);

extern int run_item_tests(void);

extern int run_list_tests(void);

extern int run_map_tests(void);

extern int run_mark_tests(void);

extern int run_ported_wasm_suite(void);

int main(void) {
    return run_actor_id_tests() + run_byte_span_tests() + run_cursor_tests() + run_doc_tests() +
           run_enum_string_tests() + run_item_tests() + run_list_tests() + run_map_tests() + run_mark_tests() +
           run_ported_wasm_suite();
}
