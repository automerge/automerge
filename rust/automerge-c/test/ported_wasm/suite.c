#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

/* third-party */
#include <cmocka.h>

extern int run_ported_wasm_basic_tests(void);

extern int run_ported_wasm_cursor_tests(void);

extern int run_ported_wasm_sync_tests(void);

int run_ported_wasm_suite(void) {
    return run_ported_wasm_basic_tests() + run_ported_wasm_cursor_tests() + run_ported_wasm_sync_tests();
}
