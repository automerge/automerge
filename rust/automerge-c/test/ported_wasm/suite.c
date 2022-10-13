#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>

/* third-party */
#include <cmocka.h>

extern int run_ported_wasm_basic_tests(void);

extern int run_ported_wasm_sync_tests(void);

int run_ported_wasm_suite(void) {
    return (
        run_ported_wasm_basic_tests() +
        run_ported_wasm_sync_tests()
    );
}
