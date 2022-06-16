#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>

/* third-party */
#include <cmocka.h>

extern int run_doc_tests(void);

extern int run_list_tests(void);

extern int run_map_tests(void);

extern int run_sync_tests(void);

int main(void) {
    return (
        run_doc_tests() +
        run_list_tests() +
        run_map_tests() +
        run_sync_tests()
    );
}
