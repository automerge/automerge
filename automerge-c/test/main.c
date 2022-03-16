#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>

/* third-party */
#include <cmocka.h>

extern int run_AMdoc_property_tests(void);

extern int run_AMlistSet_tests(void);

extern int run_AMmapSet_tests(void);

int main(void) {
    return (
        run_AMdoc_property_tests() +
        run_AMlistSet_tests() +
        run_AMmapSet_tests()
    );
}
