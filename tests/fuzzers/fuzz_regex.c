#include "rd.h"

#include <stdlib.h>
#include <string.h>
#include <setjmp.h>
#include <stdio.h>

#include "regexp.h"

int LLVMFuzzerTestOneInput(uint8_t *data, size_t size) {
    /* wrap random data in a null-terminated string */
    char *null_terminated = malloc(size+1);
    memcpy(null_terminated, data, size);
    null_terminated[size] = '\0';

    const char *error;
    Reprog *p = re_regcomp(null_terminated, 0, &error);
    if (p != NULL) {
            re_regfree(p);
    }

    /* cleanup */
    free(null_terminated);

    return 0;
}
