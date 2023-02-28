#include <stdio.h>
#include <stdlib.h>

#include <automerge-c/utils/stack.h>
#include <automerge-c/utils/string.h>

void AMstackFree(AMstack** stack) {
    if (stack) {
        while (*stack) {
            AMresultFree(AMstackPop(stack, NULL));
        }
    }
}

AMresult* AMstackPop(AMstack** stack, const AMresult* result) {
    if (!stack) {
        return NULL;
    }
    AMstack** prev = stack;
    if (result) {
        while (*prev && ((*prev)->result != result)) {
            *prev = (*prev)->prev;
        }
    }
    if (!*prev) {
        return NULL;
    }
    AMstack* target = *prev;
    *prev = target->prev;
    AMresult* popped = target->result;
    free(target);
    return popped;
}

AMresult* AMstackResult(AMstack** stack, AMresult* result, AMstackCallback callback, void* data) {
    if (!stack) {
        if (callback) {
            /* Create a local stack so that the callback can still examine the
             * result. */
            AMstack node = {.result = result, .prev = NULL};
            AMstack* stack = &node;
            callback(&stack, data);
        } else {
            /* \note There is no reason to call this function when both the
             *       stack and the callback are null. */
            fprintf(stderr, "ERROR: NULL AMstackCallback!\n");
        }
        /* \note Nothing can be returned without a stack regardless of
         *       whether or not the callback validated the result. */
        AMresultFree(result);
        return NULL;
    }
    /* Always push the result onto the stack, even if it's null, so that the
     * callback can examine it. */
    AMstack* next = calloc(1, sizeof(AMstack));
    *next = (AMstack){.result = result, .prev = *stack};
    AMstack* top = next;
    *stack = top;
    if (callback) {
        if (!callback(stack, data)) {
            /* The result didn't pass the callback's examination. */
            return NULL;
        }
    } else {
        /* Report an obvious error. */
        if (result) {
            AMbyteSpan const err_msg = AMresultError(result);
            if (err_msg.src && err_msg.count) {
                /* \note The callback may be null because the result is supposed
                 *       to be examined externally so return it despite an
                 *       error. */
                char* const cstr = AMstrdup(err_msg, NULL);
                fprintf(stderr, "WARNING: %s.\n", cstr);
                free(cstr);
            }
        } else {
            /* \note There's no reason to call this function when both the
             *       result and the callback are null. */
            fprintf(stderr, "ERROR: NULL AMresult*!\n");
            return NULL;
        }
    }
    return result;
}

AMitem* AMstackItem(AMstack** stack, AMresult* result, AMstackCallback callback, void* data) {
    AMitems items = AMstackItems(stack, result, callback, data);
    return AMitemsNext(&items, 1);
}

AMitems AMstackItems(AMstack** stack, AMresult* result, AMstackCallback callback, void* data) {
    return (AMstackResult(stack, result, callback, data)) ? AMresultItems(result) : (AMitems){0};
}

size_t AMstackSize(AMstack const* const stack) {
    if (!stack) {
        return 0;
    }
    size_t count = 0;
    AMstack const* prev = stack;
    while (prev) {
        ++count;
        prev = prev->prev;
    }
    return count;
}