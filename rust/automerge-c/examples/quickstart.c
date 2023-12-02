#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <automerge-c/automerge.h>
#include <automerge-c/utils/enum_string.h>
#include <automerge-c/utils/stack.h>
#include <automerge-c/utils/stack_callback_data.h>
#include <automerge-c/utils/string.h>

static bool abort_cb(AMstack**, void*);

/**
 * \brief Based on https://automerge.github.io/docs/quickstart
 */
int main(int argc, char** argv) {
    AMstack* stack = NULL;
    AMdoc* doc1;
    AMitemToDoc(AMstackItem(&stack, AMcreate(NULL), abort_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc1);
    AMobjId const* const cards =
        AMitemObjId(AMstackItem(&stack, AMmapPutObject(doc1, AM_ROOT, AMstr("cards"), AM_OBJ_TYPE_LIST), abort_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    AMobjId const* const card1 =
        AMitemObjId(AMstackItem(&stack, AMlistPutObject(doc1, cards, SIZE_MAX, true, AM_OBJ_TYPE_MAP), abort_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    AMstackItem(NULL, AMmapPutStr(doc1, card1, AMstr("title"), AMstr("Rewrite everything in Clojure")), abort_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutBool(doc1, card1, AMstr("done"), false), abort_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMobjId const* const card2 =
        AMitemObjId(AMstackItem(&stack, AMlistPutObject(doc1, cards, SIZE_MAX, true, AM_OBJ_TYPE_MAP), abort_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    AMstackItem(NULL, AMmapPutStr(doc1, card2, AMstr("title"), AMstr("Rewrite everything in Haskell")), abort_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutBool(doc1, card2, AMstr("done"), false), abort_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(doc1, AMstr("Add card"), NULL), abort_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    AMdoc* doc2;
    AMitemToDoc(AMstackItem(&stack, AMcreate(NULL), abort_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc2);
    AMstackItem(NULL, AMmerge(doc2, doc1), abort_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    AMbyteSpan binary;
    AMitemToBytes(AMstackItem(&stack, AMsave(doc1), abort_cb, AMexpect(AM_VAL_TYPE_BYTES)), &binary);
    AMitemToDoc(AMstackItem(&stack, AMload(binary.src, binary.count), abort_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc2);

    AMstackItem(NULL, AMmapPutBool(doc1, card1, AMstr("done"), true), abort_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(doc1, AMstr("Mark card as done"), NULL), abort_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    AMstackItem(NULL, AMlistDelete(doc2, cards, 0), abort_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(doc2, AMstr("Delete card"), NULL), abort_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    AMstackItem(NULL, AMmerge(doc1, doc2), abort_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    AMitems changes = AMstackItems(&stack, AMgetChanges(doc1, NULL), abort_cb, AMexpect(AM_VAL_TYPE_CHANGE));
    AMitem* item = NULL;
    while ((item = AMitemsNext(&changes, 1)) != NULL) {
        AMchange* change;
        AMitemToChange(item, &change);
        AMitems const heads = AMstackItems(&stack, AMitemFromChangeHash(AMchangeHash(change)), abort_cb,
                                           AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        char* const c_msg = AMstrdup(AMchangeMessage(change), NULL);
        printf("%s %zu\n", c_msg, AMobjSize(doc1, cards, &heads));
        free(c_msg);
    }
    AMstackFree(&stack);
}

/**
 * \brief Examines the result at the top of the given stack and, if it's
 *        invalid, prints an error message to `stderr`, deallocates all results
 *        in the stack and exits.
 *
 * \param[in,out] stack A pointer to a pointer to an `AMstack` struct.
 * \param[in] data A pointer to an owned `AMstackCallbackData` struct or `NULL`.
 * \return `true` if the top `AMresult` in \p stack is valid, `false` otherwise.
 * \pre \p stack `!= NULL`.
 */
static bool abort_cb(AMstack** stack, void* data) {
    static char buffer[512] = {0};

    char const* suffix = NULL;
    if (!stack) {
        suffix = "stack*";
    } else if (!*stack) {
        suffix = "stack";
    } else if (!(*stack)->result) {
        suffix = "result";
    }
    if (suffix) {
        fprintf(stderr, "Null `AM%s*`.\n", suffix);
        free(data);
        AMstackFree(stack);
        exit(EXIT_FAILURE);
        return false;
    }
    AMstatus const status = AMresultStatus((*stack)->result);
    switch (status) {
        case AM_STATUS_ERROR:
            strcpy(buffer, "Error");
            break;
        case AM_STATUS_INVALID_RESULT:
            strcpy(buffer, "Invalid result");
            break;
        case AM_STATUS_OK:
            break;
        default:
            sprintf(buffer, "Unknown `AMstatus` tag %d", status);
    }
    if (buffer[0]) {
        char* const c_msg = AMstrdup(AMresultError((*stack)->result), NULL);
        fprintf(stderr, "%s; %s.\n", buffer, c_msg);
        free(c_msg);
        free(data);
        AMstackFree(stack);
        exit(EXIT_FAILURE);
        return false;
    }
    if (data) {
        AMstackCallbackData* sc_data = (AMstackCallbackData*)data;
        AMvalType const tag = AMitemValType(AMresultItem((*stack)->result));
        if (tag != sc_data->bitmask) {
            fprintf(stderr, "Unexpected tag `%s` (%d) instead of `%s` at %s:%d.\n", AMvalTypeToString(tag), tag,
                    AMvalTypeToString(sc_data->bitmask), sc_data->file, sc_data->line);
            free(data);
            AMstackFree(stack);
            exit(EXIT_FAILURE);
            return false;
        }
    }
    free(data);
    return true;
}
