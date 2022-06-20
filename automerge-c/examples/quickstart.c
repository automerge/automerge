#include <stdio.h>
#include <stdlib.h>

#include <automerge.h>

typedef struct StackNode ResultStack;

AMvalue push(ResultStack**, AMresult*, AMvalueVariant const);

size_t free_results(ResultStack*);

/*
 *  Based on https://automerge.github.io/docs/quickstart
 */
int main(int argc, char** argv) {
    ResultStack* results = NULL;
    AMdoc* const doc1 = push(&results, AMcreate(), AM_VALUE_DOC).doc;
    AMobjId const* const
        cards = push(&results, AMmapPutObject(doc1, AM_ROOT, "cards", AM_OBJ_TYPE_LIST), AM_VALUE_OBJ_ID).obj_id;
    AMobjId const* const
        card1 = push(&results, AMlistPutObject(doc1, cards, 0, true, AM_OBJ_TYPE_MAP), AM_VALUE_OBJ_ID).obj_id;
    push(&results, AMmapPutStr(doc1, card1, "title", "Rewrite everything in Clojure"), AM_VALUE_VOID);
    push(&results, AMmapPutBool(doc1, card1, "done", false), AM_VALUE_VOID);
    AMobjId const* const
        card2 = push(&results, AMlistPutObject(doc1, cards, 0, true, AM_OBJ_TYPE_MAP), AM_VALUE_OBJ_ID).obj_id;
    push(&results, AMmapPutStr(doc1, card2, "title", "Rewrite everything in Haskell"), AM_VALUE_VOID);
    push(&results, AMmapPutBool(doc1, card2, "done", false), AM_VALUE_VOID);
    push(&results, AMcommit(doc1, "Add card", NULL), AM_VALUE_CHANGE_HASHES);

    AMdoc* doc2 = push(&results, AMcreate(), AM_VALUE_DOC).doc;
    push(&results, AMmerge(doc2, doc1), AM_VALUE_CHANGE_HASHES);

    AMbyteSpan const binary = push(&results, AMsave(doc1), AM_VALUE_BYTES).bytes;
    doc2 = push(&results, AMload(binary.src, binary.count), AM_VALUE_DOC).doc;

    push(&results, AMmapPutBool(doc1, card1, "done", true), AM_VALUE_VOID);
    push(&results, AMcommit(doc1, "Mark card as done", NULL), AM_VALUE_CHANGE_HASHES);

    push(&results, AMlistDelete(doc2, cards, 0), AM_VALUE_VOID);
    push(&results, AMcommit(doc2, "Delete card", NULL), AM_VALUE_CHANGE_HASHES);

    push(&results, AMmerge(doc1, doc2), AM_VALUE_CHANGE_HASHES);

    AMchanges changes = push(&results, AMgetChanges(doc1, NULL), AM_VALUE_CHANGES).changes;
    AMchange const* change = NULL;
    while ((change = AMchangesNext(&changes, 1)) != NULL) {
        AMbyteSpan const change_hash = AMchangeHash(change);
        AMchangeHashes const
            heads = push(&results, AMchangeHashesInit(&change_hash, 1), AM_VALUE_CHANGE_HASHES).change_hashes;
        printf("%s %ld\n", AMchangeMessage(change), AMobjSize(doc1, cards, &heads));
    }
    free_results(results);
}

/**
 * \brief A node in a singly-linked list of `AMresult` struct pointers.
 */
struct StackNode {
    AMresult* result;
    struct StackNode* next;
};

/**
 * \brief Pushes the given result onto the given stack and then either gets the
 *        value matching the given discriminant from that result or, failing
 *        that, prints an error message to `stderr`, frees all results in that
 *        stack and aborts.
 *
 * \param[in] stack A pointer to a pointer to a `ResultStack` struct.
.* \param[in] result A pointer to an `AMresult` struct.
 * \param[in] discriminant An `AMvalueVariant` enum tag.
 * \return An `AMvalue` struct.
 * \pre \p stack must be a valid address.
 * \pre \p result must be a valid address.
 */
AMvalue push(ResultStack** stack, AMresult* result, AMvalueVariant const discriminant) {
    static char prelude[64];

    if (stack == NULL) {
        fprintf(stderr, "Null `ResultStack` struct pointer pointer; previous "
                        "`AMresult` structs may have leaked!");
        AMfree(result);
        exit(EXIT_FAILURE);
    }
    if (result == NULL) {
        fprintf(stderr, "Null `AMresult` struct pointer.");
        free_results(*stack);
        exit(EXIT_FAILURE);
    }
    /* Push the result onto the stack. */
    struct StackNode* top = malloc(sizeof(struct StackNode));
    top->result = result;
    top->next = *stack;
    *stack = top;
    AMstatus const status = AMresultStatus(result);
    if (status != AM_STATUS_OK) {
        switch (status) {
            case AM_STATUS_ERROR:          sprintf(prelude, "Error");          break;
            case AM_STATUS_INVALID_RESULT: sprintf(prelude, "Invalid result"); break;
            default: sprintf(prelude, "Unknown `AMstatus` tag %d", status);
        }
        fprintf(stderr, "%s; %s.", prelude, AMerrorMessage(result));
        free_results(*stack);
        exit(EXIT_FAILURE);
    }
    AMvalue const value = AMresultValue(result);
    if (value.tag != discriminant) {
        char const* label = NULL;
        switch (value.tag) {
            case AM_VALUE_ACTOR_ID:      label = "ACTOR_ID";      break;
            case AM_VALUE_BOOLEAN:       label = "BOOLEAN";       break;
            case AM_VALUE_BYTES:         label = "BYTES";         break;
            case AM_VALUE_CHANGE_HASHES: label = "CHANGE_HASHES"; break;
            case AM_VALUE_CHANGES:       label = "CHANGES";       break;
            case AM_VALUE_COUNTER:       label = "COUNTER";       break;
            case AM_VALUE_DOC:           label = "DOC";           break;
            case AM_VALUE_F64:           label = "F64";           break;
            case AM_VALUE_INT:           label = "INT";           break;
            case AM_VALUE_NULL:          label = "NULL";          break;
            case AM_VALUE_OBJ_ID:        label = "OBJ_ID";        break;
            case AM_VALUE_STR:           label = "STR";           break;
            case AM_VALUE_STRINGS:       label = "STRINGS";       break;
            case AM_VALUE_TIMESTAMP:     label = "TIMESTAMP";     break;
            case AM_VALUE_UINT:          label = "UINT";          break;
            case AM_VALUE_SYNC_MESSAGE:  label = "SYNC_MESSAGE";  break;
            case AM_VALUE_SYNC_STATE:    label = "SYNC_STATE";    break;
            case AM_VALUE_VOID:          label = "VOID";          break;
            default:                     label = "...";
        }
        fprintf(stderr, "Unexpected `AMvalueVariant` tag `AM_VALUE_%s` (%d).", label, value.tag);
        free_results(*stack);
        exit(EXIT_FAILURE);
    }
    return value;
}

/**
 * \brief Frees a stack of `AMresult` structs.
 *
 * \param[in] stack A pointer to a `ResultStack` struct.
 * \return The number of stack nodes freed.
 * \pre \p stack must be a valid address.
 */
size_t free_results(ResultStack* stack) {
    struct StackNode* prev = NULL;
    size_t count = 0;
    for (struct StackNode* node = stack; node; node = node->next, ++count) {
        free(prev);
        AMfree(node->result);
        prev = node;
    }
    free(prev);
    return count;
}
