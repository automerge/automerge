#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <automerge.h>

static void abort_cb(AMresultStack**, uint8_t);

/*
 *  Based on https://automerge.github.io/docs/quickstart
 */
int main(int argc, char** argv) {
    AMresultStack* results = NULL;
    AMdoc* const doc1 = AMpush(&results, AMcreate(), AM_VALUE_DOC, abort_cb).doc;
    AMobjId const* const
        cards = AMpush(&results, AMmapPutObject(doc1, AM_ROOT, "cards", AM_OBJ_TYPE_LIST), AM_VALUE_OBJ_ID, abort_cb).obj_id;
    AMobjId const* const
        card1 = AMpush(&results, AMlistPutObject(doc1, cards, 0, true, AM_OBJ_TYPE_MAP), AM_VALUE_OBJ_ID, abort_cb).obj_id;
    AMpush(&results, AMmapPutStr(doc1, card1, "title", "Rewrite everything in Clojure"), AM_VALUE_VOID, abort_cb);
    AMpush(&results, AMmapPutBool(doc1, card1, "done", false), AM_VALUE_VOID, abort_cb);
    AMobjId const* const
        card2 = AMpush(&results, AMlistPutObject(doc1, cards, 0, true, AM_OBJ_TYPE_MAP), AM_VALUE_OBJ_ID, abort_cb).obj_id;
    AMpush(&results, AMmapPutStr(doc1, card2, "title", "Rewrite everything in Haskell"), AM_VALUE_VOID, abort_cb);
    AMpush(&results, AMmapPutBool(doc1, card2, "done", false), AM_VALUE_VOID, abort_cb);
    AMpush(&results, AMcommit(doc1, "Add card", NULL), AM_VALUE_CHANGE_HASHES, abort_cb);

    AMdoc* doc2 = AMpush(&results, AMcreate(), AM_VALUE_DOC, abort_cb).doc;
    AMpush(&results, AMmerge(doc2, doc1), AM_VALUE_CHANGE_HASHES, abort_cb);

    AMbyteSpan const binary = AMpush(&results, AMsave(doc1), AM_VALUE_BYTES, abort_cb).bytes;
    doc2 = AMpush(&results, AMload(binary.src, binary.count), AM_VALUE_DOC, abort_cb).doc;

    AMpush(&results, AMmapPutBool(doc1, card1, "done", true), AM_VALUE_VOID, abort_cb);
    AMpush(&results, AMcommit(doc1, "Mark card as done", NULL), AM_VALUE_CHANGE_HASHES, abort_cb);

    AMpush(&results, AMlistDelete(doc2, cards, 0), AM_VALUE_VOID, abort_cb);
    AMpush(&results, AMcommit(doc2, "Delete card", NULL), AM_VALUE_CHANGE_HASHES, abort_cb);

    AMpush(&results, AMmerge(doc1, doc2), AM_VALUE_CHANGE_HASHES, abort_cb);

    AMchanges changes = AMpush(&results, AMgetChanges(doc1, NULL), AM_VALUE_CHANGES, abort_cb).changes;
    AMchange const* change = NULL;
    while ((change = AMchangesNext(&changes, 1)) != NULL) {
        AMbyteSpan const change_hash = AMchangeHash(change);
        AMchangeHashes const
            heads = AMpush(&results, AMchangeHashesInit(&change_hash, 1), AM_VALUE_CHANGE_HASHES, abort_cb).change_hashes;
        printf("%s %ld\n", AMchangeMessage(change), AMobjSize(doc1, cards, &heads));
    }
    AMfreeStack(&results);
}

static char const* discriminant_suffix(AMvalueVariant const);

/**
 * \brief Prints an error message to `stderr`, deallocates all results in the
 *        given stack and exits.
 *
 * \param[in,out] stack A pointer to a pointer to an `AMresultStack` struct.
 * \param[in] discriminant An `AMvalueVariant` enum tag.
 * \pre \p stack` != NULL`.
 * \post `*stack == NULL`.
 */
static void abort_cb(AMresultStack** stack, uint8_t discriminant) {
    static char buffer[512] = {0};

    char const* suffix = NULL;
    if (!stack) {
        suffix = "Stack*";
    }
    else if (!*stack) {
        suffix = "Stack";
    }
    else if (!(*stack)->result) {
        suffix = "";
    }
    if (suffix) {
        fprintf(stderr, "Null `AMresult%s*`.", suffix);
        AMfreeStack(stack);
        exit(EXIT_FAILURE);
        return;
    }
    AMstatus const status = AMresultStatus((*stack)->result);
    switch (status) {
        case AM_STATUS_ERROR:          strcpy(buffer, "Error");          break;
        case AM_STATUS_INVALID_RESULT: strcpy(buffer, "Invalid result"); break;
        case AM_STATUS_OK:                                               break;
        default: sprintf(buffer, "Unknown `AMstatus` tag %d", status);
    }
    if (buffer[0]) {
        fprintf(stderr, "%s; %s.", buffer, AMerrorMessage((*stack)->result));
        AMfreeStack(stack);
        exit(EXIT_FAILURE);
        return;
    }
    AMvalue const value = AMresultValue((*stack)->result);
    fprintf(stderr, "Unexpected tag `AM_VALUE_%s` (%d); expected `AM_VALUE_%s`.",
        discriminant_suffix(value.tag),
        value.tag,
        discriminant_suffix(discriminant));
    AMfreeStack(stack);
    exit(EXIT_FAILURE);
}

/**
 * \brief Gets the suffix for a discriminant's corresponding string
 *        representation.
 *
 * \param[in] discriminant An `AMvalueVariant` enum tag.
 * \return A UTF-8 string.
 */
static char const* discriminant_suffix(AMvalueVariant const discriminant) {
    char const* suffix = NULL;
    switch (discriminant) {
        case AM_VALUE_ACTOR_ID:      suffix = "ACTOR_ID";      break;
        case AM_VALUE_BOOLEAN:       suffix = "BOOLEAN";       break;
        case AM_VALUE_BYTES:         suffix = "BYTES";         break;
        case AM_VALUE_CHANGE_HASHES: suffix = "CHANGE_HASHES"; break;
        case AM_VALUE_CHANGES:       suffix = "CHANGES";       break;
        case AM_VALUE_COUNTER:       suffix = "COUNTER";       break;
        case AM_VALUE_DOC:           suffix = "DOC";           break;
        case AM_VALUE_F64:           suffix = "F64";           break;
        case AM_VALUE_INT:           suffix = "INT";           break;
        case AM_VALUE_LIST_ITEMS:    suffix = "LIST_ITEMS";    break;
        case AM_VALUE_MAP_ITEMS:     suffix = "MAP_ITEMS";     break;
        case AM_VALUE_NULL:          suffix = "NULL";          break;
        case AM_VALUE_OBJ_ID:        suffix = "OBJ_ID";        break;
        case AM_VALUE_OBJ_ITEMS:     suffix = "OBJ_ITEMS";     break;
        case AM_VALUE_STR:           suffix = "STR";           break;
        case AM_VALUE_STRS:          suffix = "STRINGS";       break;
        case AM_VALUE_SYNC_MESSAGE:  suffix = "SYNC_MESSAGE";  break;
        case AM_VALUE_SYNC_STATE:    suffix = "SYNC_STATE";    break;
        case AM_VALUE_TIMESTAMP:     suffix = "TIMESTAMP";     break;
        case AM_VALUE_UINT:          suffix = "UINT";          break;
        case AM_VALUE_VOID:          suffix = "VOID";          break;
        default:                     suffix = "...";
    }
    return suffix;
}
