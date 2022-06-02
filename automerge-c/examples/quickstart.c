#include <stdio.h>
#include <stdlib.h>

#include <automerge.h>

AMvalue test(AMresult*, AMvalueVariant const);

/*
 *  Based on https://automerge.github.io/docs/quickstart
 */
int main(int argc, char** argv) {
    AMresult* const doc1_result = AMcreate();
    AMdoc* const doc1 = AMresultValue(doc1_result, 0).doc;
    if (doc1 == NULL) {
        fprintf(stderr, "`AMcreate()` failure.");
        exit(EXIT_FAILURE);
    }
    AMresult* const cards_result = AMmapPutObject(doc1, AM_ROOT, "cards", AM_OBJ_TYPE_LIST);
    AMvalue value = test(cards_result, AM_VALUE_OBJ_ID);
    AMobjId const* const cards = value.obj_id;
    AMresult* const card1_result = AMlistPutObject(doc1, cards, 0, true, AM_OBJ_TYPE_MAP);
    value = test(card1_result, AM_VALUE_OBJ_ID);
    AMobjId const* const card1 = value.obj_id;
    AMresult* result = AMmapPutStr(doc1, card1, "title", "Rewrite everything in Clojure");
    test(result, AM_VALUE_VOID);
    AMfree(result);
    result = AMmapPutBool(doc1, card1, "done", false);
    test(result, AM_VALUE_VOID);
    AMfree(result);
    AMresult* const card2_result = AMlistPutObject(doc1, cards, 0, true, AM_OBJ_TYPE_MAP);
    value = test(card2_result, AM_VALUE_OBJ_ID);
    AMobjId const* const card2 = value.obj_id;
    result = AMmapPutStr(doc1, card2, "title", "Rewrite everything in Haskell");
    test(result, AM_VALUE_VOID);
    AMfree(result);
    result = AMmapPutBool(doc1, card2, "done", false);
    test(result, AM_VALUE_VOID);
    AMfree(result);
    AMfree(card2_result);
    result = AMcommit(doc1, "Add card", NULL);
    test(result, AM_VALUE_CHANGE_HASHES);
    AMfree(result);

    AMresult* doc2_result = AMcreate();
    AMdoc* doc2 = AMresultValue(doc2_result, 0).doc;
    if (doc2 == NULL) {
        fprintf(stderr, "`AMcreate()` failure.");
        AMfree(card1_result);
        AMfree(cards_result);
        AMfree(doc1_result);
        exit(EXIT_FAILURE);
    }
    result = AMmerge(doc2, doc1);
    test(result, AM_VALUE_CHANGE_HASHES);
    AMfree(result);
    AMfree(doc2_result);

    AMresult* const save_result = AMsave(doc1);
    value = test(save_result, AM_VALUE_BYTES);
    AMbyteSpan binary = value.bytes;
    doc2_result = AMload(binary.src, binary.count);
    doc2 = AMresultValue(doc2_result, 0).doc;
    AMfree(save_result);
    if (doc2 == NULL) {
        fprintf(stderr, "`AMload()` failure.");
        AMfree(card1_result);
        AMfree(cards_result);
        AMfree(doc1_result);
        exit(EXIT_FAILURE);
    }

    result = AMmapPutBool(doc1, card1, "done", true);
    test(result, AM_VALUE_VOID);
    AMfree(result);
    result = AMcommit(doc1, "Mark card as done", NULL);
    test(result, AM_VALUE_CHANGE_HASHES);
    AMfree(result);
    AMfree(card1_result);

    result = AMlistDelete(doc2, cards, 0);
    test(result, AM_VALUE_VOID);
    AMfree(result);
    result = AMcommit(doc2, "Delete card", NULL);
    test(result, AM_VALUE_CHANGE_HASHES);
    AMfree(result);

    result = AMmerge(doc1, doc2);
    test(result, AM_VALUE_CHANGE_HASHES);
    AMfree(result);
    AMfree(doc2_result);

    result = AMgetChanges(doc1, NULL);
    value = test(result, AM_VALUE_CHANGES);
    AMchange const* change = NULL;
    while (value.changes.ptr && (change = AMchangesNext(&value.changes, 1))) {
        size_t const size = AMobjSizeAt(doc1, cards, change);
        printf("%s %ld\n", AMchangeMessage(change), size);
    }
    AMfree(result);
    AMfree(cards_result);
    AMfree(doc1_result);
}

/**
 * \brief Extracts a value with the given discriminant from the given result
 *        or writes a message to `stderr`, frees the given result and
 *        terminates the program.
 *
.* \param[in] result A pointer to an `AMresult` struct.
 * \param[in] discriminant An `AMvalueVariant` enum tag.
 * \return An `AMvalue` struct.
 * \pre \p result must be a valid address.
 */
AMvalue test(AMresult* result, AMvalueVariant const discriminant) {
    static char prelude[64];

    if (result == NULL) {
        fprintf(stderr, "NULL `AMresult` struct pointer.");
        exit(EXIT_FAILURE);
    }
    AMstatus const status = AMresultStatus(result);
    if (status != AM_STATUS_OK) {
        switch (status) {
            case AM_STATUS_ERROR:          sprintf(prelude, "Error");          break;
            case AM_STATUS_INVALID_RESULT: sprintf(prelude, "Invalid result"); break;
            default: sprintf(prelude, "Unknown `AMstatus` tag %d", status);
        }
        fprintf(stderr, "%s; %s.", prelude, AMerrorMessage(result));
        AMfree(result);
        exit(EXIT_FAILURE);
    }
    AMvalue const value = AMresultValue(result, 0);
    if (value.tag != discriminant) {
        char const* label = NULL;
        switch (value.tag) {
            case AM_VALUE_ACTOR_ID:      label = "AM_VALUE_ACTOR_ID";      break;
            case AM_VALUE_BOOLEAN:       label = "AM_VALUE_BOOLEAN";       break;
            case AM_VALUE_BYTES:         label = "AM_VALUE_BYTES";         break;
            case AM_VALUE_CHANGE_HASHES: label = "AM_VALUE_CHANGE_HASHES"; break;
            case AM_VALUE_CHANGES:       label = "AM_VALUE_CHANGES";       break;
            case AM_VALUE_COUNTER:       label = "AM_VALUE_COUNTER";       break;
            case AM_VALUE_F64:           label = "AM_VALUE_F64";           break;
            case AM_VALUE_INT:           label = "AM_VALUE_INT";           break;
            case AM_VALUE_VOID:          label = "AM_VALUE_VOID";          break;
            case AM_VALUE_NULL:          label = "AM_VALUE_NULL";          break;
            case AM_VALUE_OBJ_ID:        label = "AM_VALUE_OBJ_ID";        break;
            case AM_VALUE_STR:           label = "AM_VALUE_STR";           break;
            case AM_VALUE_TIMESTAMP:     label = "AM_VALUE_TIMESTAMP";     break;
            case AM_VALUE_UINT:          label = "AM_VALUE_UINT";          break;
            default:                     label = "<unknown>";
        }
        fprintf(stderr, "Unexpected `AMvalueVariant` tag `%s` (%d).", label, value.tag);
        AMfree(result);
        exit(EXIT_FAILURE);
    }
    return value;
}
