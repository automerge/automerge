#include <stdio.h>
#include <stdlib.h>

#include <automerge.h>

AMvalue test(AMresult* result, AMvalueVariant const value_tag) {
    if (result == NULL) {
        fprintf(stderr, "Invalid AMresult struct.");
        exit(-1);
    }
    AMstatus const status = AMresultStatus(result);
    if (status != AM_STATUS_OK) {
        fprintf(stderr, "Unexpected AMstatus enum tag %d.", status);
        exit(-2);
    }
    AMvalue const value = AMresultValue(result, 0);
    if (value.tag != value_tag) {
        fprintf(stderr, "Unexpected AMvalueVariant enum tag %d.", value.tag);
        exit(-3);
    }
    return value;
}

/*
 *  Based on https://automerge.github.io/docs/quickstart
 */
int main(int argc, char** argv) {
    AMdoc* const doc1 = AMalloc();
    AMresult* const cards_result = AMmapPutObject(doc1, AM_ROOT, "cards", AM_OBJ_TYPE_LIST);
    AMvalue value = test(cards_result, AM_VALUE_OBJ_ID);
    AMobjId const* const cards = value.obj_id;
    AMresult* const card1_result = AMlistPutObject(doc1, cards, 0, true, AM_OBJ_TYPE_MAP);
    value = test(card1_result, AM_VALUE_OBJ_ID);
    AMobjId const* const card1 = value.obj_id;
    AMresult* result = AMmapPutStr(doc1, card1, "title", "Rewrite everything in Clojure");
    AMfreeResult(result);
    result = AMmapPutBool(doc1, card1, "done", false);
    AMfreeResult(result);
    AMresult* const card2_result = AMlistPutObject(doc1, cards, 0, true, AM_OBJ_TYPE_MAP);
    value = test(card2_result, AM_VALUE_OBJ_ID);
    AMobjId const* const card2 = value.obj_id;
    result = AMmapPutStr(doc1, card2, "title", "Rewrite everything in Haskell");
    AMfreeResult(result);
    result = AMmapPutBool(doc1, card2, "done", false);
    AMfreeResult(result);
    AMfreeResult(card2_result);
    result = AMcommit(doc1, "Add card", NULL);
    AMfreeResult(result);

    AMdoc* doc2 = AMalloc();
    result = AMmerge(doc2, doc1);
    AMfreeResult(result);
    AMfreeDoc(doc2);

    AMresult* const save_result = AMsave(doc1);
    value = test(save_result, AM_VALUE_BYTES);
    AMbyteSpan binary = value.bytes;
    doc2 = AMalloc();
    AMresult* const load_result = AMload(doc2, binary.src, binary.count);
    AMfreeResult(load_result);
    AMfreeResult(save_result);

    result = AMmapPutBool(doc1, card1, "done", true);
    AMfreeResult(result);
    AMcommit(doc1, "Mark card as done", NULL);
    AMfreeResult(card1_result);

    result = AMlistDelete(doc2, cards, 0);
    AMfreeResult(result);
    AMcommit(doc2, "Delete card", NULL);

    result = AMmerge(doc1, doc2);
    AMfreeResult(result);
    AMfreeDoc(doc2);

    result = AMgetChanges(doc1, NULL);
    value = test(result, AM_VALUE_CHANGES);
    AMchange const* change = NULL;
    while (value.changes.ptr && (change = AMnextChange(&value.changes, 1))) {
        size_t const size = AMobjSizeAt(doc1, cards, change);
        printf("%s %ld\n", AMgetMessage(change), size);
    }
    AMfreeResult(result);
    AMfreeResult(cards_result);
    AMfreeDoc(doc1);
}
