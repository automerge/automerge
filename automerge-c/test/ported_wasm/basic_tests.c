#include <float.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* third-party */
#include <cmocka.h>

/* local */
#include "automerge.h"
#include "../stack_utils.h"

/**
 * \brief default import init() should return a promise
 */
static void test_default_import_init_should_return_a_promise(void** state);

/**
 * \brief should create, clone and free
 */
static void test_create_clone_and_free(void** state) {
    AMresultStack* stack = *state;
    /* const doc1 = create()                                                 */
    AMdoc* const doc1 = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    /* const doc2 = doc1.clone()                                             */
    AMdoc* const doc2 = AMpush(&stack, AMclone(doc1), AM_VALUE_DOC, cmocka_cb).doc;
}

/**
 * \brief should be able to start and commit
 */
static void test_start_and_commit(void** state) {
    AMresultStack* stack = *state;
    /* const doc = create()                                                  */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    /* doc.commit()                                                          */
    AMpush(&stack, AMcommit(doc, NULL, NULL), AM_VALUE_CHANGE_HASHES, cmocka_cb);
}

/**
 * \brief getting a nonexistent prop does not throw an error
 */
static void test_getting_a_nonexistent_prop_does_not_throw_an_error(void** state) {
    AMresultStack* stack = *state;
    /* const doc = create()                                                  */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    /* const root = "_root"                                                  */
    /* const result = doc.getWithType(root, "hello")                         */
    /* assert.deepEqual(result, undefined)                                   */
    AMpush(&stack,
           AMmapGet(doc, AM_ROOT, "hello", NULL),
           AM_VALUE_VOID,
           cmocka_cb);
}

/**
 * \brief should be able to set and get a simple value
 */
static void test_should_be_able_to_set_and_get_a_simple_value(void** state) {
    AMresultStack* stack = *state;
    /* const doc: Automerge = create("aabbcc")                               */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMsetActorId(doc, AMpush(&stack,
                                    AMactorIdInitStr("aabbcc"),
                                    AM_VALUE_ACTOR_ID,
                                    cmocka_cb).actor_id));
    /* const root = "_root"                                                  */
    /* let result                                                            */
    /*                                                                       */
    /* doc.put(root, "hello", "world")                                       */
    AMfree(AMmapPutStr(doc, AM_ROOT, "hello", "world"));
    /* doc.put(root, "number1", 5, "uint")                                   */
    AMfree(AMmapPutUint(doc, AM_ROOT, "number1", 5));
    /* doc.put(root, "number2", 5)                                           */
    AMfree(AMmapPutInt(doc, AM_ROOT, "number2", 5));
    /* doc.put(root, "number3", 5.5)                                         */
    AMfree(AMmapPutF64(doc, AM_ROOT, "number3", 5.5));
    /* doc.put(root, "number4", 5.5, "f64")                                  */
    AMfree(AMmapPutF64(doc, AM_ROOT, "number4", 5.5));
    /* doc.put(root, "number5", 5.5, "int")                                  */
    AMfree(AMmapPutInt(doc, AM_ROOT, "number5", 5.5));
    /* doc.put(root, "bool", true)                                           */
    AMfree(AMmapPutBool(doc, AM_ROOT, "bool", true));
    /* doc.put(root, "time1", 1000, "timestamp")                             */
    AMfree(AMmapPutTimestamp(doc, AM_ROOT, "time1", 1000));
    /* doc.put(root, "time2", new Date(1001))                                */
    AMfree(AMmapPutTimestamp(doc, AM_ROOT, "time2", 1001));
    /* doc.putObject(root, "list", []);                                      */
    AMfree(AMmapPutObject(doc, AM_ROOT, "list", AM_OBJ_TYPE_LIST));
    /* doc.put(root, "null", null)                                           */
    AMfree(AMmapPutNull(doc, AM_ROOT, "null"));
    /*                                                                       */
    /* result = doc.getWithType(root, "hello")                               */
    /* assert.deepEqual(result, ["str", "world"])                            */
    /* assert.deepEqual(doc.get("/", "hello"), "world")                      */
    assert_string_equal(AMpush(&stack,
                               AMmapGet(doc, AM_ROOT, "hello", NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "world");
    /* assert.deepEqual(doc.get("/", "hello"), "world")                      */
    /*                                                                       */
    /* result = doc.getWithType(root, "number1")                             */
    /* assert.deepEqual(result, ["uint", 5])                                 */
    assert_int_equal(AMpush(&stack,
                            AMmapGet(doc, AM_ROOT, "number1", NULL),
                            AM_VALUE_UINT,
                            cmocka_cb).uint, 5);
    /* assert.deepEqual(doc.get("/", "number1"), 5)                          */
    /*                                                                       */
    /* result = doc.getWithType(root, "number2")                             */
    /* assert.deepEqual(result, ["int", 5])                                  */
    assert_int_equal(AMpush(&stack,
                            AMmapGet(doc, AM_ROOT, "number2", NULL),
                            AM_VALUE_INT,
                            cmocka_cb).int_, 5);
    /*                                                                       */
    /* result = doc.getWithType(root, "number3")                             */
    /* assert.deepEqual(result, ["f64", 5.5])                                */
    assert_float_equal(AMpush(&stack,
                              AMmapGet(doc, AM_ROOT, "number3", NULL),
                              AM_VALUE_F64,
                              cmocka_cb).f64, 5.5, DBL_EPSILON);
    /*                                                                       */
    /* result = doc.getWithType(root, "number4")                             */
    /* assert.deepEqual(result, ["f64", 5.5])                                */
    assert_float_equal(AMpush(&stack,
                              AMmapGet(doc, AM_ROOT, "number4", NULL),
                              AM_VALUE_F64,
                              cmocka_cb).f64, 5.5, DBL_EPSILON);
    /*                                                                       */
    /* result = doc.getWithType(root, "number5")                             */
    /* assert.deepEqual(result, ["int", 5])                                  */
    assert_int_equal(AMpush(&stack,
                            AMmapGet(doc, AM_ROOT, "number5", NULL),
                            AM_VALUE_INT,
                            cmocka_cb).int_, 5);
    /*                                                                       */
    /* result = doc.getWithType(root, "bool")                                */
    /* assert.deepEqual(result, ["boolean", true])                           */
    assert_int_equal(AMpush(&stack,
                            AMmapGet(doc, AM_ROOT, "bool", NULL),
                            AM_VALUE_BOOLEAN,
                            cmocka_cb).boolean, true);
    /*                                                                       */
    /* doc.put(root, "bool", false, "boolean")                               */
    AMfree(AMmapPutBool(doc, AM_ROOT, "bool", false));
    /*                                                                       */
    /* result = doc.getWithType(root, "bool")                                */
    /* assert.deepEqual(result, ["boolean", false])                          */
    assert_int_equal(AMpush(&stack,
                            AMmapGet(doc, AM_ROOT, "bool", NULL),
                            AM_VALUE_BOOLEAN,
                            cmocka_cb).boolean, false);
    /*                                                                       */
    /* result = doc.getWithType(root, "time1")                               */
    /* assert.deepEqual(result, ["timestamp", new Date(1000)])               */
    assert_int_equal(AMpush(&stack,
                            AMmapGet(doc, AM_ROOT, "time1", NULL),
                            AM_VALUE_TIMESTAMP,
                            cmocka_cb).timestamp, 1000);
    /*                                                                       */
    /* result = doc.getWithType(root, "time2")                               */
    /* assert.deepEqual(result, ["timestamp", new Date(1001)])               */
    assert_int_equal(AMpush(&stack,
                            AMmapGet(doc, AM_ROOT, "time2", NULL),
                            AM_VALUE_TIMESTAMP,
                            cmocka_cb).timestamp, 1001);
    /*                                                                       */
    /* result = doc.getWithType(root, "list")                                */
    /* assert.deepEqual(result, ["list", "10@aabbcc"]);                      */
    AMobjId const* const list = AMpush(&stack,
                                       AMmapGet(doc, AM_ROOT, "list", NULL),
                                       AM_VALUE_OBJ_ID,
                                       cmocka_cb).obj_id;
    assert_int_equal(AMobjIdCounter(list), 10);
    assert_string_equal(AMactorIdStr(AMobjIdActorId(list)), "aabbcc");
    /*                                                                       */
    /* result = doc.getWithType(root, "null")                                */
    /* assert.deepEqual(result, ["null", null]);                             */
    AMpush(&stack,
           AMmapGet(doc, AM_ROOT, "null", NULL),
           AM_VALUE_NULL,
           cmocka_cb);
}

/**
 * \brief should be able to use bytes
 */
static void test_should_be_able_to_use_bytes(void** state) {
    AMresultStack* stack = *state;
    /* const doc = create()                                                  */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    /* doc.put("_root", "data1", new Uint8Array([10, 11, 12]));              */
    static uint8_t const DATA1[] = {10, 11, 12};
    AMfree(AMmapPutBytes(doc, AM_ROOT, "data1", DATA1, sizeof(DATA1)));
    /* doc.put("_root", "data2", new Uint8Array([13, 14, 15]), "bytes");     */
    static uint8_t const DATA2[] = {13, 14, 15};
    AMfree(AMmapPutBytes(doc, AM_ROOT, "data2", DATA2, sizeof(DATA2)));
    /* const value1 = doc.getWithType("_root", "data1")                      */
    AMbyteSpan const value1 = AMpush(&stack,
                                     AMmapGet(doc, AM_ROOT, "data1", NULL),
                                     AM_VALUE_BYTES,
                                     cmocka_cb).bytes;
    /* assert.deepEqual(value1, ["bytes", new Uint8Array([10, 11, 12])]);    */
    assert_int_equal(value1.count, sizeof(DATA1));
    assert_memory_equal(value1.src, DATA1, sizeof(DATA1));
    /* const value2 = doc.getWithType("_root", "data2")                      */
    AMbyteSpan const value2 = AMpush(&stack,
                                     AMmapGet(doc, AM_ROOT, "data2", NULL),
                                     AM_VALUE_BYTES,
                                     cmocka_cb).bytes;
    /* assert.deepEqual(value2, ["bytes", new Uint8Array([13, 14, 15])]);    */
    assert_int_equal(value2.count, sizeof(DATA2));
    assert_memory_equal(value2.src, DATA2, sizeof(DATA2));
}

/**
 * \brief should be able to make subobjects
 */
static void test_should_be_able_to_make_subobjects(void** state) {
    AMresultStack* stack = *state;
    /* const doc = create()                                                  */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    /* const root = "_root"                                                  */
    /* let result                                                            */
    /*                                                                       */
    /* const submap = doc.putObject(root, "submap", {})                      */
    AMobjId const* const submap = AMpush(
        &stack,
        AMmapPutObject(doc, AM_ROOT, "submap", AM_OBJ_TYPE_MAP),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    /* doc.put(submap, "number", 6, "uint")                                  */
    AMfree(AMmapPutUint(doc, submap, "number", 6));
    /* assert.strictEqual(doc.pendingOps(), 2)                               */
    assert_int_equal(AMpendingOps(doc), 2);
    /*                                                                       */
    /* result = doc.getWithType(root, "submap")                              */
    /* assert.deepEqual(result, ["map", submap])                             */
    assert_true(AMobjIdEqual(AMpush(&stack,
                                    AMmapGet(doc, AM_ROOT, "submap", NULL),
                                    AM_VALUE_OBJ_ID,
                                    cmocka_cb).obj_id,
                             submap));
    /*                                                                       */
    /* result = doc.getWithType(submap, "number")                            */
    /* assert.deepEqual(result, ["uint", 6])                                 */
    assert_int_equal(AMpush(&stack,
                            AMmapGet(doc, submap, "number", NULL),
                            AM_VALUE_UINT,
                            cmocka_cb).uint,
                     6);
}

/**
 * \brief should be able to make lists
 */
static void test_should_be_able_to_make_lists(void** state) {
    AMresultStack* stack = *state;
    /* const doc = create()                                                  */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    /* const root = "_root"                                                  */
    /*                                                                       */
    /* const sublist = doc.putObject(root, "numbers", [])                    */
    AMobjId const* const sublist = AMpush(
        &stack,
        AMmapPutObject(doc, AM_ROOT, "numbers", AM_OBJ_TYPE_LIST),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    /* doc.insert(sublist, 0, "a");                                          */
    AMfree(AMlistPutStr(doc, sublist, 0, true, "a"));
    /* doc.insert(sublist, 1, "b");                                          */
    AMfree(AMlistPutStr(doc, sublist, 1, true, "b"));
    /* doc.insert(sublist, 2, "c");                                          */
    AMfree(AMlistPutStr(doc, sublist, 2, true, "c"));
    /* doc.insert(sublist, 0, "z");                                          */
    AMfree(AMlistPutStr(doc, sublist, 0, true, "z"));
    /*                                                                       */
    /* assert.deepEqual(doc.getWithType(sublist, 0), ["str", "z"])           */
    assert_string_equal(AMpush(&stack,
                               AMlistGet(doc, sublist, 0, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "z");
    /* assert.deepEqual(doc.getWithType(sublist, 1), ["str", "a"])           */
    assert_string_equal(AMpush(&stack,
                               AMlistGet(doc, sublist, 1, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "a");
    /* assert.deepEqual(doc.getWithType(sublist, 2), ["str", "b"])           */
    assert_string_equal(AMpush(&stack,
                               AMlistGet(doc, sublist, 2, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "b");
    /* assert.deepEqual(doc.getWithType(sublist, 3), ["str", "c"])           */
    assert_string_equal(AMpush(&stack,
                               AMlistGet(doc, sublist, 3, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "c");
    /* assert.deepEqual(doc.length(sublist), 4)                              */
    assert_int_equal(AMobjSize(doc, sublist, NULL), 4);
    /*                                                                       */
    /* doc.put(sublist, 2, "b v2");                                          */
    AMfree(AMlistPutStr(doc, sublist, 2, false, "b v2"));
    /*                                                                       */
    /* assert.deepEqual(doc.getWithType(sublist, 2), ["str", "b v2"])        */
    assert_string_equal(AMpush(&stack,
                               AMlistGet(doc, sublist, 2, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "b v2");
    /* assert.deepEqual(doc.length(sublist), 4)                              */
    assert_int_equal(AMobjSize(doc, sublist, NULL), 4);
}

/**
 * \brief lists have insert, set, splice, and push ops
 */
static void test_lists_have_insert_set_splice_and_push_ops(void** state) {
    AMresultStack* stack = *state;
    /* const doc = create()                                                  */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    /* const root = "_root"                                                  */
    /*                                                                       */
    /* const sublist = doc.putObject(root, "letters", [])                    */
    AMobjId const* const sublist = AMpush(
        &stack,
        AMmapPutObject(doc, AM_ROOT, "letters", AM_OBJ_TYPE_LIST),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    /* doc.insert(sublist, 0, "a");                                          */
    AMfree(AMlistPutStr(doc, sublist, 0, true, "a"));
    /* doc.insert(sublist, 0, "b");                                          */
    AMfree(AMlistPutStr(doc, sublist, 0, true, "b"));
    /* assert.deepEqual(doc.materialize(), { letters: ["b", "a"] })          */
    AMmapItems doc_items = AMpush(&stack,
                                  AMmapRange(doc, AM_ROOT, NULL, NULL, NULL),
                                  AM_VALUE_MAP_ITEMS,
                                  cmocka_cb).map_items;
    AMmapItem const* doc_item = AMmapItemsNext(&doc_items, 1);
    assert_string_equal(AMmapItemKey(doc_item), "letters");
    {
        AMlistItems list_items = AMpush(
            &stack,
            AMlistRange(doc, AMmapItemObjId(doc_item), 0, SIZE_MAX, NULL),
            AM_VALUE_LIST_ITEMS,
            cmocka_cb).list_items;
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "b");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "a");
        assert_null(AMlistItemsNext(&list_items, 1));
    }
    /* doc.push(sublist, "c");                                               */
    AMfree(AMlistPutStr(doc, sublist, SIZE_MAX, true, "c"));
    /* const heads = doc.getHeads()                                          */
    AMchangeHashes const heads = AMpush(&stack,
                                        AMgetHeads(doc),
                                        AM_VALUE_CHANGE_HASHES,
                                        cmocka_cb).change_hashes;
    /* assert.deepEqual(doc.materialize(), { letters: ["b", "a", "c"] })     */
    doc_items = AMpush(&stack,
                       AMmapRange(doc, AM_ROOT, NULL, NULL, NULL),
                       AM_VALUE_MAP_ITEMS,
                       cmocka_cb).map_items;
    doc_item = AMmapItemsNext(&doc_items, 1);
    assert_string_equal(AMmapItemKey(doc_item), "letters");
    {
        AMlistItems list_items = AMpush(
            &stack,
            AMlistRange(doc, AMmapItemObjId(doc_item), 0, SIZE_MAX, NULL),
            AM_VALUE_LIST_ITEMS,
            cmocka_cb).list_items;
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "b");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "a");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "c");
        assert_null(AMlistItemsNext(&list_items, 1));
    }
    /* doc.push(sublist, 3, "timestamp");                                    */
    AMfree(AMlistPutTimestamp(doc, sublist, SIZE_MAX, true, 3));
    /* assert.deepEqual(doc.materialize(), { letters: ["b", "a", "c", new Date(3)] })*/
    doc_items = AMpush(&stack,
                       AMmapRange(doc, AM_ROOT, NULL, NULL, NULL),
                       AM_VALUE_MAP_ITEMS,
                       cmocka_cb).map_items;
    doc_item = AMmapItemsNext(&doc_items, 1);
    assert_string_equal(AMmapItemKey(doc_item), "letters");
    {
        AMlistItems list_items = AMpush(
            &stack,
            AMlistRange(doc, AMmapItemObjId(doc_item), 0, SIZE_MAX, NULL),
            AM_VALUE_LIST_ITEMS,
            cmocka_cb).list_items;
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "b");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "a");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "c");
        assert_int_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).timestamp,
                         3);
        assert_null(AMlistItemsNext(&list_items, 1));
    }
    /* doc.splice(sublist, 1, 1, ["d", "e", "f"]);                           */
    static AMvalue const DATA[] = {{.str_tag = AM_VALUE_STR, .str = "d"},
                                   {.str_tag = AM_VALUE_STR, .str = "e"},
                                   {.str_tag = AM_VALUE_STR, .str = "f"}};
    AMfree(AMsplice(doc, sublist, 1, 1, DATA, sizeof(DATA)/sizeof(AMvalue)));
    /* assert.deepEqual(doc.materialize(), { letters: ["b", "d", "e", "f", "c", new Date(3)] })*/
    doc_items = AMpush(&stack,
                       AMmapRange(doc, AM_ROOT, NULL, NULL, NULL),
                       AM_VALUE_MAP_ITEMS,
                       cmocka_cb).map_items;
    doc_item = AMmapItemsNext(&doc_items, 1);
    assert_string_equal(AMmapItemKey(doc_item), "letters");
    {
        AMlistItems list_items = AMpush(
            &stack,
            AMlistRange(doc, AMmapItemObjId(doc_item), 0, SIZE_MAX, NULL),
            AM_VALUE_LIST_ITEMS,
            cmocka_cb).list_items;
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "b");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "d");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "e");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "f");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "c");
        assert_int_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).timestamp,
                         3);
        assert_null(AMlistItemsNext(&list_items, 1));
    }
    /* doc.put(sublist, 0, "z");                                             */
    AMfree(AMlistPutStr(doc, sublist, 0, false, "z"));
    /* assert.deepEqual(doc.materialize(), { letters: ["z", "d", "e", "f", "c", new Date(3)] })*/
    doc_items = AMpush(&stack,
                       AMmapRange(doc, AM_ROOT, NULL, NULL, NULL),
                       AM_VALUE_MAP_ITEMS,
                       cmocka_cb).map_items;
    doc_item = AMmapItemsNext(&doc_items, 1);
    assert_string_equal(AMmapItemKey(doc_item), "letters");
    {
        AMlistItems list_items = AMpush(
            &stack,
            AMlistRange(doc, AMmapItemObjId(doc_item), 0, SIZE_MAX, NULL),
            AM_VALUE_LIST_ITEMS,
            cmocka_cb).list_items;
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "z");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "d");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "e");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "f");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "c");
        assert_int_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).timestamp,
                         3);
        assert_null(AMlistItemsNext(&list_items, 1));
    }
    /* assert.deepEqual(doc.materialize(sublist), ["z", "d", "e", "f", "c", new Date(3)])*/
    AMlistItems sublist_items = AMpush(
                           &stack,
                           AMlistRange(doc, sublist, 0, SIZE_MAX, NULL),
                           AM_VALUE_LIST_ITEMS,
                           cmocka_cb).list_items;
    assert_string_equal(AMlistItemValue(AMlistItemsNext(&sublist_items, 1)).str,
                        "z");
    assert_string_equal(AMlistItemValue(AMlistItemsNext(&sublist_items, 1)).str,
                        "d");
    assert_string_equal(AMlistItemValue(AMlistItemsNext(&sublist_items, 1)).str,
                        "e");
    assert_string_equal(AMlistItemValue(AMlistItemsNext(&sublist_items, 1)).str,
                        "f");
    assert_string_equal(AMlistItemValue(AMlistItemsNext(&sublist_items, 1)).str,
                        "c");
    assert_int_equal(AMlistItemValue(AMlistItemsNext(&sublist_items, 1)).timestamp,
                     3);
    assert_null(AMlistItemsNext(&sublist_items, 1));
    /* assert.deepEqual(doc.length(sublist), 6)                              */
    assert_int_equal(AMobjSize(doc, sublist, NULL), 6);
    /* assert.deepEqual(doc.materialize("/", heads), { letters: ["b", "a", "c"] })*/
    doc_items = AMpush(&stack,
                       AMmapRange(doc, AM_ROOT, NULL, NULL, &heads),
                       AM_VALUE_MAP_ITEMS,
                       cmocka_cb).map_items;
    doc_item = AMmapItemsNext(&doc_items, 1);
    assert_string_equal(AMmapItemKey(doc_item), "letters");
    {
        AMlistItems list_items = AMpush(
            &stack,
            AMlistRange(doc, AMmapItemObjId(doc_item), 0, SIZE_MAX, &heads),
            AM_VALUE_LIST_ITEMS,
            cmocka_cb).list_items;
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "b");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "a");
        assert_string_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).str,
                            "c");
        assert_null(AMlistItemsNext(&list_items, 1));
    }
}

/**
 * \brief should be able to delete non-existent props
 */
static void test_should_be_able_to_delete_non_existent_props(void** state) {
    AMresultStack* stack = *state;
    /* const doc = create()                                                  */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    /*                                                                       */
    /* doc.put("_root", "foo", "bar")                                        */
    AMfree(AMmapPutStr(doc, AM_ROOT, "foo", "bar"));
    /* doc.put("_root", "bip", "bap")                                        */
    AMfree(AMmapPutStr(doc, AM_ROOT, "bip", "bap"));
    /* const hash1 = doc.commit()                                            */
    AMchangeHashes const hash1 = AMpush(&stack,
                                        AMcommit(doc, NULL, NULL),
                                        AM_VALUE_CHANGE_HASHES,
                                        cmocka_cb).change_hashes;
    /*                                                                       */
    /* assert.deepEqual(doc.keys("_root"), ["bip", "foo"])                   */
    AMstrs keys = AMpush(&stack,
                         AMkeys(doc, AM_ROOT, NULL),
                         AM_VALUE_STRS,
                         cmocka_cb).strs;
    assert_string_equal(AMstrsNext(&keys, 1), "bip");
    assert_string_equal(AMstrsNext(&keys, 1), "foo");
    /*                                                                       */
    /* doc.delete("_root", "foo")                                            */
    AMfree(AMmapDelete(doc, AM_ROOT, "foo"));
    /* doc.delete("_root", "baz")                                            */
    AMfree(AMmapDelete(doc, AM_ROOT, "baz"));
    /* const hash2 = doc.commit()                                            */
    AMchangeHashes const hash2 = AMpush(&stack,
                                        AMcommit(doc, NULL, NULL),
                                        AM_VALUE_CHANGE_HASHES,
                                        cmocka_cb).change_hashes;
    /*                                                                       */
    /* assert.deepEqual(doc.keys("_root"), ["bip"])                          */
    keys = AMpush(&stack,
                  AMkeys(doc, AM_ROOT, NULL),
                  AM_VALUE_STRS,
                  cmocka_cb).strs;
    assert_string_equal(AMstrsNext(&keys, 1), "bip");
    /* assert.deepEqual(doc.keys("_root", [hash1]), ["bip", "foo"])          */
    keys = AMpush(&stack,
                  AMkeys(doc, AM_ROOT, &hash1),
                  AM_VALUE_STRS,
                  cmocka_cb).strs;
    assert_string_equal(AMstrsNext(&keys, 1), "bip");
    assert_string_equal(AMstrsNext(&keys, 1), "foo");
    /* assert.deepEqual(doc.keys("_root", [hash2]), ["bip"])                 */
    keys = AMpush(&stack,
                  AMkeys(doc, AM_ROOT, &hash2),
                  AM_VALUE_STRS,
                  cmocka_cb).strs;
    assert_string_equal(AMstrsNext(&keys, 1), "bip");
}

/**
 * \brief should be able to del
 */
static void test_should_be_able_to_del(void **state) {
    AMresultStack* stack = *state;
    /* const doc = create()                                                  */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    /* const root = "_root"                                                  */
    /*                                                                       */
    /* doc.put(root, "xxx", "xxx");                                          */
    AMfree(AMmapPutStr(doc, AM_ROOT, "xxx", "xxx"));
    /* assert.deepEqual(doc.getWithType(root, "xxx"), ["str", "xxx"])        */
    assert_string_equal(AMpush(&stack,
                               AMmapGet(doc, AM_ROOT, "xxx", NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "xxx");
    /* doc.delete(root, "xxx");                                              */
    AMfree(AMmapDelete(doc, AM_ROOT, "xxx"));
    /* assert.deepEqual(doc.getWithType(root, "xxx"), undefined)             */
    AMpush(&stack,
           AMmapGet(doc, AM_ROOT, "xxx", NULL),
           AM_VALUE_VOID,
           cmocka_cb);
}

/**
 * \brief should be able to use counters
 */
static void test_should_be_able_to_use_counters(void** state) {
    AMresultStack* stack = *state;
    /* const doc = create()                                                  */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    /* const root = "_root"                                                  */
    /*                                                                       */
    /* doc.put(root, "counter", 10, "counter");                              */
    AMfree(AMmapPutCounter(doc, AM_ROOT, "counter", 10));
    /* assert.deepEqual(doc.getWithType(root, "counter"), ["counter", 10])   */
    assert_int_equal(AMpush(&stack,
                            AMmapGet(doc, AM_ROOT, "counter", NULL),
                            AM_VALUE_COUNTER,
                            cmocka_cb).counter, 10);
    /* doc.increment(root, "counter", 10);                                   */
    AMfree(AMmapIncrement(doc, AM_ROOT, "counter", 10));
    /* assert.deepEqual(doc.getWithType(root, "counter"), ["counter", 20])   */
    assert_int_equal(AMpush(&stack,
                            AMmapGet(doc, AM_ROOT, "counter", NULL),
                            AM_VALUE_COUNTER,
                            cmocka_cb).counter, 20);
    /* doc.increment(root, "counter", -5);                                   */
    AMfree(AMmapIncrement(doc, AM_ROOT, "counter", -5));
    /* assert.deepEqual(doc.getWithType(root, "counter"), ["counter", 15])   */
    assert_int_equal(AMpush(&stack,
                            AMmapGet(doc, AM_ROOT, "counter", NULL),
                            AM_VALUE_COUNTER,
                            cmocka_cb).counter, 15);
}

/**
 * \brief should be able to splice text
 */
static void test_should_be_able_to_splice_text(void** state) {
    AMresultStack* stack = *state;
    /* const doc = create()                                                  */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    /* const root = "_root";                                                 */
    /*                                                                       */
    /* const text = doc.putObject(root, "text", "");                         */
    AMobjId const* const text = AMpush(
        &stack,
        AMmapPutObject(doc, AM_ROOT, "text", AM_OBJ_TYPE_TEXT),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    /* doc.splice(text, 0, 0, "hello ")                                      */
    AMfree(AMspliceText(doc, text, 0, 0, "hello "));
    /* doc.splice(text, 6, 0, ["w", "o", "r", "l", "d"])                     */
    static AMvalue const WORLD[] = {{.str_tag = AM_VALUE_STR, .str = "w"},
                                    {.str_tag = AM_VALUE_STR, .str = "o"},
                                    {.str_tag = AM_VALUE_STR, .str = "r"},
                                    {.str_tag = AM_VALUE_STR, .str = "l"},
                                    {.str_tag = AM_VALUE_STR, .str = "d"}};
    AMfree(AMsplice(doc, text, 6, 0, WORLD, sizeof(WORLD)/sizeof(AMvalue)));
    /* doc.splice(text, 11, 0, ["!", "?"])                                   */
    static AMvalue const INTERROBANG[] = {{.str_tag = AM_VALUE_STR, .str = "!"},
                                          {.str_tag = AM_VALUE_STR, .str = "?"}};
    AMfree(AMsplice(doc, text, 11, 0, INTERROBANG, sizeof(INTERROBANG)/sizeof(AMvalue)));
    /* assert.deepEqual(doc.getWithType(text, 0), ["str", "h"])              */
    assert_string_equal(AMpush(&stack,
                               AMlistGet(doc, text, 0, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "h");
    /* assert.deepEqual(doc.getWithType(text, 1), ["str", "e"])              */
    assert_string_equal(AMpush(&stack,
                               AMlistGet(doc, text, 1, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "e");
    /* assert.deepEqual(doc.getWithType(text, 9), ["str", "l"])              */
    assert_string_equal(AMpush(&stack,
                               AMlistGet(doc, text, 9, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "l");
    /* assert.deepEqual(doc.getWithType(text, 10), ["str", "d"])             */
    assert_string_equal(AMpush(&stack,
                               AMlistGet(doc, text, 10, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "d");
    /* assert.deepEqual(doc.getWithType(text, 11), ["str", "!"])             */
    assert_string_equal(AMpush(&stack,
                               AMlistGet(doc, text, 11, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "!");
    /* assert.deepEqual(doc.getWithType(text, 12), ["str", "?"])             */
    assert_string_equal(AMpush(&stack,
                               AMlistGet(doc, text, 12, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "?");
}

/**
 * \brief should be able to insert objects into text
 */
static void test_should_be_able_to_insert_objects_into_text(void** state) {
    AMresultStack* stack = *state;
    /* const doc = create()                                                  */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    /* const text = doc.putObject("/", "text", "Hello world");               */
    AMobjId const* const text = AMpush(
        &stack,
        AMmapPutObject(doc, AM_ROOT, "text", AM_OBJ_TYPE_TEXT),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    AMfree(AMspliceText(doc, text, 0, 0, "Hello world"));
    /* const obj = doc.insertObject(text, 6, { hello: "world" });            */
    AMobjId const* const obj = AMpush(
        &stack,
        AMlistPutObject(doc, text, 6, true, AM_OBJ_TYPE_MAP),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    AMfree(AMmapPutStr(doc, obj, "hello", "world"));
    /* assert.deepEqual(doc.text(text), "Hello \ufffcworld");                */
    assert_string_equal(AMpush(&stack,
                               AMtext(doc, text, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "Hello \ufffcworld");
    /* assert.deepEqual(doc.getWithType(text, 6), ["map", obj]);             */
    assert_true(AMobjIdEqual(AMpush(&stack,
                                    AMlistGet(doc, text, 6, NULL),
                                    AM_VALUE_OBJ_ID,
                                    cmocka_cb).obj_id, obj));
    /* assert.deepEqual(doc.getWithType(obj, "hello"), ["str", "world"]);    */
    assert_string_equal(AMpush(&stack,
                               AMmapGet(doc, obj, "hello", NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "world");
}

/**
 * \brief should be able save all or incrementally
 */
static void test_should_be_able_to_save_all_or_incrementally(void** state) {
    AMresultStack* stack = *state;
    /* const doc = create()                                                  */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    /*                                                                       */
    /* doc.put("_root", "foo", 1)                                            */
    AMfree(AMmapPutInt(doc, AM_ROOT, "foo", 1));
    /*                                                                       */
    /* const save1 = doc.save()                                              */
    AMbyteSpan const save1 = AMpush(&stack,
                                    AMsave(doc),
                                    AM_VALUE_BYTES,
                                    cmocka_cb).bytes;
    /*                                                                       */
    /* doc.put("_root", "bar", 2)                                            */
    AMfree(AMmapPutInt(doc, AM_ROOT, "bar", 2));
    /*                                                                       */
    /* const saveMidway = doc.clone().save();                                */
    AMbyteSpan const saveMidway = AMpush(&stack,
                                         AMsave(
                                           AMpush(&stack,
                                                  AMclone(doc),
                                                  AM_VALUE_DOC,
                                                  cmocka_cb).doc),
                                         AM_VALUE_BYTES,
                                         cmocka_cb).bytes;
    /*                                                                       */
    /* const save2 = doc.saveIncremental();                                  */
    AMbyteSpan const save2 = AMpush(&stack,
                                    AMsaveIncremental(doc),
                                    AM_VALUE_BYTES,
                                    cmocka_cb).bytes;
    /*                                                                       */
    /* doc.put("_root", "baz", 3);                                           */
    AMfree(AMmapPutInt(doc, AM_ROOT, "baz", 3));
    /*                                                                       */
    /* const save3 = doc.saveIncremental();                                  */
    AMbyteSpan const save3 = AMpush(&stack,
                                    AMsaveIncremental(doc),
                                    AM_VALUE_BYTES,
                                    cmocka_cb).bytes;
    /*                                                                       */
    /* const saveA = doc.save();                                             */
    AMbyteSpan const saveA = AMpush(&stack,
                                    AMsave(doc),
                                    AM_VALUE_BYTES,
                                    cmocka_cb).bytes;
    /* const saveB = new Uint8Array([...save1, ...save2, ...save3]);         */
    size_t const saveB_count = save1.count + save2.count + save3.count;
    uint8_t* const saveB_src = test_malloc(saveB_count);
    memcpy(saveB_src, save1.src, save1.count);
    memcpy(saveB_src + save1.count, save2.src, save2.count);
    memcpy(saveB_src + save1.count + save2.count, save3.src, save3.count);
    /*                                                                       */
    /* assert.notDeepEqual(saveA, saveB);                                    */
    assert_memory_not_equal(saveA.src, saveB_src, saveA.count);
    /*                                                                       */
    /* const docA = load(saveA);                                             */
    AMdoc* const docA = AMpush(&stack,
                               AMload(saveA.src, saveA.count),
                               AM_VALUE_DOC,
                               cmocka_cb).doc;
    /* const docB = load(saveB);                                             */
    AMdoc* const docB = AMpush(&stack,
                               AMload(saveB_src, saveB_count),
                               AM_VALUE_DOC,
                               cmocka_cb).doc;
    test_free(saveB_src);
    /* const docC = load(saveMidway)                                         */
    AMdoc* const docC = AMpush(&stack,
                               AMload(saveMidway.src, saveMidway.count),
                               AM_VALUE_DOC,
                               cmocka_cb).doc;
    /* docC.loadIncremental(save3)                                           */
    AMfree(AMloadIncremental(docC, save3.src, save3.count));
    /*                                                                       */
    /* assert.deepEqual(docA.keys("_root"), docB.keys("_root"));             */
    AMstrs const keysA = AMpush(&stack,
                                AMkeys(docA, AM_ROOT, NULL),
                                AM_VALUE_STRS,
                                cmocka_cb).strs;
    AMstrs const keysB = AMpush(&stack,
                                AMkeys(docB, AM_ROOT, NULL),
                                AM_VALUE_STRS,
                                cmocka_cb).strs;
    assert_int_equal(AMstrsCmp(&keysA, &keysB), 0);
    /* assert.deepEqual(docA.save(), docB.save());                           */
    AMbyteSpan const save = AMpush(&stack,
                                   AMsave(docA),
                                   AM_VALUE_BYTES,
                                   cmocka_cb).bytes;
    assert_memory_equal(save.src,
                        AMpush(&stack,
                               AMsave(docB),
                               AM_VALUE_BYTES,
                               cmocka_cb).bytes.src,
                        save.count);
    /* assert.deepEqual(docA.save(), docC.save());                           */
    assert_memory_equal(save.src,
                        AMpush(&stack,
                               AMsave(docC),
                               AM_VALUE_BYTES,
                               cmocka_cb).bytes.src,
                        save.count);
}

/**
 * \brief should be able to splice text #2
 */
static void test_should_be_able_to_splice_text_2(void** state) {
    AMresultStack* stack = *state;
    /* const doc = create()                                                  */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    /* const text = doc.putObject("_root", "text", "");                      */
    AMobjId const* const text = AMpush(
        &stack,
        AMmapPutObject(doc, AM_ROOT, "text", AM_OBJ_TYPE_TEXT),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    /* doc.splice(text, 0, 0, "hello world");                                */
    AMfree(AMspliceText(doc, text, 0, 0, "hello world"));
    /* const hash1 = doc.commit();                                           */
    AMchangeHashes const hash1 = AMpush(&stack,
                                        AMcommit(doc, NULL, NULL),
                                        AM_VALUE_CHANGE_HASHES,
                                        cmocka_cb).change_hashes;
    /* doc.splice(text, 6, 0, "big bad ");                                   */
    AMfree(AMspliceText(doc, text, 6, 0, "big bad "));
    /* const hash2 = doc.commit();                                           */
    AMchangeHashes const hash2 = AMpush(&stack,
                                        AMcommit(doc, NULL, NULL),
                                        AM_VALUE_CHANGE_HASHES,
                                        cmocka_cb).change_hashes;
    /* assert.strictEqual(doc.text(text), "hello big bad world")             */
    assert_string_equal(AMpush(&stack,
                               AMtext(doc, text, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "hello big bad world");
    /* assert.strictEqual(doc.length(text), 19)                              */
    assert_int_equal(AMobjSize(doc, text, NULL), 19);
    /* assert.strictEqual(doc.text(text, [hash1]), "hello world")            */
    assert_string_equal(AMpush(&stack,
                               AMtext(doc, text, &hash1),
                               AM_VALUE_STR,
                               cmocka_cb).str, "hello world");
    /* assert.strictEqual(doc.length(text, [hash1]), 11)                     */
    assert_int_equal(AMobjSize(doc, text, &hash1), 11);
    /* assert.strictEqual(doc.text(text, [hash2]), "hello big bad world")    */
    assert_string_equal(AMpush(&stack,
                               AMtext(doc, text, &hash2),
                               AM_VALUE_STR,
                               cmocka_cb).str, "hello big bad world");
    /* assert.strictEqual(doc.length(text, [hash2]), 19)                     */
    assert_int_equal(AMobjSize(doc, text, &hash2), 19);
}

/**
 * \brief local inc increments all visible counters in a map
 */
static void test_local_inc_increments_all_visible_counters_in_a_map(void** state) {
    AMresultStack* stack = *state;
    /* const doc1 = create("aaaa")                                           */
    AMdoc* const doc1 = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMsetActorId(doc1, AMpush(&stack,
                                     AMactorIdInitStr("aaaa"),
                                     AM_VALUE_ACTOR_ID,
                                     cmocka_cb).actor_id));
    /* doc1.put("_root", "hello", "world")                                   */
    AMfree(AMmapPutStr(doc1, AM_ROOT, "hello", "world"));
    /* const doc2 = load(doc1.save(), "bbbb");                               */
    AMbyteSpan const save = AMpush(&stack,
                                   AMsave(doc1),
                                   AM_VALUE_BYTES,
                                   cmocka_cb).bytes;
    AMdoc* const doc2 = AMpush(&stack,
                               AMload(save.src, save.count),
                               AM_VALUE_DOC,
                               cmocka_cb).doc;
    AMfree(AMsetActorId(doc2, AMpush(&stack,
                                     AMactorIdInitStr("bbbb"),
                                     AM_VALUE_ACTOR_ID,
                                     cmocka_cb).actor_id));
    /* const doc3 = load(doc1.save(), "cccc");                               */
    AMdoc* const doc3 = AMpush(&stack,
                               AMload(save.src, save.count),
                               AM_VALUE_DOC,
                               cmocka_cb).doc;
    AMfree(AMsetActorId(doc3, AMpush(&stack,
                                     AMactorIdInitStr("cccc"),
                                     AM_VALUE_ACTOR_ID,
                                     cmocka_cb).actor_id));
    /* let heads = doc1.getHeads()                                           */
    AMchangeHashes const heads1 = AMpush(&stack,
                                         AMgetHeads(doc1),
                                         AM_VALUE_CHANGE_HASHES,
                                         cmocka_cb).change_hashes;
    /* doc1.put("_root", "cnt", 20)                                          */
    AMfree(AMmapPutInt(doc1, AM_ROOT, "cnt", 20));
    /* doc2.put("_root", "cnt", 0, "counter")                                */
    AMfree(AMmapPutCounter(doc2, AM_ROOT, "cnt", 0));
    /* doc3.put("_root", "cnt", 10, "counter")                               */
    AMfree(AMmapPutCounter(doc3, AM_ROOT, "cnt", 10));
    /* doc1.applyChanges(doc2.getChanges(heads))                             */
    AMchanges const changes2 = AMpush(&stack,
                                      AMgetChanges(doc2, &heads1),
                                      AM_VALUE_CHANGES,
                                      cmocka_cb).changes;
    AMfree(AMapplyChanges(doc1, &changes2));
    /* doc1.applyChanges(doc3.getChanges(heads))                             */
    AMchanges const changes3 = AMpush(&stack,
                                      AMgetChanges(doc3, &heads1),
                                      AM_VALUE_CHANGES,
                                      cmocka_cb).changes;
    AMfree(AMapplyChanges(doc1, &changes3));
    /* let result = doc1.getAll("_root", "cnt")                              */
    AMobjItems result = AMpush(&stack,
                               AMmapGetAll(doc1, AM_ROOT, "cnt", NULL),
                               AM_VALUE_OBJ_ITEMS,
                               cmocka_cb).obj_items;
    /* assert.deepEqual(result, [
         ['int', 20, '2@aaaa'],
         ['counter', 0, '2@bbbb'],
         ['counter', 10, '2@cccc'],
       ])                                                                    */
    AMobjItem const* result_item = AMobjItemsNext(&result, 1);
    assert_int_equal(AMobjItemValue(result_item).int_, 20);
    assert_int_equal(AMobjIdCounter(AMobjItemObjId(result_item)), 2);
    assert_string_equal(AMactorIdStr(AMobjIdActorId(AMobjItemObjId(result_item))),
                        "aaaa");
    result_item = AMobjItemsNext(&result, 1);
    assert_int_equal(AMobjItemValue(result_item).counter, 0);
    assert_int_equal(AMobjIdCounter(AMobjItemObjId(result_item)), 2);
    assert_string_equal(AMactorIdStr(AMobjIdActorId(AMobjItemObjId(result_item))),
                        "bbbb");
    result_item = AMobjItemsNext(&result, 1);
    assert_int_equal(AMobjItemValue(result_item).counter, 10);
    assert_int_equal(AMobjIdCounter(AMobjItemObjId(result_item)), 2);
    assert_string_equal(AMactorIdStr(AMobjIdActorId(AMobjItemObjId(result_item))),
                        "cccc");
    /* doc1.increment("_root", "cnt", 5)                                     */
    AMfree(AMmapIncrement(doc1, AM_ROOT, "cnt", 5));
    /* result = doc1.getAll("_root", "cnt")                                  */
    result = AMpush(&stack,
                    AMmapGetAll(doc1, AM_ROOT, "cnt", NULL),
                    AM_VALUE_OBJ_ITEMS,
                    cmocka_cb).obj_items;
    /* assert.deepEqual(result, [
         ['counter', 5, '2@bbbb'],
         ['counter', 15, '2@cccc'],
       ])                                                                    */
    result_item = AMobjItemsNext(&result, 1);
    assert_int_equal(AMobjItemValue(result_item).counter, 5);
    assert_int_equal(AMobjIdCounter(AMobjItemObjId(result_item)), 2);
    assert_string_equal(AMactorIdStr(AMobjIdActorId(AMobjItemObjId(result_item))),
                        "bbbb");
    result_item = AMobjItemsNext(&result, 1);
    assert_int_equal(AMobjItemValue(result_item).counter, 15);
    assert_int_equal(AMobjIdCounter(AMobjItemObjId(result_item)), 2);
    assert_string_equal(AMactorIdStr(AMobjIdActorId(AMobjItemObjId(result_item))),
                        "cccc");
    /*                                                                       */
    /* const save1 = doc1.save()                                             */
    AMbyteSpan const save1 = AMpush(&stack,
                                    AMsave(doc1),
                                    AM_VALUE_BYTES,
                                    cmocka_cb).bytes;
    /* const doc4 = load(save1)                                              */
    AMdoc* const doc4 = AMpush(&stack,
                               AMload(save1.src, save1.count),
                               AM_VALUE_DOC,
                               cmocka_cb).doc;
    /* assert.deepEqual(doc4.save(), save1);                                 */
    assert_memory_equal(AMpush(&stack,
                               AMsave(doc4),
                               AM_VALUE_BYTES,
                               cmocka_cb).bytes.src,
                        save1.src,
                        save1.count);
}

/**
 * \brief local inc increments all visible counters in a sequence
 */
static void test_local_inc_increments_all_visible_counters_in_a_sequence(void** state) {
    AMresultStack* stack = *state;
    /* const doc1 = create("aaaa")                                           */
    AMdoc* const doc1 = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMsetActorId(doc1, AMpush(&stack,
                                     AMactorIdInitStr("aaaa"),
                                     AM_VALUE_ACTOR_ID,
                                     cmocka_cb).actor_id));
    /* const seq = doc1.putObject("_root", "seq", [])                        */
    AMobjId const* const seq = AMpush(
        &stack,
        AMmapPutObject(doc1, AM_ROOT, "seq", AM_OBJ_TYPE_LIST),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    /* doc1.insert(seq, 0, "hello")                                          */
    AMfree(AMlistPutStr(doc1, seq, 0, true, "hello"));
    /* const doc2 = load(doc1.save(), "bbbb");                               */
    AMbyteSpan const save1 = AMpush(&stack,
                                    AMsave(doc1),
                                    AM_VALUE_BYTES,
                                    cmocka_cb).bytes;
    AMdoc* const doc2 = AMpush(&stack,
                               AMload(save1.src, save1.count),
                               AM_VALUE_DOC,
                               cmocka_cb).doc;
    AMfree(AMsetActorId(doc2, AMpush(&stack,
                                     AMactorIdInitStr("bbbb"),
                                     AM_VALUE_ACTOR_ID,
                                     cmocka_cb).actor_id));
    /* const doc3 = load(doc1.save(), "cccc");                               */
    AMdoc* const doc3 = AMpush(&stack,
                               AMload(save1.src, save1.count),
                               AM_VALUE_DOC,
                               cmocka_cb).doc;
    AMfree(AMsetActorId(doc3, AMpush(&stack,
                                     AMactorIdInitStr("cccc"),
                                     AM_VALUE_ACTOR_ID,
                                     cmocka_cb).actor_id));
    /* let heads = doc1.getHeads()                                           */
    AMchangeHashes const heads1 = AMpush(&stack,
                                         AMgetHeads(doc1),
                                         AM_VALUE_CHANGE_HASHES,
                                         cmocka_cb).change_hashes;
    /* doc1.put(seq, 0, 20)                                                  */
    AMfree(AMlistPutInt(doc1, seq, 0, false, 20));
    /* doc2.put(seq, 0, 0, "counter")                                        */
    AMfree(AMlistPutCounter(doc2, seq, 0, false, 0));
    /* doc3.put(seq, 0, 10, "counter")                                       */
    AMfree(AMlistPutCounter(doc3, seq, 0, false, 10));
    /* doc1.applyChanges(doc2.getChanges(heads))                             */
    AMchanges const changes2 = AMpush(&stack,
                                      AMgetChanges(doc2, &heads1),
                                      AM_VALUE_CHANGES,
                                      cmocka_cb).changes;
    AMfree(AMapplyChanges(doc1, &changes2));
    /* doc1.applyChanges(doc3.getChanges(heads))                             */
    AMchanges const changes3 = AMpush(&stack,
                                      AMgetChanges(doc3, &heads1),
                                      AM_VALUE_CHANGES,
                                      cmocka_cb).changes;
    AMfree(AMapplyChanges(doc1, &changes3));
    /* let result = doc1.getAll(seq, 0)                                      */
    AMobjItems result = AMpush(&stack,
                               AMlistGetAll(doc1, seq, 0, NULL),
                               AM_VALUE_OBJ_ITEMS,
                               cmocka_cb).obj_items;
    /* assert.deepEqual(result, [
         ['int', 20, '3@aaaa'],
         ['counter', 0, '3@bbbb'],
         ['counter', 10, '3@cccc'],
       ])                                                                    */
    AMobjItem const* result_item = AMobjItemsNext(&result, 1);
    assert_int_equal(AMobjItemValue(result_item).int_, 20);
    assert_int_equal(AMobjIdCounter(AMobjItemObjId(result_item)), 3);
    assert_string_equal(AMactorIdStr(AMobjIdActorId(AMobjItemObjId(result_item))),
                        "aaaa");
    result_item = AMobjItemsNext(&result, 1);
    assert_int_equal(AMobjItemValue(result_item).counter, 0);
    assert_int_equal(AMobjIdCounter(AMobjItemObjId(result_item)), 3);
    assert_string_equal(AMactorIdStr(AMobjIdActorId(AMobjItemObjId(result_item))),
                        "bbbb");
    result_item = AMobjItemsNext(&result, 1);
    assert_int_equal(AMobjItemValue(result_item).counter, 10);
    assert_int_equal(AMobjIdCounter(AMobjItemObjId(result_item)), 3);
    assert_string_equal(AMactorIdStr(AMobjIdActorId(AMobjItemObjId(result_item))),
                        "cccc");
    /* doc1.increment(seq, 0, 5)                                             */
    AMfree(AMlistIncrement(doc1, seq, 0, 5));
    /* result = doc1.getAll(seq, 0)                                          */
    result = AMpush(&stack,
                    AMlistGetAll(doc1, seq, 0, NULL),
                    AM_VALUE_OBJ_ITEMS,
                    cmocka_cb).obj_items;
    /* assert.deepEqual(result, [
         ['counter', 5, '3@bbbb'],
         ['counter', 15, '3@cccc'],
       ])                                                                    */
    result_item = AMobjItemsNext(&result, 1);
    assert_int_equal(AMobjItemValue(result_item).counter, 5);
    assert_int_equal(AMobjIdCounter(AMobjItemObjId(result_item)), 3);
    assert_string_equal(AMactorIdStr(AMobjIdActorId(AMobjItemObjId(result_item))),
                        "bbbb");
    result_item = AMobjItemsNext(&result, 1);
    assert_int_equal(AMobjItemValue(result_item).counter, 15);
    assert_int_equal(AMobjIdCounter(AMobjItemObjId(result_item)), 3);
    assert_string_equal(AMactorIdStr(AMobjIdActorId(AMobjItemObjId(result_item))),
                        "cccc");
    /*                                                                       */
    /* const save = doc1.save()                                              */
    AMbyteSpan const save = AMpush(&stack,
                                   AMsave(doc1),
                                   AM_VALUE_BYTES,
                                   cmocka_cb).bytes;
    /* const doc4 = load(save)                                               */
    AMdoc* const doc4 = AMpush(&stack,
                               AMload(save.src, save.count),
                               AM_VALUE_DOC,
                               cmocka_cb).doc;
    /* assert.deepEqual(doc4.save(), save);                                  */
    assert_memory_equal(AMpush(&stack,
                               AMsave(doc4),
                               AM_VALUE_BYTES,
                               cmocka_cb).bytes.src,
                        save.src,
                        save.count);
}

/**
 * \brief paths can be used instead of objids
 */
static void test_paths_can_be_used_instead_of_objids(void** state);

/**
 * \brief should be able to fetch changes by hash
 */
static void test_should_be_able_to_fetch_changes_by_hash(void** state) {
    AMresultStack* stack = *state;
    /* const doc1 = create("aaaa")                                           */
    AMdoc* const doc1 = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMsetActorId(doc1, AMpush(&stack,
                                     AMactorIdInitStr("aaaa"),
                                     AM_VALUE_ACTOR_ID,
                                     cmocka_cb).actor_id));
    /* const doc2 = create("bbbb")                                           */
    AMdoc* const doc2 = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMsetActorId(doc2, AMpush(&stack,
                                     AMactorIdInitStr("bbbb"),
                                     AM_VALUE_ACTOR_ID,
                                     cmocka_cb).actor_id));
    /* doc1.put("/", "a", "b")                                               */
    AMfree(AMmapPutStr(doc1, AM_ROOT, "a", "b"));
    /* doc2.put("/", "b", "c")                                               */
    AMfree(AMmapPutStr(doc2, AM_ROOT, "b", "c"));
    /* const head1 = doc1.getHeads()                                         */
    AMchangeHashes head1 = AMpush(&stack,
                                  AMgetHeads(doc1),
                                  AM_VALUE_CHANGE_HASHES,
                                  cmocka_cb).change_hashes;
    /* const head2 = doc2.getHeads()                                         */
    AMchangeHashes head2 = AMpush(&stack,
                                  AMgetHeads(doc2),
                                  AM_VALUE_CHANGE_HASHES,
                                  cmocka_cb).change_hashes;
    /* const change1 = doc1.getChangeByHash(head1[0])
       if (change1 === null) { throw new RangeError("change1 should not be null") }*/
    AMbyteSpan const change_hash1 = AMchangeHashesNext(&head1, 1);
    AMchanges change1 = AMpush(
        &stack,
        AMgetChangeByHash(doc1, change_hash1.src, change_hash1.count),
        AM_VALUE_CHANGES,
        cmocka_cb).changes;
    /* const change2 = doc1.getChangeByHash(head2[0])
       assert.deepEqual(change2, null)                                       */
    AMbyteSpan const change_hash2 = AMchangeHashesNext(&head2, 1);
    AMpush(&stack,
           AMgetChangeByHash(doc1, change_hash2.src, change_hash2.count),
           AM_VALUE_VOID,
           cmocka_cb);
    /* assert.deepEqual(decodeChange(change1).hash, head1[0])                */
    assert_memory_equal(AMchangeHash(AMchangesNext(&change1, 1)).src,
                        change_hash1.src,
                        change_hash1.count);
}

/**
 * \brief recursive sets are possible
 */
static void test_recursive_sets_are_possible(void** state) {
    AMresultStack* stack = *state;
    /* const doc = create("aaaa")                                            */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMsetActorId(doc, AMpush(&stack,
                                    AMactorIdInitStr("aaaa"),
                                    AM_VALUE_ACTOR_ID,
                                    cmocka_cb).actor_id));
    /* const l1 = doc.putObject("_root", "list", [{ foo: "bar" }, [1, 2, 3]])*/
    AMobjId const* const l1 = AMpush(
        &stack,
        AMmapPutObject(doc, AM_ROOT, "list", AM_OBJ_TYPE_LIST),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    {
        AMobjId const* const map = AMpush(
            &stack,
            AMlistPutObject(doc, l1, 0, true, AM_OBJ_TYPE_MAP),
            AM_VALUE_OBJ_ID,
            cmocka_cb).obj_id;
        AMfree(AMmapPutStr(doc, map, "foo", "bar"));
        AMobjId const* const list = AMpush(
            &stack,
            AMlistPutObject(doc, l1, SIZE_MAX, true, AM_OBJ_TYPE_LIST),
            AM_VALUE_OBJ_ID,
            cmocka_cb).obj_id;
        for (int value = 1; value != 4; ++value) {
            AMfree(AMlistPutInt(doc, list, SIZE_MAX, true, value));
        }
    }
    /* const l2 = doc.insertObject(l1, 0, { zip: ["a", "b"] })               */
    AMobjId const* const l2 = AMpush(
        &stack,
        AMlistPutObject(doc, l1, 0, true, AM_OBJ_TYPE_MAP),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    {
        AMobjId const* const list = AMpush(
            &stack,
            AMmapPutObject(doc, l2, "zip", AM_OBJ_TYPE_LIST),
            AM_VALUE_OBJ_ID,
            cmocka_cb).obj_id;
        AMfree(AMlistPutStr(doc, list, SIZE_MAX, true, "a"));
        AMfree(AMlistPutStr(doc, list, SIZE_MAX, true, "b"));
    }
    /* const l3 = doc.putObject("_root", "info1", "hello world") // 'text' object*/
    AMobjId const* const l3 = AMpush(
        &stack,
        AMmapPutObject(doc, AM_ROOT, "info1", AM_OBJ_TYPE_TEXT),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    AMfree(AMspliceText(doc, l3, 0, 0, "hello world"));
    /* doc.put("_root", "info2", "hello world")  // 'str'                    */
    AMfree(AMmapPutStr(doc, AM_ROOT, "info2", "hello world"));
    /* const l4 = doc.putObject("_root", "info3", "hello world")             */
    AMobjId const* const l4 = AMpush(
        &stack,
        AMmapPutObject(doc, AM_ROOT, "info3", AM_OBJ_TYPE_TEXT),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    AMfree(AMspliceText(doc, l4, 0, 0, "hello world"));
    /* assert.deepEqual(doc.materialize(), {
         "list": [{ zip: ["a", "b"] }, { foo: "bar" }, [1, 2, 3]],
         "info1": "hello world",
         "info2": "hello world",
         "info3": "hello world",
       })                                                                       */
    AMmapItems doc_items = AMpush(&stack,
                                  AMmapRange(doc, AM_ROOT, NULL, NULL, NULL),
                                  AM_VALUE_MAP_ITEMS,
                                  cmocka_cb).map_items;
    AMmapItem const* doc_item = AMmapItemsNext(&doc_items, 1);
    assert_string_equal(AMmapItemKey(doc_item), "info1");
    assert_string_equal(AMpush(&stack,
                               AMtext(doc, AMmapItemObjId(doc_item), NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "hello world");
    doc_item = AMmapItemsNext(&doc_items, 1);
    assert_string_equal(AMmapItemKey(doc_item), "info2");
    assert_string_equal(AMmapItemValue(doc_item).str, "hello world");
    doc_item = AMmapItemsNext(&doc_items, 1);
    assert_string_equal(AMmapItemKey(doc_item), "info3");
    assert_string_equal(AMpush(&stack,
                               AMtext(doc, AMmapItemObjId(doc_item), NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "hello world");
    doc_item = AMmapItemsNext(&doc_items, 1);
    assert_string_equal(AMmapItemKey(doc_item), "list");
    {
        AMlistItems list_items = AMpush(
            &stack,
            AMlistRange(doc, AMmapItemObjId(doc_item), 0, SIZE_MAX, NULL),
            AM_VALUE_LIST_ITEMS,
            cmocka_cb).list_items;
        AMlistItem const* list_item = AMlistItemsNext(&list_items, 1);
        {
            AMmapItems map_items = AMpush(
                &stack,
                AMmapRange(doc, AMlistItemObjId(list_item), NULL, NULL, NULL),
                AM_VALUE_MAP_ITEMS,
                cmocka_cb).map_items;
            AMmapItem const* map_item = AMmapItemsNext(&map_items, 1);
            assert_string_equal(AMmapItemKey(map_item), "zip");
            {
                AMlistItems list_items = AMpush(
                    &stack,
                    AMlistRange(doc, AMmapItemObjId(map_item), 0, SIZE_MAX, NULL),
                    AM_VALUE_LIST_ITEMS,
                    cmocka_cb).list_items;
                assert_string_equal(AMlistItemValue(
                                        AMlistItemsNext(&list_items, 1)).str,
                                    "a");
                assert_string_equal(AMlistItemValue(
                                        AMlistItemsNext(&list_items, 1)).str,
                                    "b");
            }
        }
        list_item = AMlistItemsNext(&list_items, 1);
        {
            AMmapItems map_items = AMpush(
                &stack,
                AMmapRange(doc, AMlistItemObjId(list_item), NULL, NULL, NULL),
                AM_VALUE_MAP_ITEMS,
                cmocka_cb).map_items;
            AMmapItem const* map_item = AMmapItemsNext(&map_items, 1);
            assert_string_equal(AMmapItemKey(map_item), "foo");
            assert_string_equal(AMmapItemValue(map_item).str, "bar");
        }
        list_item = AMlistItemsNext(&list_items, 1);
        {
            AMlistItems list_items = AMpush(
                &stack,
                AMlistRange(doc, AMlistItemObjId(list_item), 0, SIZE_MAX, NULL),
                AM_VALUE_LIST_ITEMS,
                cmocka_cb).list_items;
            assert_int_equal(AMlistItemValue(
                                 AMlistItemsNext(&list_items, 1)).int_,
                             1);
            assert_int_equal(AMlistItemValue(
                                 AMlistItemsNext(&list_items, 1)).int_,
                             2);
            assert_int_equal(AMlistItemValue(
                                 AMlistItemsNext(&list_items, 1)).int_,
                             3);
        }
    }
    /* assert.deepEqual(doc.materialize(l2), { zip: ["a", "b"] })            */
    AMmapItems map_items = AMpush(
        &stack,
        AMmapRange(doc, l2, NULL, NULL, NULL),
        AM_VALUE_MAP_ITEMS,
        cmocka_cb).map_items;
    AMmapItem const* map_item = AMmapItemsNext(&map_items, 1);
    assert_string_equal(AMmapItemKey(map_item), "zip");
    {
        AMlistItems list_items = AMpush(
            &stack,
            AMlistRange(doc, AMmapItemObjId(map_item), 0, SIZE_MAX, NULL),
            AM_VALUE_LIST_ITEMS,
            cmocka_cb).list_items;
        assert_string_equal(AMlistItemValue(
                                AMlistItemsNext(&list_items, 1)).str,
                            "a");
        assert_string_equal(AMlistItemValue(
                                AMlistItemsNext(&list_items, 1)).str,
                            "b");
    }
    /* assert.deepEqual(doc.materialize(l1), [{ zip: ["a", "b"] }, { foo: "bar" }, [1, 2, 3]])*/
    AMlistItems list_items = AMpush(
        &stack,
        AMlistRange(doc, l1, 0, SIZE_MAX, NULL),
        AM_VALUE_LIST_ITEMS,
        cmocka_cb).list_items;
    AMlistItem const* list_item = AMlistItemsNext(&list_items, 1);
    {
        AMmapItems map_items = AMpush(
            &stack,
            AMmapRange(doc, AMlistItemObjId(list_item), NULL, NULL, NULL),
            AM_VALUE_MAP_ITEMS,
            cmocka_cb).map_items;
        AMmapItem const* map_item = AMmapItemsNext(&map_items, 1);
        assert_string_equal(AMmapItemKey(map_item), "zip");
        {
            AMlistItems list_items = AMpush(
                &stack,
                AMlistRange(doc, AMmapItemObjId(map_item), 0, SIZE_MAX, NULL),
                AM_VALUE_LIST_ITEMS,
                cmocka_cb).list_items;
            assert_string_equal(
                AMlistItemValue(AMlistItemsNext(&list_items, 1)).str, "a");
            assert_string_equal(AMlistItemValue(
                AMlistItemsNext(&list_items, 1)).str, "b");
        }
    }
    list_item = AMlistItemsNext(&list_items, 1);
    {
        AMmapItems map_items = AMpush(
            &stack,
            AMmapRange(doc, AMlistItemObjId(list_item), NULL, NULL, NULL),
            AM_VALUE_MAP_ITEMS,
            cmocka_cb).map_items;
        AMmapItem const* map_item = AMmapItemsNext(&map_items, 1);
        assert_string_equal(AMmapItemKey(map_item), "foo");
        assert_string_equal(AMmapItemValue(map_item).str, "bar");
    }
    list_item = AMlistItemsNext(&list_items, 1);
    {
        AMlistItems list_items = AMpush(
            &stack,
            AMlistRange(doc, AMlistItemObjId(list_item), 0, SIZE_MAX, NULL),
            AM_VALUE_LIST_ITEMS,
            cmocka_cb).list_items;
        assert_int_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).int_,
                         1);
        assert_int_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).int_,
                         2);
        assert_int_equal(AMlistItemValue(AMlistItemsNext(&list_items, 1)).int_,
                         3);
    }
    /* assert.deepEqual(doc.materialize(l4), "hello world")                  */
    assert_string_equal(AMpush(&stack,
                               AMtext(doc, l4, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "hello world");
}

/**
 * \brief only returns an object id when objects are created
 */
static void test_only_returns_an_object_id_when_objects_are_created(void** state) {
    AMresultStack* stack = *state;
    /* const doc = create("aaaa")                                            */
    AMdoc* const doc = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMsetActorId(doc, AMpush(&stack,
                                    AMactorIdInitStr("aaaa"),
                                    AM_VALUE_ACTOR_ID,
                                    cmocka_cb).actor_id));
    /* const r1 = doc.put("_root", "foo", "bar")
       assert.deepEqual(r1, null);                                           */
    AMpush(&stack,
           AMmapPutStr(doc, AM_ROOT, "foo", "bar"),
           AM_VALUE_VOID,
           cmocka_cb);
    /* const r2 = doc.putObject("_root", "list", [])                         */
    AMobjId const* const r2 = AMpush(
        &stack,
        AMmapPutObject(doc, AM_ROOT, "list", AM_OBJ_TYPE_LIST),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    /* const r3 = doc.put("_root", "counter", 10, "counter")
       assert.deepEqual(r3, null);                                           */
    AMpush(&stack,
           AMmapPutCounter(doc, AM_ROOT, "counter", 10),
           AM_VALUE_VOID,
           cmocka_cb);
    /* const r4 = doc.increment("_root", "counter", 1)
       assert.deepEqual(r4, null);                                           */
    AMpush(&stack,
           AMmapIncrement(doc, AM_ROOT, "counter", 1),
           AM_VALUE_VOID,
           cmocka_cb);
    /* const r5 = doc.delete("_root", "counter")
       assert.deepEqual(r5, null);                                           */
    AMpush(&stack,
           AMmapDelete(doc, AM_ROOT, "counter"),
           AM_VALUE_VOID,
           cmocka_cb);
    /* const r6 = doc.insert(r2, 0, 10);
       assert.deepEqual(r6, null);                                           */
    AMpush(&stack,
           AMlistPutInt(doc, r2, 0, true, 10),
           AM_VALUE_VOID,
           cmocka_cb);
    /* const r7 = doc.insertObject(r2, 0, {});                               */
    AMobjId const* const r7 = AMpush(
        &stack,
        AMlistPutObject(doc, r2, 0, true, AM_OBJ_TYPE_LIST),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    /* const r8 = doc.splice(r2, 1, 0, ["a", "b", "c"]);                     */
    AMvalue const STRS[] = {{.str_tag = AM_VALUE_STR, .str = "a",
                             .str_tag = AM_VALUE_STR, .str = "b",
                             .str_tag = AM_VALUE_STR, .str = "c"}};
    AMpush(&stack,
           AMsplice(doc, r2, 1, 0, STRS, sizeof(STRS)/sizeof(AMvalue)),
           AM_VALUE_VOID,
           cmocka_cb);
    /* assert.deepEqual(r2, "2@aaaa");                                       */
    assert_int_equal(AMobjIdCounter(r2), 2);
    assert_string_equal(AMactorIdStr(AMobjIdActorId(r2)), "aaaa");
    /* assert.deepEqual(r7, "7@aaaa");                                       */
    assert_int_equal(AMobjIdCounter(r7), 7);
    assert_string_equal(AMactorIdStr(AMobjIdActorId(r7)), "aaaa");
}

/**
 * \brief objects without properties are preserved
 */
static void test_objects_without_properties_are_preserved(void** state) {
    AMresultStack* stack = *state;
    /* const doc1 = create("aaaa")                                           */
    AMdoc* const doc1 = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMsetActorId(doc1, AMpush(&stack,
                                    AMactorIdInitStr("aaaa"),
                                    AM_VALUE_ACTOR_ID,
                                    cmocka_cb).actor_id));
    /* const a = doc1.putObject("_root", "a", {});                           */
    AMobjId const* const a = AMpush(
        &stack,
        AMmapPutObject(doc1, AM_ROOT, "a", AM_OBJ_TYPE_MAP),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    /* const b = doc1.putObject("_root", "b", {});                           */
    AMobjId const* const b = AMpush(
        &stack,
        AMmapPutObject(doc1, AM_ROOT, "b", AM_OBJ_TYPE_MAP),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    /* const c = doc1.putObject("_root", "c", {});                           */
    AMobjId const* const c = AMpush(
        &stack,
        AMmapPutObject(doc1, AM_ROOT, "c", AM_OBJ_TYPE_MAP),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    /* const d = doc1.put(c, "d", "dd");                                     */
    AMfree(AMmapPutStr(doc1, c, "d", "dd"));
    /* const saved = doc1.save();                                            */
    AMbyteSpan const saved = AMpush(&stack,
                                    AMsave(doc1),
                                    AM_VALUE_BYTES,
                                    cmocka_cb).bytes;
    /* const doc2 = load(saved);                                             */
    AMdoc* const doc2 = AMpush(&stack,
                               AMload(saved.src, saved.count),
                               AM_VALUE_DOC,
                               cmocka_cb).doc;
    /* assert.deepEqual(doc2.getWithType("_root", "a"), ["map", a])          */
    AMmapItems doc_items = AMpush(&stack,
                                  AMmapRange(doc2, AM_ROOT, NULL, NULL, NULL),
                                  AM_VALUE_MAP_ITEMS,
                                  cmocka_cb).map_items;
    assert_true(AMobjIdEqual(AMmapItemObjId(AMmapItemsNext(&doc_items, 1)), a));
    /* assert.deepEqual(doc2.keys(a), [])                                    */
    AMstrs keys = AMpush(&stack,
                         AMkeys(doc1, a, NULL),
                         AM_VALUE_STRS,
                         cmocka_cb).strs;
    assert_int_equal(AMstrsSize(&keys), 0);
    /* assert.deepEqual(doc2.getWithType("_root", "b"), ["map", b])          */
    assert_true(AMobjIdEqual(AMmapItemObjId(AMmapItemsNext(&doc_items, 1)), b));
    /* assert.deepEqual(doc2.keys(b), [])                                    */
    keys = AMpush(&stack, AMkeys(doc1, b, NULL), AM_VALUE_STRS, cmocka_cb).strs;
    assert_int_equal(AMstrsSize(&keys), 0);
    /* assert.deepEqual(doc2.getWithType("_root", "c"), ["map", c])          */
    assert_true(AMobjIdEqual(AMmapItemObjId(AMmapItemsNext(&doc_items, 1)), c));
    /* assert.deepEqual(doc2.keys(c), ["d"])                                 */
    keys = AMpush(&stack, AMkeys(doc1, c, NULL), AM_VALUE_STRS, cmocka_cb).strs;
    assert_string_equal(AMstrsNext(&keys, 1), "d");
    /* assert.deepEqual(doc2.getWithType(c, "d"), ["str", "dd"])             */
    AMobjItems obj_items = AMpush(&stack,
                                  AMobjValues(doc1, c, NULL),
                                  AM_VALUE_OBJ_ITEMS,
                                  cmocka_cb).obj_items;
    assert_string_equal(AMobjItemValue(AMobjItemsNext(&obj_items, 1)).str, "dd");
}

/**
 * \brief should allow you to forkAt a heads
 */
static void test_should_allow_you_to_forkAt_a_heads(void** state) {
    AMresultStack* stack = *state;
    /* const A = create("aaaaaa")                                            */
    AMdoc* const A = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMsetActorId(A, AMpush(&stack,
                                  AMactorIdInitStr("aaaaaa"),
                                  AM_VALUE_ACTOR_ID,
                                  cmocka_cb).actor_id));
    /* A.put("/", "key1", "val1");                                           */
    AMfree(AMmapPutStr(A, AM_ROOT, "key1", "val1"));
    /* A.put("/", "key2", "val2");                                           */
    AMfree(AMmapPutStr(A, AM_ROOT, "key2", "val2"));
    /* const heads1 = A.getHeads();                                          */
    AMchangeHashes const heads1 = AMpush(&stack,
                                         AMgetHeads(A),
                                         AM_VALUE_CHANGE_HASHES,
                                         cmocka_cb).change_hashes;
    /* const B = A.fork("bbbbbb")                                            */
    AMdoc* const B = AMpush(&stack, AMfork(A, NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMsetActorId(B, AMpush(&stack,
                                  AMactorIdInitStr("bbbbbb"),
                                  AM_VALUE_ACTOR_ID,
                                  cmocka_cb).actor_id));
    /* A.put("/", "key3", "val3");                                           */
    AMfree(AMmapPutStr(A, AM_ROOT, "key3", "val3"));
    /* B.put("/", "key4", "val4");                                           */
    AMfree(AMmapPutStr(B, AM_ROOT, "key4", "val4"));
    /* A.merge(B)                                                            */
    AMfree(AMmerge(A, B));
    /* const heads2 = A.getHeads();                                          */
    AMchangeHashes const heads2 = AMpush(&stack,
                                         AMgetHeads(A),
                                         AM_VALUE_CHANGE_HASHES,
                                         cmocka_cb).change_hashes;
    /* A.put("/", "key5", "val5");                                           */
    AMfree(AMmapPutStr(A, AM_ROOT, "key5", "val5"));
    /* assert.deepEqual(A.forkAt(heads1).materialize("/"), A.materialize("/", heads1))*/
    AMmapItems AforkAt1_items = AMpush(
        &stack,
        AMmapRange(
            AMpush(&stack, AMfork(A, &heads1), AM_VALUE_DOC, cmocka_cb).doc,
            AM_ROOT, NULL, NULL, NULL),
        AM_VALUE_MAP_ITEMS,
        cmocka_cb).map_items;
    AMmapItems A1_items = AMpush(&stack,
                                 AMmapRange(A, AM_ROOT, NULL, NULL, &heads1),
                                 AM_VALUE_MAP_ITEMS,
                                 cmocka_cb).map_items;
    assert_true(AMmapItemsEqual(&AforkAt1_items, &A1_items));
    /* assert.deepEqual(A.forkAt(heads2).materialize("/"), A.materialize("/", heads2))*/
    AMmapItems AforkAt2_items = AMpush(
        &stack,
        AMmapRange(
            AMpush(&stack, AMfork(A, &heads2), AM_VALUE_DOC, cmocka_cb).doc,
            AM_ROOT, NULL, NULL, NULL),
        AM_VALUE_MAP_ITEMS,
        cmocka_cb).map_items;
    AMmapItems A2_items = AMpush(&stack,
                                 AMmapRange(A, AM_ROOT, NULL, NULL, &heads2),
                                 AM_VALUE_MAP_ITEMS,
                                 cmocka_cb).map_items;
    assert_true(AMmapItemsEqual(&AforkAt2_items, &A2_items));
}

/**
 * \brief should handle merging text conflicts then saving & loading
 */
static void test_should_handle_merging_text_conflicts_then_saving_and_loading(void** state) {
    AMresultStack* stack = *state;
    /* const A = create("aabbcc")                                            */
    AMdoc* const A = AMpush(&stack, AMcreate(), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMsetActorId(A, AMpush(&stack,
                                  AMactorIdInitStr("aabbcc"),
                                  AM_VALUE_ACTOR_ID,
                                  cmocka_cb).actor_id));
    /* const At = A.putObject('_root', 'text', "")                           */
    AMobjId const* const At = AMpush(
        &stack,
        AMmapPutObject(A, AM_ROOT, "text", AM_OBJ_TYPE_TEXT),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    /* A.splice(At, 0, 0, 'hello')                                           */
    AMfree(AMspliceText(A, At, 0, 0, "hello"));
    /*                                                                       */
    /* const B = A.fork()                                                    */
    AMdoc* const B = AMpush(&stack, AMfork(A, NULL), AM_VALUE_DOC, cmocka_cb).doc;
    /*                                                                       */
    /* assert.deepEqual(B.getWithType("_root", "text"), ["text", At])        */
    assert_string_equal(AMpush(&stack,
                               AMtext(B,
                                      AMpush(&stack,
                                             AMmapGet(B, AM_ROOT, "text", NULL),
                                             AM_VALUE_OBJ_ID,
                                             cmocka_cb).obj_id,
                                      NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str,
                        AMpush(&stack,
                               AMtext(A, At, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str);
    /*                                                                       */
    /* B.splice(At, 4, 1)                                                    */
    AMfree(AMspliceText(B, At, 4, 1, NULL));
    /* B.splice(At, 4, 0, '!')                                               */
    AMfree(AMspliceText(B, At, 4, 0, "!"));
    /* B.splice(At, 5, 0, ' ')                                               */
    AMfree(AMspliceText(B, At, 5, 0, " "));
    /* B.splice(At, 6, 0, 'world')                                           */
    AMfree(AMspliceText(B, At, 6, 0, "world"));
    /*                                                                       */
    /* A.merge(B)                                                            */
    AMfree(AMmerge(A, B));
    /*                                                                       */
    /* const binary = A.save()                                               */
    AMbyteSpan const binary = AMpush(&stack,
                                     AMsave(A),
                                     AM_VALUE_BYTES,
                                     cmocka_cb).bytes;
    /*                                                                       */
    /* const C = load(binary)                                                */
    AMdoc* const C = AMpush(&stack,
                            AMload(binary.src, binary.count),
                            AM_VALUE_DOC,
                            cmocka_cb).doc;
    /*                                                                       */
    /* assert.deepEqual(C.getWithType('_root', 'text'), ['text', '1@aabbcc'])*/
    AMobjId const* const C_text = AMpush(&stack,
                                         AMmapGet(C, AM_ROOT, "text", NULL),
                                         AM_VALUE_OBJ_ID,
                                         cmocka_cb).obj_id;
    assert_int_equal(AMobjIdCounter(C_text), 1);
    assert_string_equal(AMactorIdStr(AMobjIdActorId(C_text)), "aabbcc");
    /* assert.deepEqual(C.text(At), 'hell! world')                           */
    assert_string_equal(AMpush(&stack,
                               AMtext(C, At, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, "hell! world");
}

int run_ported_wasm_basic_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_create_clone_and_free, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_start_and_commit, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_getting_a_nonexistent_prop_does_not_throw_an_error, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_set_and_get_a_simple_value, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_use_bytes, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_make_subobjects, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_make_lists, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_lists_have_insert_set_splice_and_push_ops, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_delete_non_existent_props, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_del, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_use_counters, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_splice_text, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_insert_objects_into_text, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_save_all_or_incrementally, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_splice_text_2, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_local_inc_increments_all_visible_counters_in_a_map, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_local_inc_increments_all_visible_counters_in_a_sequence, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_fetch_changes_by_hash, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_recursive_sets_are_possible, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_only_returns_an_object_id_when_objects_are_created, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_objects_without_properties_are_preserved, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_should_allow_you_to_forkAt_a_heads, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_should_handle_merging_text_conflicts_then_saving_and_loading, setup_stack, teardown_stack)
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
