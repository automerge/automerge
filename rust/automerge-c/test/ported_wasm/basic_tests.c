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
#include <automerge-c/automerge.h>
#include <automerge-c/utils/result.h>
#include <automerge-c/utils/stack_callback_data.h>
#include "../base_state.h"
#include "../cmocka_utils.h"

/**
 * \brief default import init() should return a promise
 */
static void test_default_import_init_should_return_a_promise(void** state);

/**
 * \brief should create, clone and free
 */
static void test_create_clone_and_free(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc1 = create()                                                 */
    AMdoc* doc1;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc1));
    /* const doc2 = doc1.clone()                                             */
    AMdoc* doc2;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMclone(doc1), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc2));
}

/**
 * \brief should be able to start and commit
 */
static void test_start_and_commit(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc = create()                                                  */
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /* doc.commit()                                                          */
    AMstackItems(stack_ptr, AMemptyChange(doc, AMstr(NULL), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
}

/**
 * \brief getting a nonexistent prop does not throw an error
 */
static void test_getting_a_nonexistent_prop_does_not_throw_an_error(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc = create()                                                  */
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /* const root = "_root"                                                  */
    /* const result = doc.getWithType(root, "hello")                         */
    /* assert.deepEqual(result, undefined)                                   */
    AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("hello"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
}

/**
 * \brief should be able to set and get a simple value
 */
static void test_should_be_able_to_set_and_get_a_simple_value(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc: Automerge = create("aabbcc")                               */
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("aabbcc")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /* const root = "_root"                                                  */
    /* let result                                                            */
    /*                                                                       */
    /* doc.put(root, "hello", "world")                                       */
    AMstackItem(NULL, AMmapPutStr(doc, AM_ROOT, AMstr("hello"), AMstr("world")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.put(root, "number1", 5, "uint")                                   */
    AMstackItem(NULL, AMmapPutUint(doc, AM_ROOT, AMstr("number1"), 5), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.put(root, "number2", 5)                                           */
    AMstackItem(NULL, AMmapPutInt(doc, AM_ROOT, AMstr("number2"), 5), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.put(root, "number3", 5.5)                                         */
    AMstackItem(NULL, AMmapPutF64(doc, AM_ROOT, AMstr("number3"), 5.5), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.put(root, "number4", 5.5, "f64")                                  */
    AMstackItem(NULL, AMmapPutF64(doc, AM_ROOT, AMstr("number4"), 5.5), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.put(root, "number5", 5.5, "int")                                  */
    AMstackItem(NULL, AMmapPutInt(doc, AM_ROOT, AMstr("number5"), 5.5), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.put(root, "bool", true)                                           */
    AMstackItem(NULL, AMmapPutBool(doc, AM_ROOT, AMstr("bool"), true), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.put(root, "time1", 1000, "timestamp")                             */
    AMstackItem(NULL, AMmapPutTimestamp(doc, AM_ROOT, AMstr("time1"), 1000), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.put(root, "time2", new Date(1001))                                */
    AMstackItem(NULL, AMmapPutTimestamp(doc, AM_ROOT, AMstr("time2"), 1001), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.putObject(root, "list", []);                                      */
    AMstackItem(NULL, AMmapPutObject(doc, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST), cmocka_cb,
                AMexpect(AM_VAL_TYPE_OBJ_TYPE));
    /* doc.put(root, "null", null)                                           */
    AMstackItem(NULL, AMmapPutNull(doc, AM_ROOT, AMstr("null")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*                                                                       */
    /* result = doc.getWithType(root, "hello")                               */
    /* assert.deepEqual(result, ["str", "world"])                            */
    /* assert.deepEqual(doc.get("/", "hello"), "world")                      */
    AMbyteSpan str;
    assert_true(AMitemToStr(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("hello"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)),
        &str));
    assert_int_equal(str.count, strlen("world"));
    assert_memory_equal(str.src, "world", str.count);
    /* assert.deepEqual(doc.get("/", "hello"), "world")                      */
    /*                                                                       */
    /* result = doc.getWithType(root, "number1")                             */
    /* assert.deepEqual(result, ["uint", 5])                                 */
    uint64_t uint;
    assert_true(AMitemToUint(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("number1"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_UINT)),
        &uint));
    assert_int_equal(uint, 5);
    /* assert.deepEqual(doc.get("/", "number1"), 5)                          */
    /*                                                                       */
    /* result = doc.getWithType(root, "number2")                             */
    /* assert.deepEqual(result, ["int", 5])                                  */
    int64_t int_;
    assert_true(AMitemToInt(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("number2"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_INT)),
        &int_));
    assert_int_equal(int_, 5);
    /*                                                                       */
    /* result = doc.getWithType(root, "number3")                             */
    /* assert.deepEqual(result, ["f64", 5.5])                                */
    double f64;
    assert_true(AMitemToF64(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("number3"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_F64)),
        &f64));
    assert_float_equal(f64, 5.5, DBL_EPSILON);
    /*                                                                       */
    /* result = doc.getWithType(root, "number4")                             */
    /* assert.deepEqual(result, ["f64", 5.5])                                */
    assert_true(AMitemToF64(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("number4"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_F64)),
        &f64));
    assert_float_equal(f64, 5.5, DBL_EPSILON);
    /*                                                                       */
    /* result = doc.getWithType(root, "number5")                             */
    /* assert.deepEqual(result, ["int", 5])                                  */
    assert_true(AMitemToInt(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("number5"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_INT)),
        &int_));
    assert_int_equal(int_, 5);
    /*                                                                       */
    /* result = doc.getWithType(root, "bool")                                */
    /* assert.deepEqual(result, ["boolean", true])                           */
    bool boolean;
    assert_true(AMitemToBool(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("bool"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_BOOL)),
        &boolean));
    assert_true(boolean);
    /*                                                                       */
    /* doc.put(root, "bool", false, "boolean")                               */
    AMstackItem(NULL, AMmapPutBool(doc, AM_ROOT, AMstr("bool"), false), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*                                                                       */
    /* result = doc.getWithType(root, "bool")                                */
    /* assert.deepEqual(result, ["boolean", false])                          */
    assert_true(AMitemToBool(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("bool"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_BOOL)),
        &boolean));
    assert_false(boolean);
    /*                                                                       */
    /* result = doc.getWithType(root, "time1")                               */
    /* assert.deepEqual(result, ["timestamp", new Date(1000)])               */
    int64_t timestamp;
    assert_true(AMitemToTimestamp(AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("time1"), NULL), cmocka_cb,
                                              AMexpect(AM_VAL_TYPE_TIMESTAMP)),
                                  &timestamp));
    assert_int_equal(timestamp, 1000);
    /*                                                                       */
    /* result = doc.getWithType(root, "time2")                               */
    /* assert.deepEqual(result, ["timestamp", new Date(1001)])               */
    assert_true(AMitemToTimestamp(AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("time2"), NULL), cmocka_cb,
                                              AMexpect(AM_VAL_TYPE_TIMESTAMP)),
                                  &timestamp));
    assert_int_equal(timestamp, 1001);
    /*                                                                       */
    /* result = doc.getWithType(root, "list")                                */
    /* assert.deepEqual(result, ["list", "10@aabbcc"]);                      */
    AMobjId const* const list = AMitemObjId(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("list"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    assert_int_equal(AMobjIdCounter(list), 10);
    str = AMactorIdStr(AMobjIdActorId(list));
    assert_int_equal(str.count, strlen("aabbcc"));
    assert_memory_equal(str.src, "aabbcc", str.count);
    /*                                                                       */
    /* result = doc.getWithType(root, "null")                                */
    /* assert.deepEqual(result, ["null", null]);                             */
    AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("null"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_NULL));
}

/**
 * \brief should be able to use bytes
 */
static void test_should_be_able_to_use_bytes(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc = create()                                                  */
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /* doc.put("_root", "data1", new Uint8Array([10, 11, 12]));              */
    static uint8_t const DATA1[] = {10, 11, 12};
    AMstackItem(NULL, AMmapPutBytes(doc, AM_ROOT, AMstr("data1"), AMbytes(DATA1, sizeof(DATA1))), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* doc.put("_root", "data2", new Uint8Array([13, 14, 15]), "bytes");     */
    static uint8_t const DATA2[] = {13, 14, 15};
    AMstackItem(NULL, AMmapPutBytes(doc, AM_ROOT, AMstr("data2"), AMbytes(DATA2, sizeof(DATA2))), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* const value1 = doc.getWithType("_root", "data1")                      */
    AMbyteSpan value1;
    assert_true(AMitemToBytes(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("data1"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)),
        &value1));
    /* assert.deepEqual(value1, ["bytes", new Uint8Array([10, 11, 12])]);    */
    assert_int_equal(value1.count, sizeof(DATA1));
    assert_memory_equal(value1.src, DATA1, sizeof(DATA1));
    /* const value2 = doc.getWithType("_root", "data2")                      */
    AMbyteSpan value2;
    assert_true(AMitemToBytes(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("data2"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)),
        &value2));
    /* assert.deepEqual(value2, ["bytes", new Uint8Array([13, 14, 15])]);    */
    assert_int_equal(value2.count, sizeof(DATA2));
    assert_memory_equal(value2.src, DATA2, sizeof(DATA2));
}

/**
 * \brief should be able to make subobjects
 */
static void test_should_be_able_to_make_subobjects(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc = create()                                                  */
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /* const root = "_root"                                                  */
    /* let result                                                            */
    /*                                                                       */
    /* const submap = doc.putObject(root, "submap", {})                      */
    AMobjId const* const submap =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("submap"), AM_OBJ_TYPE_MAP), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* doc.put(submap, "number", 6, "uint")                                  */
    AMstackItem(NULL, AMmapPutUint(doc, submap, AMstr("number"), 6), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.strictEqual(doc.pendingOps(), 2)                               */
    assert_int_equal(AMpendingOps(doc), 2);
    /*                                                                       */
    /* result = doc.getWithType(root, "submap")                              */
    /* assert.deepEqual(result, ["map", submap])                             */
    assert_true(AMobjIdEqual(AMitemObjId(AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("submap"), NULL),
                                                     cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE))),
                             submap));
    /*                                                                       */
    /* result = doc.getWithType(submap, "number")                            */
    /* assert.deepEqual(result, ["uint", 6])                                 */
    uint64_t uint;
    assert_true(AMitemToUint(
        AMstackItem(stack_ptr, AMmapGet(doc, submap, AMstr("number"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_UINT)),
        &uint));
    assert_int_equal(uint, 6);
}

/**
 * \brief should be able to make lists
 */
static void test_should_be_able_to_make_lists(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc = create()                                                  */
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /* const root = "_root"                                                  */
    /*                                                                       */
    /* const sublist = doc.putObject(root, "numbers", [])                    */
    AMobjId const* const sublist =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("numbers"), AM_OBJ_TYPE_LIST), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* doc.insert(sublist, 0, "a");                                          */
    AMstackItem(NULL, AMlistPutStr(doc, sublist, 0, true, AMstr("a")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.insert(sublist, 1, "b");                                          */
    AMstackItem(NULL, AMlistPutStr(doc, sublist, 1, true, AMstr("b")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.insert(sublist, 2, "c");                                          */
    AMstackItem(NULL, AMlistPutStr(doc, sublist, 2, true, AMstr("c")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.insert(sublist, 0, "z");                                          */
    AMstackItem(NULL, AMlistPutStr(doc, sublist, 0, true, AMstr("z")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*                                                                       */
    /* assert.deepEqual(doc.getWithType(sublist, 0), ["str", "z"])           */
    AMbyteSpan str;
    assert_true(AMitemToStr(
        AMstackItem(stack_ptr, AMlistGet(doc, sublist, 0, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "z", str.count);
    /* assert.deepEqual(doc.getWithType(sublist, 1), ["str", "a"])           */
    assert_true(AMitemToStr(
        AMstackItem(stack_ptr, AMlistGet(doc, sublist, 1, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "a", str.count);
    /* assert.deepEqual(doc.getWithType(sublist, 2), ["str", "b"])           */
    assert_true(AMitemToStr(
        AMstackItem(stack_ptr, AMlistGet(doc, sublist, 2, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "b", str.count);
    /* assert.deepEqual(doc.getWithType(sublist, 3), ["str", "c"])           */
    assert_true(AMitemToStr(
        AMstackItem(stack_ptr, AMlistGet(doc, sublist, 3, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "c", str.count);
    /* assert.deepEqual(doc.length(sublist), 4)                              */
    assert_int_equal(AMobjSize(doc, sublist, NULL), 4);
    /*                                                                       */
    /* doc.put(sublist, 2, "b v2");                                          */
    AMstackItem(NULL, AMlistPutStr(doc, sublist, 2, false, AMstr("b v2")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*                                                                       */
    /* assert.deepEqual(doc.getWithType(sublist, 2), ["str", "b v2"])        */
    assert_true(AMitemToStr(
        AMstackItem(stack_ptr, AMlistGet(doc, sublist, 2, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, 4);
    assert_memory_equal(str.src, "b v2", str.count);
    /* assert.deepEqual(doc.length(sublist), 4)                              */
    assert_int_equal(AMobjSize(doc, sublist, NULL), 4);
}

/**
 * \brief lists have insert, set, splice, and push ops
 */
static void test_lists_have_insert_set_splice_and_push_ops(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc = create()                                                  */
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /* const root = "_root"                                                  */
    /*                                                                       */
    /* const sublist = doc.putObject(root, "letters", [])                    */
    AMobjId const* const sublist =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("letters"), AM_OBJ_TYPE_LIST), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* doc.insert(sublist, 0, "a");                                          */
    AMstackItem(NULL, AMlistPutStr(doc, sublist, 0, true, AMstr("a")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.insert(sublist, 0, "b");                                          */
    AMstackItem(NULL, AMlistPutStr(doc, sublist, 0, true, AMstr("b")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepEqual(doc.materialize(), { letters: ["b", "a"] })          */
    AMitem* doc_item = AMstackItem(stack_ptr, AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                                   AMexpect(AM_VAL_TYPE_OBJ_TYPE));
    assert_int_equal(AMitemIdxType(doc_item), AM_IDX_TYPE_KEY);
    AMbyteSpan key;
    assert_true(AMitemKey(doc_item, &key));
    assert_int_equal(key.count, strlen("letters"));
    assert_memory_equal(key.src, "letters", key.count);
    {
        AMitems list_items = AMstackItems(stack_ptr, AMlistRange(doc, AMitemObjId(doc_item), 0, SIZE_MAX, NULL),
                                          cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
        assert_int_equal(AMitemsSize(&list_items), 2);
        AMbyteSpan str;
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "b", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "a", str.count);
        assert_null(AMitemsNext(&list_items, 1));
    }
    /* doc.push(sublist, "c");                                               */
    AMstackItem(NULL, AMlistPutStr(doc, sublist, SIZE_MAX, true, AMstr("c")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const heads = doc.getHeads()                                          */
    AMitems const heads = AMstackItems(stack_ptr, AMgetHeads(doc), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* assert.deepEqual(doc.materialize(), { letters: ["b", "a", "c"] })     */
    doc_item = AMstackItem(stack_ptr, AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                           AMexpect(AM_VAL_TYPE_OBJ_TYPE));
    assert_int_equal(AMitemIdxType(doc_item), AM_IDX_TYPE_KEY);
    assert_true(AMitemKey(doc_item, &key));
    assert_int_equal(key.count, strlen("letters"));
    assert_memory_equal(key.src, "letters", key.count);
    {
        AMitems list_items = AMstackItems(stack_ptr, AMlistRange(doc, AMitemObjId(doc_item), 0, SIZE_MAX, NULL),
                                          cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
        assert_int_equal(AMitemsSize(&list_items), 3);
        AMbyteSpan str;
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "b", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "a", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "c", str.count);
        assert_null(AMitemsNext(&list_items, 1));
    }
    /* doc.push(sublist, 3, "timestamp");                                    */
    AMstackItem(NULL, AMlistPutTimestamp(doc, sublist, SIZE_MAX, true, 3), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepEqual(doc.materialize(), { letters: ["b", "a", "c", new
     * Date(3)] } */
    doc_item = AMstackItem(stack_ptr, AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                           AMexpect(AM_VAL_TYPE_OBJ_TYPE));
    assert_int_equal(AMitemIdxType(doc_item), AM_IDX_TYPE_KEY);
    assert_true(AMitemKey(doc_item, &key));
    assert_int_equal(key.count, strlen("letters"));
    assert_memory_equal(key.src, "letters", key.count);
    {
        AMitems list_items = AMstackItems(stack_ptr, AMlistRange(doc, AMitemObjId(doc_item), 0, SIZE_MAX, NULL),
                                          cmocka_cb, AMexpect(AM_VAL_TYPE_STR | AM_VAL_TYPE_TIMESTAMP));
        assert_int_equal(AMitemsSize(&list_items), 4);
        AMbyteSpan str;
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "b", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "a", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "c", str.count);
        int64_t timestamp;
        assert_true(AMitemToTimestamp(AMitemsNext(&list_items, 1), &timestamp));
        assert_int_equal(timestamp, 3);
        assert_null(AMitemsNext(&list_items, 1));
    }
    /* doc.splice(sublist, 1, 1, ["d", "e", "f"]);                           */
    AMresult* data = AMstackResult(
        stack_ptr, AMresultFrom(3, AMitemFromStr(AMstr("d")), AMitemFromStr(AMstr("e")), AMitemFromStr(AMstr("f"))),
        NULL, NULL);
    AMstackItem(NULL, AMsplice(doc, sublist, 1, 1, AMresultItems(data)), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepEqual(doc.materialize(), { letters: ["b", "d", "e", "f", "c",
     * new Date(3)] } */
    doc_item = AMstackItem(stack_ptr, AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                           AMexpect(AM_VAL_TYPE_OBJ_TYPE));
    assert_int_equal(AMitemIdxType(doc_item), AM_IDX_TYPE_KEY);
    assert_true(AMitemKey(doc_item, &key));
    assert_int_equal(key.count, strlen("letters"));
    assert_memory_equal(key.src, "letters", key.count);
    {
        AMitems list_items = AMstackItems(stack_ptr, AMlistRange(doc, AMitemObjId(doc_item), 0, SIZE_MAX, NULL),
                                          cmocka_cb, AMexpect(AM_VAL_TYPE_STR | AM_VAL_TYPE_TIMESTAMP));
        AMbyteSpan str;
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "b", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "d", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "e", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "f", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "c", str.count);
        int64_t timestamp;
        assert_true(AMitemToTimestamp(AMitemsNext(&list_items, 1), &timestamp));
        assert_int_equal(timestamp, 3);
        assert_null(AMitemsNext(&list_items, 1));
    }
    /* doc.put(sublist, 0, "z");                                             */
    AMstackItem(NULL, AMlistPutStr(doc, sublist, 0, false, AMstr("z")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepEqual(doc.materialize(), { letters: ["z", "d", "e", "f", "c",
     * new Date(3)] } */
    doc_item = AMstackItem(stack_ptr, AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                           AMexpect(AM_VAL_TYPE_OBJ_TYPE));
    assert_int_equal(AMitemIdxType(doc_item), AM_IDX_TYPE_KEY);
    assert_true(AMitemKey(doc_item, &key));
    assert_int_equal(key.count, strlen("letters"));
    assert_memory_equal(key.src, "letters", key.count);
    {
        AMitems list_items = AMstackItems(stack_ptr, AMlistRange(doc, AMitemObjId(doc_item), 0, SIZE_MAX, NULL),
                                          cmocka_cb, AMexpect(AM_VAL_TYPE_STR | AM_VAL_TYPE_TIMESTAMP));
        AMbyteSpan str;
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "z", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "d", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "e", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "f", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "c", str.count);
        int64_t timestamp;
        assert_true(AMitemToTimestamp(AMitemsNext(&list_items, 1), &timestamp));
        assert_int_equal(timestamp, 3);
        assert_null(AMitemsNext(&list_items, 1));
    }
    /* assert.deepEqual(doc.materialize(sublist), ["z", "d", "e", "f", "c", new
     * Date(3)] */
    AMitems sublist_items = AMstackItems(stack_ptr, AMlistRange(doc, sublist, 0, SIZE_MAX, NULL), cmocka_cb,
                                         AMexpect(AM_VAL_TYPE_STR | AM_VAL_TYPE_TIMESTAMP));
    AMbyteSpan str;
    assert_true(AMitemToStr(AMitemsNext(&sublist_items, 1), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "z", str.count);
    assert_true(AMitemToStr(AMitemsNext(&sublist_items, 1), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "d", str.count);
    assert_true(AMitemToStr(AMitemsNext(&sublist_items, 1), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "e", str.count);
    assert_true(AMitemToStr(AMitemsNext(&sublist_items, 1), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "f", str.count);
    assert_true(AMitemToStr(AMitemsNext(&sublist_items, 1), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "c", str.count);
    int64_t timestamp;
    assert_true(AMitemToTimestamp(AMitemsNext(&sublist_items, 1), &timestamp));
    assert_int_equal(timestamp, 3);
    assert_null(AMitemsNext(&sublist_items, 1));
    /* assert.deepEqual(doc.length(sublist), 6)                              */
    assert_int_equal(AMobjSize(doc, sublist, NULL), 6);
    /* assert.deepEqual(doc.materialize("/", heads), { letters: ["b", "a", "c"]
     * } */
    doc_item = AMstackItem(stack_ptr, AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr(NULL), &heads), cmocka_cb,
                           AMexpect(AM_VAL_TYPE_OBJ_TYPE));
    assert_int_equal(AMitemIdxType(doc_item), AM_IDX_TYPE_KEY);
    assert_true(AMitemKey(doc_item, &key));
    assert_int_equal(key.count, strlen("letters"));
    assert_memory_equal(key.src, "letters", key.count);
    {
        AMitems list_items = AMstackItems(stack_ptr, AMlistRange(doc, AMitemObjId(doc_item), 0, SIZE_MAX, &heads),
                                          cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
        AMbyteSpan str;
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "b", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "a", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "c", str.count);
        assert_null(AMitemsNext(&list_items, 1));
    }
}

/**
 * \brief should be able to delete non-existent props
 */
static void test_should_be_able_to_delete_non_existent_props(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc = create()                                                  */
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /*                                                                       */
    /* doc.put("_root", "foo", "bar")                                        */
    AMstackItem(NULL, AMmapPutStr(doc, AM_ROOT, AMstr("foo"), AMstr("bar")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.put("_root", "bip", "bap")                                        */
    AMstackItem(NULL, AMmapPutStr(doc, AM_ROOT, AMstr("bip"), AMstr("bap")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const hash1 = doc.commit()                                            */
    AMitems const hash1 =
        AMstackItems(stack_ptr, AMcommit(doc, AMstr(NULL), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /*                                                                       */
    /* assert.deepEqual(doc.keys("_root"), ["bip", "foo"])                   */
    AMitems keys = AMstackItems(stack_ptr, AMkeys(doc, AM_ROOT, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    AMbyteSpan str;
    assert_true(AMitemToStr(AMitemsNext(&keys, 1), &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "bip", str.count);
    assert_true(AMitemToStr(AMitemsNext(&keys, 1), &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "foo", str.count);
    /*                                                                       */
    /* doc.delete("_root", "foo")                                            */
    AMstackItem(NULL, AMmapDelete(doc, AM_ROOT, AMstr("foo")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.delete("_root", "baz")                                            */
    AMstackItem(NULL, AMmapDelete(doc, AM_ROOT, AMstr("baz")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const hash2 = doc.commit()                                            */
    AMitems const hash2 =
        AMstackItems(stack_ptr, AMcommit(doc, AMstr(NULL), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /*                                                                       */
    /* assert.deepEqual(doc.keys("_root"), ["bip"])                          */
    keys = AMstackItems(stack_ptr, AMkeys(doc, AM_ROOT, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_true(AMitemToStr(AMitemsNext(&keys, 1), &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "bip", str.count);
    /* assert.deepEqual(doc.keys("_root", [hash1]), ["bip", "foo"])          */
    keys = AMstackItems(stack_ptr, AMkeys(doc, AM_ROOT, &hash1), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_true(AMitemToStr(AMitemsNext(&keys, 1), &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "bip", str.count);
    assert_true(AMitemToStr(AMitemsNext(&keys, 1), &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "foo", str.count);
    /* assert.deepEqual(doc.keys("_root", [hash2]), ["bip"])                 */
    keys = AMstackItems(stack_ptr, AMkeys(doc, AM_ROOT, &hash2), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_true(AMitemToStr(AMitemsNext(&keys, 1), &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "bip", str.count);
}

/**
 * \brief should be able to del
 */
static void test_should_be_able_to_del(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc = create()                                                  */
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /* const root = "_root"                                                  */
    /*                                                                       */
    /* doc.put(root, "xxx", "xxx");                                          */
    AMstackItem(NULL, AMmapPutStr(doc, AM_ROOT, AMstr("xxx"), AMstr("xxx")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepEqual(doc.getWithType(root, "xxx"), ["str", "xxx"])        */
    AMbyteSpan str;
    assert_true(AMitemToStr(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("xxx"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)),
        &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "xxx", str.count);
    /* doc.delete(root, "xxx");                                              */
    AMstackItem(NULL, AMmapDelete(doc, AM_ROOT, AMstr("xxx")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepEqual(doc.getWithType(root, "xxx"), undefined)             */
    AMstackItem(NULL, AMmapGet(doc, AM_ROOT, AMstr("xxx"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
}

/**
 * \brief should be able to use counters
 */
static void test_should_be_able_to_use_counters(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc = create()                                                  */
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /* const root = "_root"                                                  */
    /*                                                                       */
    /* doc.put(root, "counter", 10, "counter");                              */
    AMstackItem(NULL, AMmapPutCounter(doc, AM_ROOT, AMstr("counter"), 10), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepEqual(doc.getWithType(root, "counter"), ["counter", 10])   */
    int64_t counter;
    assert_true(AMitemToCounter(AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("counter"), NULL), cmocka_cb,
                                            AMexpect(AM_VAL_TYPE_COUNTER)),
                                &counter));
    assert_int_equal(counter, 10);
    /* doc.increment(root, "counter", 10);                                   */
    AMstackItem(NULL, AMmapIncrement(doc, AM_ROOT, AMstr("counter"), 10), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepEqual(doc.getWithType(root, "counter"), ["counter", 20])   */
    assert_true(AMitemToCounter(AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("counter"), NULL), cmocka_cb,
                                            AMexpect(AM_VAL_TYPE_COUNTER)),
                                &counter));
    assert_int_equal(counter, 20);
    /* doc.increment(root, "counter", -5);                                   */
    AMstackItem(NULL, AMmapIncrement(doc, AM_ROOT, AMstr("counter"), -5), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepEqual(doc.getWithType(root, "counter"), ["counter", 15])   */
    assert_true(AMitemToCounter(AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("counter"), NULL), cmocka_cb,
                                            AMexpect(AM_VAL_TYPE_COUNTER)),
                                &counter));
    assert_int_equal(counter, 15);
}

/**
 * \brief should be able to splice text
 */
static void test_should_be_able_to_splice_text(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc = create()                                                  */
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /* const root = "_root";                                                 */
    /*                                                                       */
    /* const text = doc.putObject(root, "text", "");                         */
    AMobjId const* const text =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("text"), AM_OBJ_TYPE_TEXT), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* doc.splice(text, 0, 0, "hello ")                                      */
    AMstackItem(NULL, AMspliceText(doc, text, 0, 0, AMstr("hello ")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.splice(text, 6, 0, "world")                                       */
    AMstackItem(NULL, AMspliceText(doc, text, 6, 0, AMstr("world")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.splice(text, 11, 0, "!?")                                         */
    AMstackItem(NULL, AMspliceText(doc, text, 11, 0, AMstr("!?")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepEqual(doc.getWithType(text, 0), ["str", "h"])              */
    AMbyteSpan str;
    assert_true(
        AMitemToStr(AMstackItem(stack_ptr, AMlistGet(doc, text, 0, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "h", str.count);
    /* assert.deepEqual(doc.getWithType(text, 1), ["str", "e"])              */
    assert_true(
        AMitemToStr(AMstackItem(stack_ptr, AMlistGet(doc, text, 1, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "e", str.count);
    /* assert.deepEqual(doc.getWithType(text, 9), ["str", "l"])              */
    assert_true(
        AMitemToStr(AMstackItem(stack_ptr, AMlistGet(doc, text, 9, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "l", str.count);
    /* assert.deepEqual(doc.getWithType(text, 10), ["str", "d"])             */
    assert_true(AMitemToStr(
        AMstackItem(stack_ptr, AMlistGet(doc, text, 10, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "d", str.count);
    /* assert.deepEqual(doc.getWithType(text, 11), ["str", "!"])             */
    assert_true(AMitemToStr(
        AMstackItem(stack_ptr, AMlistGet(doc, text, 11, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "!", str.count);
    /* assert.deepEqual(doc.getWithType(text, 12), ["str", "?"])             */
    assert_true(AMitemToStr(
        AMstackItem(stack_ptr, AMlistGet(doc, text, 12, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "?", str.count);
}

/**
 * \brief should be able to save all or incrementally
 */
static void test_should_be_able_to_save_all_or_incrementally(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc = create()                                                  */
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /*                                                                       */
    /* doc.put("_root", "foo", 1)                                            */
    AMstackItem(NULL, AMmapPutInt(doc, AM_ROOT, AMstr("foo"), 1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*                                                                       */
    /* const save1 = doc.save()                                              */
    AMbyteSpan save1;
    assert_true(AMitemToBytes(AMstackItem(stack_ptr, AMsave(doc), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &save1));
    /*                                                                       */
    /* doc.put("_root", "bar", 2)                                            */
    AMstackItem(NULL, AMmapPutInt(doc, AM_ROOT, AMstr("bar"), 2), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*                                                                       */
    /* const saveMidway = doc.clone().save();                                */
    AMdoc* doc_clone;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMclone(doc), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc_clone));
    AMbyteSpan saveMidway;
    assert_true(
        AMitemToBytes(AMstackItem(stack_ptr, AMsave(doc_clone), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &saveMidway));
    /*                                                                       */
    /* const save2 = doc.saveIncremental();                                  */
    AMbyteSpan save2;
    assert_true(
        AMitemToBytes(AMstackItem(stack_ptr, AMsaveIncremental(doc), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &save2));
    /*                                                                       */
    /* doc.put("_root", "baz", 3);                                           */
    AMstackItem(NULL, AMmapPutInt(doc, AM_ROOT, AMstr("baz"), 3), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*                                                                       */
    /* const save3 = doc.saveIncremental();                                  */
    AMbyteSpan save3;
    assert_true(
        AMitemToBytes(AMstackItem(stack_ptr, AMsaveIncremental(doc), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &save3));
    /*                                                                       */
    /* const saveA = doc.save();                                             */
    AMbyteSpan saveA;
    assert_true(AMitemToBytes(AMstackItem(stack_ptr, AMsave(doc), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &saveA));
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
    AMdoc* docA;
    assert_true(AMitemToDoc(
        AMstackItem(stack_ptr, AMload(saveA.src, saveA.count), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &docA));
    /* const docB = load(saveB);                                             */
    AMdoc* docB;
    assert_true(AMitemToDoc(
        AMstackItem(stack_ptr, AMload(saveB_src, saveB_count), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &docB));
    test_free(saveB_src);
    /* const docC = load(saveMidway)                                         */
    AMdoc* docC;
    assert_true(AMitemToDoc(
        AMstackItem(stack_ptr, AMload(saveMidway.src, saveMidway.count), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &docC));
    /* docC.loadIncremental(save3)                                           */
    AMstackItem(NULL, AMloadIncremental(docC, save3.src, save3.count), cmocka_cb, AMexpect(AM_VAL_TYPE_UINT));
    /*                                                                       */
    /* assert.deepEqual(docA.keys("_root"), docB.keys("_root"));             */
    AMitems const keysA = AMstackItems(stack_ptr, AMkeys(docA, AM_ROOT, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    AMitems const keysB = AMstackItems(stack_ptr, AMkeys(docB, AM_ROOT, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_true(AMitemsEqual(&keysA, &keysB));
    /* assert.deepEqual(docA.save(), docB.save());                           */
    AMbyteSpan docA_save;
    assert_true(
        AMitemToBytes(AMstackItem(stack_ptr, AMsave(docA), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &docA_save));
    AMbyteSpan docB_save;
    assert_true(
        AMitemToBytes(AMstackItem(stack_ptr, AMsave(docB), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &docB_save));
    assert_int_equal(docA_save.count, docB_save.count);
    assert_memory_equal(docA_save.src, docB_save.src, docA_save.count);
    /* assert.deepEqual(docA.save(), docC.save());                           */
    AMbyteSpan docC_save;
    assert_true(
        AMitemToBytes(AMstackItem(stack_ptr, AMsave(docC), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &docC_save));
    assert_int_equal(docA_save.count, docC_save.count);
    assert_memory_equal(docA_save.src, docC_save.src, docA_save.count);
}

/**
 * \brief should be able to splice text #2
 */
static void test_should_be_able_to_splice_text_2(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc = create()                                                  */
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /* const text = doc.putObject("_root", "text", "");                      */
    AMobjId const* const text =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("text"), AM_OBJ_TYPE_TEXT), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* doc.splice(text, 0, 0, "hello world");                                */
    AMstackItem(NULL, AMspliceText(doc, text, 0, 0, AMstr("hello world")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const hash1 = doc.commit();                                           */
    AMitems const hash1 =
        AMstackItems(stack_ptr, AMcommit(doc, AMstr(NULL), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* doc.splice(text, 6, 0, "big bad ");                                   */
    AMstackItem(NULL, AMspliceText(doc, text, 6, 0, AMstr("big bad ")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const hash2 = doc.commit();                                           */
    AMitems const hash2 =
        AMstackItems(stack_ptr, AMcommit(doc, AMstr(NULL), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* assert.strictEqual(doc.text(text), "hello big bad world")             */
    AMbyteSpan str;
    assert_true(
        AMitemToStr(AMstackItem(stack_ptr, AMtext(doc, text, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, strlen("hello big bad world"));
    assert_memory_equal(str.src, "hello big bad world", str.count);
    /* assert.strictEqual(doc.length(text), 19)                              */
    assert_int_equal(AMobjSize(doc, text, NULL), 19);
    /* assert.strictEqual(doc.text(text, [hash1]), "hello world")            */
    assert_true(
        AMitemToStr(AMstackItem(stack_ptr, AMtext(doc, text, &hash1), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, strlen("hello world"));
    assert_memory_equal(str.src, "hello world", str.count);
    /* assert.strictEqual(doc.length(text, [hash1]), 11)                     */
    assert_int_equal(AMobjSize(doc, text, &hash1), 11);
    /* assert.strictEqual(doc.text(text, [hash2]), "hello big bad world")    */
    assert_true(
        AMitemToStr(AMstackItem(stack_ptr, AMtext(doc, text, &hash2), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, strlen("hello big bad world"));
    assert_memory_equal(str.src, "hello big bad world", str.count);
    /* assert.strictEqual(doc.length(text, [hash2]), 19)                     */
    assert_int_equal(AMobjSize(doc, text, &hash2), 19);
}

/**
 * \brief local inc increments all visible counters in a map
 */
static void test_local_inc_increments_all_visible_counters_in_a_map(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc1 = create("aaaa")                                           */
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("aaaa")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)), &actor_id));
    AMdoc* doc1;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc1));
    /* doc1.put("_root", "hello", "world")                                   */
    AMstackItem(NULL, AMmapPutStr(doc1, AM_ROOT, AMstr("hello"), AMstr("world")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* const doc2 = load(doc1.save(), "bbbb");                               */
    AMbyteSpan save;
    assert_true(AMitemToBytes(AMstackItem(stack_ptr, AMsave(doc1), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &save));
    AMdoc* doc2;
    assert_true(
        AMitemToDoc(AMstackItem(stack_ptr, AMload(save.src, save.count), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc2));
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("bbbb")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)), &actor_id));
    AMstackItem(NULL, AMsetActorId(doc2, actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const doc3 = load(doc1.save(), "cccc");                               */
    AMdoc* doc3;
    assert_true(
        AMitemToDoc(AMstackItem(stack_ptr, AMload(save.src, save.count), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc3));
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("cccc")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)), &actor_id));
    AMstackItem(NULL, AMsetActorId(doc3, actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* let heads = doc1.getHeads()                                           */
    AMitems const heads1 = AMstackItems(stack_ptr, AMgetHeads(doc1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* doc1.put("_root", "cnt", 20)                                          */
    AMstackItem(NULL, AMmapPutInt(doc1, AM_ROOT, AMstr("cnt"), 20), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc2.put("_root", "cnt", 0, "counter")                                */
    AMstackItem(NULL, AMmapPutCounter(doc2, AM_ROOT, AMstr("cnt"), 0), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc3.put("_root", "cnt", 10, "counter")                               */
    AMstackItem(NULL, AMmapPutCounter(doc3, AM_ROOT, AMstr("cnt"), 10), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc1.applyChanges(doc2.getChanges(heads))                             */
    AMitems const changes2 =
        AMstackItems(stack_ptr, AMgetChanges(doc2, &heads1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE));
    AMstackItem(NULL, AMapplyChanges(doc1, &changes2), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc1.applyChanges(doc3.getChanges(heads))                             */
    AMitems const changes3 =
        AMstackItems(stack_ptr, AMgetChanges(doc3, &heads1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE));
    AMstackItem(NULL, AMapplyChanges(doc1, &changes3), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* let result = doc1.getAll("_root", "cnt")                              */
    AMitems result = AMstackItems(stack_ptr, AMmapGetAll(doc1, AM_ROOT, AMstr("cnt"), NULL), cmocka_cb,
                                  AMexpect(AM_VAL_TYPE_COUNTER | AM_VAL_TYPE_INT | AM_VAL_TYPE_STR));
    /* assert.deepEqual(result, [
         ['int', 20, '2@aaaa'],
         ['counter', 0, '2@bbbb'],
         ['counter', 10, '2@cccc'],
       ])                                                                    */
    AMitem* result_item = AMitemsNext(&result, 1);
    int64_t int_;
    assert_true(AMitemToInt(result_item, &int_));
    assert_int_equal(int_, 20);
    assert_int_equal(AMobjIdCounter(AMitemObjId(result_item)), 2);
    AMbyteSpan str = AMactorIdStr(AMobjIdActorId(AMitemObjId(result_item)));
    assert_int_equal(str.count, 4);
    assert_memory_equal(str.src, "aaaa", str.count);
    result_item = AMitemsNext(&result, 1);
    int64_t counter;
    assert_true(AMitemToCounter(result_item, &counter));
    assert_int_equal(counter, 0);
    assert_int_equal(AMobjIdCounter(AMitemObjId(result_item)), 2);
    str = AMactorIdStr(AMobjIdActorId(AMitemObjId(result_item)));
    assert_int_equal(str.count, 4);
    assert_memory_equal(str.src, "bbbb", str.count);
    result_item = AMitemsNext(&result, 1);
    assert_true(AMitemToCounter(result_item, &counter));
    assert_int_equal(counter, 10);
    assert_int_equal(AMobjIdCounter(AMitemObjId(result_item)), 2);
    str = AMactorIdStr(AMobjIdActorId(AMitemObjId(result_item)));
    assert_int_equal(str.count, 4);
    assert_memory_equal(str.src, "cccc", str.count);
    /* doc1.increment("_root", "cnt", 5)                                     */
    AMstackItem(NULL, AMmapIncrement(doc1, AM_ROOT, AMstr("cnt"), 5), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* result = doc1.getAll("_root", "cnt")                                  */
    result = AMstackItems(stack_ptr, AMmapGetAll(doc1, AM_ROOT, AMstr("cnt"), NULL), cmocka_cb,
                          AMexpect(AM_VAL_TYPE_COUNTER));
    /* assert.deepEqual(result, [
         ['counter', 5, '2@bbbb'],
         ['counter', 15, '2@cccc'],
       ])                                                                    */
    result_item = AMitemsNext(&result, 1);
    assert_true(AMitemToCounter(result_item, &counter));
    assert_int_equal(counter, 5);
    assert_int_equal(AMobjIdCounter(AMitemObjId(result_item)), 2);
    str = AMactorIdStr(AMobjIdActorId(AMitemObjId(result_item)));
    assert_int_equal(str.count, 4);
    assert_memory_equal(str.src, "bbbb", str.count);
    result_item = AMitemsNext(&result, 1);
    assert_true(AMitemToCounter(result_item, &counter));
    assert_int_equal(counter, 15);
    assert_int_equal(AMobjIdCounter(AMitemObjId(result_item)), 2);
    str = AMactorIdStr(AMobjIdActorId(AMitemObjId(result_item)));
    assert_int_equal(str.count, 4);
    assert_memory_equal(str.src, "cccc", str.count);
    /*                                                                       */
    /* const save1 = doc1.save()                                             */
    AMbyteSpan save1;
    assert_true(AMitemToBytes(AMstackItem(stack_ptr, AMsave(doc1), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &save1));
    /* const doc4 = load(save1)                                              */
    AMdoc* doc4;
    assert_true(AMitemToDoc(
        AMstackItem(stack_ptr, AMload(save1.src, save1.count), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc4));
    /* assert.deepEqual(doc4.save(), save1);                                 */
    AMbyteSpan doc4_save;
    assert_true(
        AMitemToBytes(AMstackItem(stack_ptr, AMsave(doc4), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &doc4_save));
    assert_int_equal(doc4_save.count, save1.count);
    assert_memory_equal(doc4_save.src, save1.src, doc4_save.count);
}

/**
 * \brief local inc increments all visible counters in a sequence
 */
static void test_local_inc_increments_all_visible_counters_in_a_sequence(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc1 = create("aaaa")                                           */
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("aaaa")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)), &actor_id));
    AMdoc* doc1;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc1));
    /* const seq = doc1.putObject("_root", "seq", [])                        */
    AMobjId const* const seq =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc1, AM_ROOT, AMstr("seq"), AM_OBJ_TYPE_LIST), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* doc1.insert(seq, 0, "hello")                                          */
    AMstackItem(NULL, AMlistPutStr(doc1, seq, 0, true, AMstr("hello")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const doc2 = load(doc1.save(), "bbbb");                               */
    AMbyteSpan save1;
    assert_true(AMitemToBytes(AMstackItem(stack_ptr, AMsave(doc1), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &save1));
    AMdoc* doc2;
    assert_true(AMitemToDoc(
        AMstackItem(stack_ptr, AMload(save1.src, save1.count), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc2));
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("bbbb")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)), &actor_id));
    AMstackItem(NULL, AMsetActorId(doc2, actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const doc3 = load(doc1.save(), "cccc");                               */
    AMdoc* doc3;
    assert_true(AMitemToDoc(
        AMstackItem(stack_ptr, AMload(save1.src, save1.count), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc3));
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("cccc")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)), &actor_id));
    AMstackItem(NULL, AMsetActorId(doc3, actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* let heads = doc1.getHeads()                                           */
    AMitems const heads1 = AMstackItems(stack_ptr, AMgetHeads(doc1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* doc1.put(seq, 0, 20)                                                  */
    AMstackItem(NULL, AMlistPutInt(doc1, seq, 0, false, 20), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc2.put(seq, 0, 0, "counter")                                        */
    AMstackItem(NULL, AMlistPutCounter(doc2, seq, 0, false, 0), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc3.put(seq, 0, 10, "counter")                                       */
    AMstackItem(NULL, AMlistPutCounter(doc3, seq, 0, false, 10), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc1.applyChanges(doc2.getChanges(heads))                             */
    AMitems const changes2 =
        AMstackItems(stack_ptr, AMgetChanges(doc2, &heads1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE));
    AMstackItem(NULL, AMapplyChanges(doc1, &changes2), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc1.applyChanges(doc3.getChanges(heads))                             */
    AMitems const changes3 =
        AMstackItems(stack_ptr, AMgetChanges(doc3, &heads1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE));
    AMstackItem(NULL, AMapplyChanges(doc1, &changes3), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* let result = doc1.getAll(seq, 0)                                      */
    AMitems result = AMstackItems(stack_ptr, AMlistGetAll(doc1, seq, 0, NULL), cmocka_cb,
                                  AMexpect(AM_VAL_TYPE_COUNTER | AM_VAL_TYPE_INT));
    /* assert.deepEqual(result, [
         ['int', 20, '3@aaaa'],
         ['counter', 0, '3@bbbb'],
         ['counter', 10, '3@cccc'],
       ])                                                                    */
    AMitem* result_item = AMitemsNext(&result, 1);
    int64_t int_;
    assert_true(AMitemToInt(result_item, &int_));
    assert_int_equal(int_, 20);
    assert_int_equal(AMobjIdCounter(AMitemObjId(result_item)), 3);
    AMbyteSpan str = AMactorIdStr(AMobjIdActorId(AMitemObjId(result_item)));
    assert_int_equal(str.count, 4);
    assert_memory_equal(str.src, "aaaa", str.count);
    result_item = AMitemsNext(&result, 1);
    int64_t counter;
    assert_true(AMitemToCounter(result_item, &counter));
    assert_int_equal(counter, 0);
    assert_int_equal(AMobjIdCounter(AMitemObjId(result_item)), 3);
    str = AMactorIdStr(AMobjIdActorId(AMitemObjId(result_item)));
    assert_memory_equal(str.src, "bbbb", str.count);
    result_item = AMitemsNext(&result, 1);
    assert_true(AMitemToCounter(result_item, &counter));
    assert_int_equal(counter, 10);
    assert_int_equal(AMobjIdCounter(AMitemObjId(result_item)), 3);
    str = AMactorIdStr(AMobjIdActorId(AMitemObjId(result_item)));
    assert_int_equal(str.count, 4);
    assert_memory_equal(str.src, "cccc", str.count);
    /* doc1.increment(seq, 0, 5)                                             */
    AMstackItem(NULL, AMlistIncrement(doc1, seq, 0, 5), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* result = doc1.getAll(seq, 0)                                          */
    result = AMstackItems(stack_ptr, AMlistGetAll(doc1, seq, 0, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_COUNTER));
    /* assert.deepEqual(result, [
         ['counter', 5, '3@bbbb'],
         ['counter', 15, '3@cccc'],
       ])                                                                    */
    result_item = AMitemsNext(&result, 1);
    assert_true(AMitemToCounter(result_item, &counter));
    assert_int_equal(counter, 5);
    assert_int_equal(AMobjIdCounter(AMitemObjId(result_item)), 3);
    str = AMactorIdStr(AMobjIdActorId(AMitemObjId(result_item)));
    assert_int_equal(str.count, 4);
    assert_memory_equal(str.src, "bbbb", str.count);
    result_item = AMitemsNext(&result, 1);
    assert_true(AMitemToCounter(result_item, &counter));
    assert_int_equal(counter, 15);
    assert_int_equal(AMobjIdCounter(AMitemObjId(result_item)), 3);
    str = AMactorIdStr(AMobjIdActorId(AMitemObjId(result_item)));
    assert_memory_equal(str.src, "cccc", str.count);
    /*                                                                       */
    /* const save = doc1.save()                                              */
    AMbyteSpan save;
    assert_true(AMitemToBytes(AMstackItem(stack_ptr, AMsave(doc1), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &save));
    /* const doc4 = load(save)                                               */
    AMdoc* doc4;
    assert_true(
        AMitemToDoc(AMstackItem(stack_ptr, AMload(save.src, save.count), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc4));
    /* assert.deepEqual(doc4.save(), save);                                  */
    AMbyteSpan doc4_save;
    assert_true(
        AMitemToBytes(AMstackItem(stack_ptr, AMsave(doc4), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &doc4_save));
    assert_int_equal(doc4_save.count, save.count);
    assert_memory_equal(doc4_save.src, save.src, doc4_save.count);
}

/**
 * \brief paths can be used instead of objids
 */
static void test_paths_can_be_used_instead_of_objids(void** state);

/**
 * \brief should be able to fetch changes by hash
 */
static void test_should_be_able_to_fetch_changes_by_hash(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc1 = create("aaaa")                                           */
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("aaaa")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)), &actor_id));
    AMdoc* doc1;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc1));
    /* const doc2 = create("bbbb")                                           */
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("bbbb")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)), &actor_id));
    AMdoc* doc2;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc2));
    /* doc1.put("/", "a", "b")                                               */
    AMstackItem(NULL, AMmapPutStr(doc1, AM_ROOT, AMstr("a"), AMstr("b")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc2.put("/", "b", "c")                                               */
    AMstackItem(NULL, AMmapPutStr(doc2, AM_ROOT, AMstr("b"), AMstr("c")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const head1 = doc1.getHeads()                                         */
    AMitems head1 = AMstackItems(stack_ptr, AMgetHeads(doc1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* const head2 = doc2.getHeads()                                         */
    AMitems head2 = AMstackItems(stack_ptr, AMgetHeads(doc2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* const change1 = doc1.getChangeByHash(head1[0])
       if (change1 === null) { throw new RangeError("change1 should not be
       null")  */
    AMbyteSpan change_hash1;
    assert_true(AMitemToChangeHash(AMitemsNext(&head1, 1), &change_hash1));
    AMchange* change1;
    assert_true(AMitemToChange(AMstackItem(stack_ptr, AMgetChangeByHash(doc1, change_hash1.src, change_hash1.count),
                                           cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE)),
                               &change1));
    /* const change2 = doc1.getChangeByHash(head2[0])
       assert.deepEqual(change2, null)                                       */
    AMbyteSpan change_hash2;
    assert_true(AMitemToChangeHash(AMitemsNext(&head2, 1), &change_hash2));
    AMstackItem(NULL, AMgetChangeByHash(doc1, change_hash2.src, change_hash2.count), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepEqual(decodeChange(change1).hash, head1[0])                */
    assert_memory_equal(AMchangeHash(change1).src, change_hash1.src, change_hash1.count);
}

/**
 * \brief recursive sets are possible
 */
static void test_recursive_sets_are_possible(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc = create("aaaa")                                            */
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("aaaa")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)), &actor_id));
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /* const l1 = doc.putObject("_root", "list", [{ foo: "bar" }, [1, 2, 3]] */
    AMobjId const* const l1 =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    {
        AMobjId const* const map = AMitemObjId(AMstackItem(
            stack_ptr, AMlistPutObject(doc, l1, 0, true, AM_OBJ_TYPE_MAP), cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
        AMstackItem(NULL, AMmapPutStr(doc, map, AMstr("foo"), AMstr("bar")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        AMobjId const* const list =
            AMitemObjId(AMstackItem(stack_ptr, AMlistPutObject(doc, l1, SIZE_MAX, true, AM_OBJ_TYPE_LIST), cmocka_cb,
                                    AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
        for (int value = 1; value != 4; ++value) {
            AMstackItem(NULL, AMlistPutInt(doc, list, SIZE_MAX, true, value), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        }
    }
    /* const l2 = doc.insertObject(l1, 0, { zip: ["a", "b"] })               */
    AMobjId const* const l2 = AMitemObjId(AMstackItem(stack_ptr, AMlistPutObject(doc, l1, 0, true, AM_OBJ_TYPE_MAP),
                                                      cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    {
        AMobjId const* const list =
            AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, l2, AMstr("zip"), AM_OBJ_TYPE_LIST), cmocka_cb,
                                    AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
        AMstackItem(NULL, AMlistPutStr(doc, list, SIZE_MAX, true, AMstr("a")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        AMstackItem(NULL, AMlistPutStr(doc, list, SIZE_MAX, true, AMstr("b")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    }
    /* const l3 = doc.putObject("_root", "info1", "hello world") // 'text'
     * object */
    AMobjId const* const l3 =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("info1"), AM_OBJ_TYPE_TEXT), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    AMstackItem(NULL, AMspliceText(doc, l3, 0, 0, AMstr("hello world")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* doc.put("_root", "info2", "hello world")  // 'str'                    */
    AMstackItem(NULL, AMmapPutStr(doc, AM_ROOT, AMstr("info2"), AMstr("hello world")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* const l4 = doc.putObject("_root", "info3", "hello world")             */
    AMobjId const* const l4 =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("info3"), AM_OBJ_TYPE_TEXT), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    AMstackItem(NULL, AMspliceText(doc, l4, 0, 0, AMstr("hello world")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepEqual(doc.materialize(), {
         "list": [{ zip: ["a", "b"] }, { foo: "bar" }, [1, 2, 3]],
         "info1": "hello world",
         "info2": "hello world",
         "info3": "hello world",
       }) */
    AMitems doc_items = AMstackItems(stack_ptr, AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_OBJ_TYPE | AM_VAL_TYPE_STR));
    AMitem* doc_item = AMitemsNext(&doc_items, 1);
    assert_int_equal(AMitemIdxType(doc_item), AM_IDX_TYPE_KEY);
    AMbyteSpan key;
    assert_true(AMitemKey(doc_item, &key));
    assert_int_equal(key.count, strlen("info1"));
    assert_memory_equal(key.src, "info1", key.count);
    AMbyteSpan str;
    assert_true(AMitemToStr(
        AMstackItem(stack_ptr, AMtext(doc, AMitemObjId(doc_item), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, strlen("hello world"));
    assert_memory_equal(str.src, "hello world", str.count);
    doc_item = AMitemsNext(&doc_items, 1);
    assert_int_equal(AMitemIdxType(doc_item), AM_IDX_TYPE_KEY);
    assert_true(AMitemKey(doc_item, &key));
    assert_int_equal(key.count, strlen("info2"));
    assert_memory_equal(key.src, "info2", key.count);
    assert_true(AMitemToStr(doc_item, &str));
    assert_int_equal(str.count, strlen("hello world"));
    assert_memory_equal(str.src, "hello world", str.count);
    doc_item = AMitemsNext(&doc_items, 1);
    assert_int_equal(AMitemIdxType(doc_item), AM_IDX_TYPE_KEY);
    assert_true(AMitemKey(doc_item, &key));
    assert_int_equal(key.count, strlen("info3"));
    assert_memory_equal(key.src, "info3", key.count);
    assert_true(AMitemToStr(
        AMstackItem(stack_ptr, AMtext(doc, AMitemObjId(doc_item), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, strlen("hello world"));
    assert_memory_equal(str.src, "hello world", str.count);
    doc_item = AMitemsNext(&doc_items, 1);
    assert_int_equal(AMitemIdxType(doc_item), AM_IDX_TYPE_KEY);
    assert_true(AMitemKey(doc_item, &key));
    assert_int_equal(key.count, strlen("list"));
    assert_memory_equal(key.src, "list", key.count);
    {
        AMitems list_items = AMstackItems(stack_ptr, AMlistRange(doc, AMitemObjId(doc_item), 0, SIZE_MAX, NULL),
                                          cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE));
        AMitem const* list_item = AMitemsNext(&list_items, 1);
        {
            AMitems map_items =
                AMstackItems(stack_ptr, AMmapRange(doc, AMitemObjId(list_item), AMstr(NULL), AMstr(NULL), NULL),
                             cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE));
            AMitem const* map_item = AMitemsNext(&map_items, 1);
            assert_int_equal(AMitemIdxType(map_item), AM_IDX_TYPE_KEY);
            AMbyteSpan key;
            assert_true(AMitemKey(map_item, &key));
            assert_int_equal(key.count, strlen("zip"));
            assert_memory_equal(key.src, "zip", key.count);
            {
                AMitems list_items = AMstackItems(stack_ptr, AMlistRange(doc, AMitemObjId(map_item), 0, SIZE_MAX, NULL),
                                                  cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE | AM_VAL_TYPE_STR));
                AMbyteSpan str;
                assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
                assert_int_equal(str.count, 1);
                assert_memory_equal(str.src, "a", str.count);
                assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
                assert_int_equal(str.count, 1);
                assert_memory_equal(str.src, "b", str.count);
            }
        }
        list_item = AMitemsNext(&list_items, 1);
        {
            AMitems map_items =
                AMstackItems(stack_ptr, AMmapRange(doc, AMitemObjId(list_item), AMstr(NULL), AMstr(NULL), NULL),
                             cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE | AM_VAL_TYPE_STR));
            AMitem* map_item = AMitemsNext(&map_items, 1);
            assert_int_equal(AMitemIdxType(map_item), AM_IDX_TYPE_KEY);
            AMbyteSpan key;
            assert_true(AMitemKey(map_item, &key));
            assert_int_equal(key.count, strlen("foo"));
            assert_memory_equal(key.src, "foo", key.count);
            AMbyteSpan str;
            assert_true(AMitemToStr(map_item, &str));
            assert_int_equal(str.count, 3);
            assert_memory_equal(str.src, "bar", str.count);
        }
        list_item = AMitemsNext(&list_items, 1);
        {
            AMitems list_items = AMstackItems(stack_ptr, AMlistRange(doc, AMitemObjId(list_item), 0, SIZE_MAX, NULL),
                                              cmocka_cb, AMexpect(AM_VAL_TYPE_INT));
            int64_t int_;
            assert_true(AMitemToInt(AMitemsNext(&list_items, 1), &int_));
            assert_int_equal(int_, 1);
            assert_true(AMitemToInt(AMitemsNext(&list_items, 1), &int_));
            assert_int_equal(int_, 2);
            assert_true(AMitemToInt(AMitemsNext(&list_items, 1), &int_));
            assert_int_equal(int_, 3);
        }
    }
    /* assert.deepEqual(doc.materialize(l2), { zip: ["a", "b"] })            */
    AMitems map_items = AMstackItems(stack_ptr, AMmapRange(doc, l2, AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_OBJ_TYPE));
    AMitem const* map_item = AMitemsNext(&map_items, 1);
    assert_int_equal(AMitemIdxType(map_item), AM_IDX_TYPE_KEY);
    assert_true(AMitemKey(map_item, &key));
    assert_int_equal(key.count, strlen("zip"));
    assert_memory_equal(key.src, "zip", key.count);
    {
        AMitems list_items = AMstackItems(stack_ptr, AMlistRange(doc, AMitemObjId(map_item), 0, SIZE_MAX, NULL),
                                          cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
        AMbyteSpan str;
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "a", str.count);
        assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
        assert_int_equal(str.count, 1);
        assert_memory_equal(str.src, "b", str.count);
    }
    /* assert.deepEqual(doc.materialize(l1), [{ zip: ["a", "b"] }, { foo: "bar"
     * }, [1, 2, 3]] */
    AMitems list_items =
        AMstackItems(stack_ptr, AMlistRange(doc, l1, 0, SIZE_MAX, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE));
    AMitem const* list_item = AMitemsNext(&list_items, 1);
    {
        AMitems map_items =
            AMstackItems(stack_ptr, AMmapRange(doc, AMitemObjId(list_item), AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                         AMexpect(AM_VAL_TYPE_OBJ_TYPE));
        AMitem const* map_item = AMitemsNext(&map_items, 1);
        assert_int_equal(AMitemIdxType(map_item), AM_IDX_TYPE_KEY);
        AMbyteSpan key;
        assert_true(AMitemKey(map_item, &key));
        assert_int_equal(key.count, strlen("zip"));
        assert_memory_equal(key.src, "zip", key.count);
        {
            AMitems list_items = AMstackItems(stack_ptr, AMlistRange(doc, AMitemObjId(map_item), 0, SIZE_MAX, NULL),
                                              cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
            AMbyteSpan str;
            assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
            assert_int_equal(str.count, 1);
            assert_memory_equal(str.src, "a", str.count);
            assert_true(AMitemToStr(AMitemsNext(&list_items, 1), &str));
            assert_int_equal(str.count, 1);
            assert_memory_equal(str.src, "b", str.count);
        }
    }
    list_item = AMitemsNext(&list_items, 1);
    {
        AMitems map_items =
            AMstackItems(stack_ptr, AMmapRange(doc, AMitemObjId(list_item), AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                         AMexpect(AM_VAL_TYPE_STR));
        AMitem* map_item = AMitemsNext(&map_items, 1);
        assert_int_equal(AMitemIdxType(map_item), AM_IDX_TYPE_KEY);
        AMbyteSpan key;
        assert_true(AMitemKey(map_item, &key));
        assert_int_equal(key.count, strlen("foo"));
        assert_memory_equal(key.src, "foo", key.count);
        AMbyteSpan str;
        assert_true(AMitemToStr(map_item, &str));
        assert_int_equal(str.count, 3);
        assert_memory_equal(str.src, "bar", str.count);
    }
    list_item = AMitemsNext(&list_items, 1);
    {
        AMitems list_items = AMstackItems(stack_ptr, AMlistRange(doc, AMitemObjId(list_item), 0, SIZE_MAX, NULL),
                                          cmocka_cb, AMexpect(AM_VAL_TYPE_INT));
        int64_t int_;
        assert_true(AMitemToInt(AMitemsNext(&list_items, 1), &int_));
        assert_int_equal(int_, 1);
        assert_true(AMitemToInt(AMitemsNext(&list_items, 1), &int_));
        assert_int_equal(int_, 2);
        assert_true(AMitemToInt(AMitemsNext(&list_items, 1), &int_));
        assert_int_equal(int_, 3);
    }
    /* assert.deepEqual(doc.materialize(l4), "hello world")                  */
    assert_true(AMitemToStr(AMstackItem(stack_ptr, AMtext(doc, l4, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, strlen("hello world"));
    assert_memory_equal(str.src, "hello world", str.count);
}

/**
 * \brief only returns an object id when objects are created
 */
static void test_only_returns_an_object_id_when_objects_are_created(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc = create("aaaa")                                            */
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("aaaa")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)), &actor_id));
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    /* const r1 = doc.put("_root", "foo", "bar")
       assert.deepEqual(r1, null);                                           */
    AMstackItem(NULL, AMmapPutStr(doc, AM_ROOT, AMstr("foo"), AMstr("bar")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const r2 = doc.putObject("_root", "list", [])                         */
    AMobjId const* const r2 =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* const r3 = doc.put("_root", "counter", 10, "counter")
       assert.deepEqual(r3, null);                                           */
    AMstackItem(NULL, AMmapPutCounter(doc, AM_ROOT, AMstr("counter"), 10), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const r4 = doc.increment("_root", "counter", 1)
       assert.deepEqual(r4, null);                                           */
    AMstackItem(NULL, AMmapIncrement(doc, AM_ROOT, AMstr("counter"), 1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const r5 = doc.delete("_root", "counter")
       assert.deepEqual(r5, null);                                           */
    AMstackItem(NULL, AMmapDelete(doc, AM_ROOT, AMstr("counter")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const r6 = doc.insert(r2, 0, 10);
       assert.deepEqual(r6, null);                                           */
    AMstackItem(NULL, AMlistPutInt(doc, r2, 0, true, 10), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const r7 = doc.insertObject(r2, 0, {});                               */
    AMobjId const* const r7 = AMitemObjId(AMstackItem(stack_ptr, AMlistPutObject(doc, r2, 0, true, AM_OBJ_TYPE_LIST),
                                                      cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* const r8 = doc.splice(r2, 1, 0, ["a", "b", "c"]);                     */
    AMresult* data = AMstackResult(
        stack_ptr, AMresultFrom(3, AMitemFromStr(AMstr("a")), AMitemFromStr(AMstr("b")), AMitemFromStr(AMstr("c"))),
        NULL, NULL);
    AMstackItem(NULL, AMsplice(doc, r2, 1, 0, AMresultItems(data)), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepEqual(r2, "2@aaaa");                                       */
    assert_int_equal(AMobjIdCounter(r2), 2);
    AMbyteSpan str = AMactorIdStr(AMobjIdActorId(r2));
    assert_int_equal(str.count, 4);
    assert_memory_equal(str.src, "aaaa", str.count);
    /* assert.deepEqual(r7, "7@aaaa");                                       */
    assert_int_equal(AMobjIdCounter(r7), 7);
    str = AMactorIdStr(AMobjIdActorId(r7));
    assert_memory_equal(str.src, "aaaa", str.count);
}

/**
 * \brief objects without properties are preserved
 */
static void test_objects_without_properties_are_preserved(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const doc1 = create("aaaa")                                           */
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("aaaa")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)), &actor_id));
    AMdoc* doc1;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc1));
    /* const a = doc1.putObject("_root", "a", {});                           */
    AMobjId const* const a =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc1, AM_ROOT, AMstr("a"), AM_OBJ_TYPE_MAP), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* const b = doc1.putObject("_root", "b", {});                           */
    AMobjId const* const b =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc1, AM_ROOT, AMstr("b"), AM_OBJ_TYPE_MAP), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* const c = doc1.putObject("_root", "c", {});                           */
    AMobjId const* const c =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc1, AM_ROOT, AMstr("c"), AM_OBJ_TYPE_MAP), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* const d = doc1.put(c, "d", "dd");                                     */
    AMstackItem(NULL, AMmapPutStr(doc1, c, AMstr("d"), AMstr("dd")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const saved = doc1.save();                                            */
    AMbyteSpan saved;
    assert_true(AMitemToBytes(AMstackItem(stack_ptr, AMsave(doc1), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &saved));
    /* const doc2 = load(saved);                                             */
    AMdoc* doc2;
    assert_true(AMitemToDoc(
        AMstackItem(stack_ptr, AMload(saved.src, saved.count), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc2));
    /* assert.deepEqual(doc2.getWithType("_root", "a"), ["map", a])          */
    AMitems doc_items = AMstackItems(stack_ptr, AMmapRange(doc2, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_OBJ_TYPE));
    assert_true(AMobjIdEqual(AMitemObjId(AMitemsNext(&doc_items, 1)), a));
    /* assert.deepEqual(doc2.keys(a), [])                                    */
    AMitems keys = AMstackItems(stack_ptr, AMkeys(doc1, a, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_int_equal(AMitemsSize(&keys), 0);
    /* assert.deepEqual(doc2.getWithType("_root", "b"), ["map", b])          */
    assert_true(AMobjIdEqual(AMitemObjId(AMitemsNext(&doc_items, 1)), b));
    /* assert.deepEqual(doc2.keys(b), [])                                    */
    keys = AMstackItems(stack_ptr, AMkeys(doc1, b, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_int_equal(AMitemsSize(&keys), 0);
    /* assert.deepEqual(doc2.getWithType("_root", "c"), ["map", c])          */
    assert_true(AMobjIdEqual(AMitemObjId(AMitemsNext(&doc_items, 1)), c));
    /* assert.deepEqual(doc2.keys(c), ["d"])                                 */
    keys = AMstackItems(stack_ptr, AMkeys(doc1, c, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    AMbyteSpan str;
    assert_true(AMitemToStr(AMitemsNext(&keys, 1), &str));
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "d", str.count);
    /* assert.deepEqual(doc2.getWithType(c, "d"), ["str", "dd"])             */
    AMitems obj_items = AMstackItems(stack_ptr, AMobjItems(doc1, c, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_true(AMitemToStr(AMitemsNext(&obj_items, 1), &str));
    assert_int_equal(str.count, 2);
    assert_memory_equal(str.src, "dd", str.count);
}

/**
 * \brief should allow you to forkAt a heads
 */
static void test_should_allow_you_to_forkAt_a_heads(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const A = create("aaaaaa")                                            */
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("aaaaaa")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMdoc* A;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &A));
    /* A.put("/", "key1", "val1");                                           */
    AMstackItem(NULL, AMmapPutStr(A, AM_ROOT, AMstr("key1"), AMstr("val1")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* A.put("/", "key2", "val2");                                           */
    AMstackItem(NULL, AMmapPutStr(A, AM_ROOT, AMstr("key2"), AMstr("val2")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const heads1 = A.getHeads();                                          */
    AMitems const heads1 = AMstackItems(stack_ptr, AMgetHeads(A), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* const B = A.fork("bbbbbb")                                            */
    AMdoc* B;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMfork(A, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &B));
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("bbbbbb")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMstackItem(NULL, AMsetActorId(B, actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* A.put("/", "key3", "val3");                                           */
    AMstackItem(NULL, AMmapPutStr(A, AM_ROOT, AMstr("key3"), AMstr("val3")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* B.put("/", "key4", "val4");                                           */
    AMstackItem(NULL, AMmapPutStr(B, AM_ROOT, AMstr("key4"), AMstr("val4")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* A.merge(B)                                                            */
    AMstackItem(NULL, AMmerge(A, B), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* const heads2 = A.getHeads();                                          */
    AMitems const heads2 = AMstackItems(stack_ptr, AMgetHeads(A), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* A.put("/", "key5", "val5");                                           */
    AMstackItem(NULL, AMmapPutStr(A, AM_ROOT, AMstr("key5"), AMstr("val5")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepEqual(A.forkAt(heads1).materialize("/"), A.materialize("/",
     * heads1) */
    AMdoc* A_forkAt1;
    assert_true(
        AMitemToDoc(AMstackItem(stack_ptr, AMfork(A, &heads1), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &A_forkAt1));
    AMitems AforkAt1_items = AMstackItems(stack_ptr, AMmapRange(A_forkAt1, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL),
                                          cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    AMitems A1_items = AMstackItems(stack_ptr, AMmapRange(A, AM_ROOT, AMstr(NULL), AMstr(NULL), &heads1), cmocka_cb,
                                    AMexpect(AM_VAL_TYPE_STR));
    assert_true(AMitemsEqual(&AforkAt1_items, &A1_items));
    /* assert.deepEqual(A.forkAt(heads2).materialize("/"), A.materialize("/",
     * heads2) */
    AMdoc* A_forkAt2;
    assert_true(
        AMitemToDoc(AMstackItem(stack_ptr, AMfork(A, &heads2), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &A_forkAt2));
    AMitems AforkAt2_items = AMstackItems(stack_ptr, AMmapRange(A_forkAt2, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL),
                                          cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    AMitems A2_items = AMstackItems(stack_ptr, AMmapRange(A, AM_ROOT, AMstr(NULL), AMstr(NULL), &heads2), cmocka_cb,
                                    AMexpect(AM_VAL_TYPE_STR));
    assert_true(AMitemsEqual(&AforkAt2_items, &A2_items));
}

/**
 * \brief should handle merging text conflicts then saving & loading
 */
static void test_should_handle_merging_text_conflicts_then_saving_and_loading(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    /* const A = create("aabbcc")                                            */
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("aabbcc")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMdoc* A;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &A));
    /* const At = A.putObject('_root', 'text', "")                           */
    AMobjId const* const At =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(A, AM_ROOT, AMstr("text"), AM_OBJ_TYPE_TEXT), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* A.splice(At, 0, 0, 'hello')                                           */
    AMstackItem(NULL, AMspliceText(A, At, 0, 0, AMstr("hello")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*                                                                       */
    /* const B = A.fork()                                                    */
    AMdoc* B;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMfork(A, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &B));
    /*                                                                       */
    /* assert.deepEqual(B.getWithType("_root", "text"), ["text", At])        */
    AMbyteSpan str;
    assert_true(
        AMitemToStr(AMstackItem(stack_ptr,
                                AMtext(B,
                                       AMitemObjId(AMstackItem(stack_ptr, AMmapGet(B, AM_ROOT, AMstr("text"), NULL),
                                                               cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE))),
                                       NULL),
                                cmocka_cb, AMexpect(AM_VAL_TYPE_STR)),
                    &str));
    AMbyteSpan str2;
    assert_true(AMitemToStr(AMstackItem(stack_ptr, AMtext(A, At, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str2));
    assert_int_equal(str.count, str2.count);
    assert_memory_equal(str.src, str2.src, str.count);
    /*                                                                       */
    /* B.splice(At, 4, 1)                                                    */
    AMstackItem(NULL, AMspliceText(B, At, 4, 1, AMstr(NULL)), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* B.splice(At, 4, 0, '!')                                               */
    AMstackItem(NULL, AMspliceText(B, At, 4, 0, AMstr("!")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* B.splice(At, 5, 0, ' ')                                               */
    AMstackItem(NULL, AMspliceText(B, At, 5, 0, AMstr(" ")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* B.splice(At, 6, 0, 'world')                                           */
    AMstackItem(NULL, AMspliceText(B, At, 6, 0, AMstr("world")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*                                                                       */
    /* A.merge(B)                                                            */
    AMstackItem(NULL, AMmerge(A, B), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /*                                                                       */
    /* const binary = A.save()                                               */
    AMbyteSpan binary;
    assert_true(AMitemToBytes(AMstackItem(stack_ptr, AMsave(A), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &binary));
    /*                                                                       */
    /* const C = load(binary)                                                */
    AMdoc* C;
    assert_true(AMitemToDoc(
        AMstackItem(stack_ptr, AMload(binary.src, binary.count), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &C));
    /*                                                                       */
    /* assert.deepEqual(C.getWithType('_root', 'text'), ['text', '1@aabbcc'] */
    AMobjId const* const C_text = AMitemObjId(
        AMstackItem(stack_ptr, AMmapGet(C, AM_ROOT, AMstr("text"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    assert_int_equal(AMobjIdCounter(C_text), 1);
    str = AMactorIdStr(AMobjIdActorId(C_text));
    assert_int_equal(str.count, strlen("aabbcc"));
    assert_memory_equal(str.src, "aabbcc", str.count);
    /* assert.deepEqual(C.text(At), 'hell! world')                           */
    assert_true(AMitemToStr(AMstackItem(stack_ptr, AMtext(C, At, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, strlen("hell! world"));
    assert_memory_equal(str.src, "hell! world", str.count);
}

int run_ported_wasm_basic_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_create_clone_and_free, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_start_and_commit, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_getting_a_nonexistent_prop_does_not_throw_an_error, setup_base,
                                        teardown_base),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_set_and_get_a_simple_value, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_use_bytes, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_make_subobjects, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_make_lists, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_lists_have_insert_set_splice_and_push_ops, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_delete_non_existent_props, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_del, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_use_counters, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_splice_text, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_save_all_or_incrementally, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_splice_text_2, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_local_inc_increments_all_visible_counters_in_a_map, setup_base,
                                        teardown_base),
        cmocka_unit_test_setup_teardown(test_local_inc_increments_all_visible_counters_in_a_sequence, setup_base,
                                        teardown_base),
        cmocka_unit_test_setup_teardown(test_should_be_able_to_fetch_changes_by_hash, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_recursive_sets_are_possible, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_only_returns_an_object_id_when_objects_are_created, setup_base,
                                        teardown_base),
        cmocka_unit_test_setup_teardown(test_objects_without_properties_are_preserved, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_should_allow_you_to_forkAt_a_heads, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_should_handle_merging_text_conflicts_then_saving_and_loading, setup_base,
                                        teardown_base)};

    return cmocka_run_group_tests(tests, NULL, NULL);
}
