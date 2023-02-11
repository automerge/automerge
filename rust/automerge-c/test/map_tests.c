#include <float.h>
#include <limits.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* third-party */
#include <cmocka.h>

/* local */
#include <automerge-c/automerge.h>
#include <automerge-c/utils/stack_callback_data.h>
#include <automerge-c/utils/string.h>
#include "base_state.h"
#include "cmocka_utils.h"
#include "doc_state.h"
#include "macro_utils.h"

static void test_AMmapIncrement(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;
    AMstackItem(NULL, AMmapPutCounter(doc_state->doc, AM_ROOT, AMstr("Counter"), 0), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    assert_int_equal(AMitemToCounter(AMstackItem(stack_ptr, AMmapGet(doc_state->doc, AM_ROOT, AMstr("Counter"), NULL),
                                                 cmocka_cb, AMexpect(AM_VAL_TYPE_COUNTER))),
                     0);
    AMfree(AMstackPop(stack_ptr, NULL));
    AMstackItem(NULL, AMmapIncrement(doc_state->doc, AM_ROOT, AMstr("Counter"), 3), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    assert_int_equal(AMitemToCounter(AMstackItem(stack_ptr, AMmapGet(doc_state->doc, AM_ROOT, AMstr("Counter"), NULL),
                                                 cmocka_cb, AMexpect(AM_VAL_TYPE_COUNTER))),
                     3);
    AMfree(AMstackPop(stack_ptr, NULL));
}

#define test_AMmapPut(suffix) test_AMmapPut##suffix

#define static_void_test_AMmapPut(suffix, scalar_value)                                                               \
    static void test_AMmapPut##suffix(void** state) {                                                                 \
        DocState* doc_state = *state;                                                                                 \
        AMstack** stack_ptr = &doc_state->base_state->stack;                                                          \
        AMstackItem(NULL, AMmapPut##suffix(doc_state->doc, AM_ROOT, AMstr(#suffix), scalar_value), cmocka_cb,         \
                    AMexpect(AM_VAL_TYPE_VOID));                                                                      \
        assert_true(AMitemTo##suffix(AMstackItem(stack_ptr, AMmapGet(doc_state->doc, AM_ROOT, AMstr(#suffix), NULL),  \
                                                 cmocka_cb, AMexpect(suffix_to_val_type(#suffix)))) == scalar_value); \
        AMfree(AMstackPop(stack_ptr, NULL));                                                                          \
    }

static void test_AMmapPutBytes(void** state) {
    static AMbyteSpan const KEY = {"Bytes", 5};
    static uint8_t const BYTES_VALUE[] = {INT8_MIN, INT8_MAX / 2, INT8_MAX};
    static size_t const BYTES_SIZE = sizeof(BYTES_VALUE) / sizeof(uint8_t);

    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;
    AMstackItem(NULL, AMmapPutBytes(doc_state->doc, AM_ROOT, KEY, AMbytes(BYTES_VALUE, BYTES_SIZE)), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMbyteSpan const bytes = AMitemToBytes(
        AMstackItem(stack_ptr, AMmapGet(doc_state->doc, AM_ROOT, KEY, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)));
    assert_int_equal(bytes.count, BYTES_SIZE);
    assert_memory_equal(bytes.src, BYTES_VALUE, BYTES_SIZE);
    AMfree(AMstackPop(stack_ptr, NULL));
}

static void test_AMmapPutNull(void** state) {
    static AMbyteSpan const KEY = {"Null", 4};

    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;
    AMstackItem(NULL, AMmapPutNull(doc_state->doc, AM_ROOT, KEY), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMresult* result = AMstackResult(stack_ptr, AMmapGet(doc_state->doc, AM_ROOT, KEY, NULL), NULL, NULL);
    if (AMresultStatus(result) != AM_STATUS_OK) {
        fail_msg_view("%s", AMerrorMessage(result));
    }
    assert_int_equal(AMresultSize(result), 1);
    AMitem* item = AMresultItem(result);
    assert_int_equal(AMitemValType(item), AM_VAL_TYPE_NULL);
}

#define test_AMmapPutObject(label) test_AMmapPutObject_##label

#define static_void_test_AMmapPutObject(label)                                                                   \
    static void test_AMmapPutObject_##label(void** state) {                                                      \
        DocState* doc_state = *state;                                                                            \
        AMstack** stack_ptr = &doc_state->base_state->stack;                                                     \
        AMobjType const obj_type = suffix_to_obj_type(#label);                                                   \
        AMobjId const* const obj_id =                                                                            \
            AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc_state->doc, AM_ROOT, AMstr(#label), obj_type), \
                                    cmocka_cb, AMexpect(AM_VAL_TYPE_VOID)));                                     \
        assert_non_null(obj_id);                                                                                 \
        assert_int_equal(AMobjObjType(doc_state->doc, obj_id), obj_type);                                        \
        assert_int_equal(AMobjSize(doc_state->doc, obj_id, NULL), 0);                                            \
        AMfree(AMstackPop(stack_ptr, NULL));                                                                     \
    }

static void test_AMmapPutStr(void** state) {
    DocState* doc_state = *state;
    AMstack** stack_ptr = &doc_state->base_state->stack;
    AMstackItem(NULL, AMmapPutStr(doc_state->doc, AM_ROOT, AMstr("Str"), AMstr("Hello, world!")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMbyteSpan const str = AMitemToStr(AMstackItem(stack_ptr, AMmapGet(doc_state->doc, AM_ROOT, AMstr("Str"), NULL),
                                                   cmocka_cb, AMexpect(AM_VAL_TYPE_STR)));
    assert_int_equal(str.count, strlen("Hello, world!"));
    assert_memory_equal(str.src, "Hello, world!", str.count);
    AMfree(AMstackPop(stack_ptr, NULL));
}

static_void_test_AMmapPut(Bool, true);

static_void_test_AMmapPut(Counter, INT64_MAX);

static_void_test_AMmapPut(F64, DBL_MAX);

static_void_test_AMmapPut(Int, INT64_MAX);

static_void_test_AMmapPutObject(List);

static_void_test_AMmapPutObject(Map);

static_void_test_AMmapPutObject(Text);

static_void_test_AMmapPut(Timestamp, INT64_MAX);

static_void_test_AMmapPut(Uint, UINT64_MAX);

/** \brief A JavaScript application can introduce NUL (`\0`) characters into
 * a map object's key which will truncate it in a C application.
 */
static void test_get_NUL_key(void** state) {
    /*
    import * as Automerge from "@automerge/automerge";
    let doc = Automerge.init();
    doc = Automerge.change(doc, doc => {
    doc['o\0ps'] = 'oops';
    });
    const bytes = Automerge.save(doc);
    console.log("static uint8_t const SAVED_DOC[] = {" + Array.apply([],
    bytes).join(", ") + "};");
    */
    static uint8_t const OOPS_SRC[] = {'o', '\0', 'p', 's'};
    static AMbyteSpan const OOPS_KEY = {.src = OOPS_SRC, .count = sizeof(OOPS_SRC) / sizeof(uint8_t)};

    static uint8_t const SAVED_DOC[] = {
        133, 111, 74,  131, 233, 150, 60, 244, 0,   116, 1,   16,  223, 253, 146, 193, 58,  122, 66,  134, 151,
        225, 210, 51,  58,  86,  247, 8,  1,   49,  118, 234, 228, 42,  116, 171, 13,  164, 99,  244, 27,  19,
        150, 44,  201, 136, 222, 219, 90, 246, 226, 123, 77,  120, 157, 155, 55,  182, 2,   178, 64,  6,   1,
        2,   3,   2,   19,  2,   35,  2,  64,  2,   86,  2,   8,   21,  6,   33,  2,   35,  2,   52,  1,   66,
        2,   86,  2,   87,  4,   128, 1,  2,   127, 0,   127, 1,   127, 1,   127, 0,   127, 0,   127, 7,   127,
        4,   111, 0,   112, 115, 127, 0,  127, 1,   1,   127, 1,   127, 70,  111, 111, 112, 115, 127, 0,   0};
    static size_t const SAVED_DOC_SIZE = sizeof(SAVED_DOC) / sizeof(uint8_t);

    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    AMdoc* const doc =
        AMitemToDoc(AMstackItem(stack_ptr, AMload(SAVED_DOC, SAVED_DOC_SIZE), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));
    AMbyteSpan const str = AMitemToStr(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, OOPS_KEY, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)));
    assert_int_not_equal(OOPS_KEY.count, strlen(OOPS_KEY.src));
    assert_int_equal(str.count, strlen("oops"));
    assert_memory_equal(str.src, "oops", str.count);
}

/** \brief A JavaScript application can introduce NUL (`\0`) characters into a
 *         map object's string value which will truncate it in a C application.
 */
static void test_get_NUL_string_value(void** state) {
    /*
    import * as Automerge from "@automerge/automerge";
    let doc = Automerge.init();
    doc = Automerge.change(doc, doc => {
        doc.oops = 'o\0ps';
    });
    const bytes = Automerge.save(doc);
    console.log("static uint8_t const SAVED_DOC[] = {" + Array.apply([],
    bytes).join(", ") + "};");
    */
    static uint8_t const OOPS_VALUE[] = {'o', '\0', 'p', 's'};
    static size_t const OOPS_SIZE = sizeof(OOPS_VALUE) / sizeof(uint8_t);

    static uint8_t const SAVED_DOC[] = {
        133, 111, 74,  131, 63,  94,  151, 29,  0,   116, 1,   16,  156, 159, 189, 12,  125, 55,  71,  154, 136,
        104, 237, 186, 45,  224, 32,  22,  1,   36,  163, 164, 222, 81,  42,  1,   247, 231, 156, 54,  222, 76,
        6,   109, 18,  172, 75,  36,  118, 120, 68,  73,  87,  186, 230, 127, 68,  19,  81,  149, 185, 6,   1,
        2,   3,   2,   19,  2,   35,  2,   64,  2,   86,  2,   8,   21,  6,   33,  2,   35,  2,   52,  1,   66,
        2,   86,  2,   87,  4,   128, 1,   2,   127, 0,   127, 1,   127, 1,   127, 0,   127, 0,   127, 7,   127,
        4,   111, 111, 112, 115, 127, 0,   127, 1,   1,   127, 1,   127, 70,  111, 0,   112, 115, 127, 0,   0};
    static size_t const SAVED_DOC_SIZE = sizeof(SAVED_DOC) / sizeof(uint8_t);

    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    AMdoc* const doc =
        AMitemToDoc(AMstackItem(stack_ptr, AMload(SAVED_DOC, SAVED_DOC_SIZE), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));
    AMbyteSpan const str = AMitemToStr(
        AMstackItem(stack_ptr, AMmapGet(doc, AM_ROOT, AMstr("oops"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)));
    assert_int_not_equal(str.count, strlen(OOPS_VALUE));
    assert_int_equal(str.count, OOPS_SIZE);
    assert_memory_equal(str.src, OOPS_VALUE, str.count);
}

static void test_range_iter_map(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    AMdoc* const doc = AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));
    AMstackItem(NULL, AMmapPutUint(doc, AM_ROOT, AMstr("a"), 3), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutUint(doc, AM_ROOT, AMstr("b"), 4), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutUint(doc, AM_ROOT, AMstr("c"), 5), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutUint(doc, AM_ROOT, AMstr("d"), 6), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(doc, AMstr(NULL), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMstackItem(NULL, AMmapPutUint(doc, AM_ROOT, AMstr("a"), 7), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(doc, AMstr(NULL), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMstackItem(NULL, AMmapPutUint(doc, AM_ROOT, AMstr("a"), 8), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutUint(doc, AM_ROOT, AMstr("d"), 9), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(doc, AMstr(NULL), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMactorId const* const actor_id =
        AMitemToActorId(AMstackItem(stack_ptr, AMgetActorId(doc), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)));
    AMitems map_items = AMstackItems(stack_ptr, AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_UINT));
    assert_int_equal(AMitemsSize(&map_items), 4);

    /* ["b"-"d") */
    AMitems range = AMstackItems(stack_ptr, AMmapRange(doc, AM_ROOT, AMstr("b"), AMstr("d"), NULL), cmocka_cb,
                                 AMexpect(AM_VAL_TYPE_UINT));
    /* First */
    AMitem* next = AMitemsNext(&range, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    AMbyteSpan key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "b", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_UINT);
    assert_int_equal(AMitemToUint(next), 4);
    AMobjId const* next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Second */
    next = AMitemsNext(&range, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "c", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_UINT);
    assert_int_equal(AMitemToUint(next), 5);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    assert_null(AMitemsNext(&range, 1));

    /* ["b"-<key-n>) */
    range = AMstackItems(stack_ptr, AMmapRange(doc, AM_ROOT, AMstr("b"), AMstr(NULL), NULL), cmocka_cb,
                         AMexpect(AM_VAL_TYPE_UINT));
    /* First */
    next = AMitemsNext(&range, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "b", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_UINT);
    assert_int_equal(AMitemToUint(next), 4);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Second */
    next = AMitemsNext(&range, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "c", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_UINT);
    assert_int_equal(AMitemToUint(next), 5);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next = AMitemsNext(&range, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "d", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_UINT);
    assert_int_equal(AMitemToUint(next), 9);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 7);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Fourth */
    assert_null(AMitemsNext(&range, 1));

    /* [<key-0>-"d") */
    range = AMstackItems(stack_ptr, AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr("d"), NULL), cmocka_cb,
                         AMexpect(AM_VAL_TYPE_UINT));
    /* First */
    next = AMitemsNext(&range, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "a", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_UINT);
    assert_int_equal(AMitemToUint(next), 8);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 6);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Second */
    next = AMitemsNext(&range, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "b", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_UINT);
    assert_int_equal(AMitemToUint(next), 4);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next = AMitemsNext(&range, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "c", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_UINT);
    assert_int_equal(AMitemToUint(next), 5);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Fourth */
    assert_null(AMitemsNext(&range, 1));

    /* ["a"-<key-n>) */
    range = AMstackItems(stack_ptr, AMmapRange(doc, AM_ROOT, AMstr("a"), AMstr(NULL), NULL), cmocka_cb,
                         AMexpect(AM_VAL_TYPE_UINT));
    /* First */
    next = AMitemsNext(&range, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "a", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_UINT);
    assert_int_equal(AMitemToUint(next), 8);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 6);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Second */
    next = AMitemsNext(&range, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "b", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_UINT);
    assert_int_equal(AMitemToUint(next), 4);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next = AMitemsNext(&range, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "c", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_UINT);
    assert_int_equal(AMitemToUint(next), 5);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Fourth */
    next = AMitemsNext(&range, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "d", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_UINT);
    assert_int_equal(AMitemToUint(next), 9);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 7);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Fifth */
    assert_null(AMitemsNext(&range, 1));
}

static void test_map_range_back_and_forth_single(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    AMdoc* const doc = AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));
    AMactorId const* const actor_id =
        AMitemToActorId(AMstackItem(stack_ptr, AMgetActorId(doc), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)));

    AMstackItem(NULL, AMmapPutStr(doc, AM_ROOT, AMstr("1"), AMstr("a")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc, AM_ROOT, AMstr("2"), AMstr("b")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc, AM_ROOT, AMstr("3"), AMstr("c")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    /* Forward, back, back. */
    AMitems range_all = AMstackItems(stack_ptr, AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_STR));
    /* First */
    AMitem* next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    AMbyteSpan key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    AMbyteSpan str = AMitemToStr(next);
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "a", str.count);
    AMobjId const* next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    AMitems range_back_all = AMitemsReversed(&range_all);
    range_back_all = AMitemsRewound(&range_back_all);
    AMitem* next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    AMbyteSpan str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 1);
    assert_memory_equal(str_back.src, "c", str_back.count);
    AMobjId const* next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 1);
    assert_memory_equal(str_back.src, "b", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);

    /* Forward, back, forward. */
    range_all = AMitemsRewound(&range_all);
    range_back_all = AMitemsRewound(&range_back_all);
    /* First */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);

    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "a", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 1);
    assert_memory_equal(str_back.src, "c", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "b", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);

    /* Forward, forward, forward. */
    range_all = AMitemsRewound(&range_all);
    /* First */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "a", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Second */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "b", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "c", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Forward stop */
    assert_null(AMitemsNext(&range_all, 1));

    /* Back, back, back. */
    range_back_all = AMitemsRewound(&range_back_all);
    /* Third */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 1);
    assert_memory_equal(str_back.src, "c", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 1);
    assert_memory_equal(str_back.src, "b", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* First */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 1);
    assert_memory_equal(str_back.src, "a", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Back stop */
    assert_null(AMitemsNext(&range_back_all, 1));
}

static void test_map_range_back_and_forth_double(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    AMdoc* const doc1 = AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));
    AMactorId const* const actor_id1 =
        AMitemToActorId(AMstackItem(stack_ptr, AMactorIdFromBytes("\0", 1), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)));
    AMstackItem(NULL, AMsetActorId(doc1, actor_id1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    AMstackItem(NULL, AMmapPutStr(doc1, AM_ROOT, AMstr("1"), AMstr("a")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc1, AM_ROOT, AMstr("2"), AMstr("b")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc1, AM_ROOT, AMstr("3"), AMstr("c")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    /* The second actor should win all conflicts here. */
    AMdoc* const doc2 = AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));
    AMactorId const* const actor_id2 =
        AMitemToActorId(AMstackItem(stack_ptr, AMactorIdFromBytes("\1", 1), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)));
    AMstackItem(NULL, AMsetActorId(doc2, actor_id2), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc2, AM_ROOT, AMstr("1"), AMstr("aa")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc2, AM_ROOT, AMstr("2"), AMstr("bb")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc2, AM_ROOT, AMstr("3"), AMstr("cc")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    AMstackItem(NULL, AMmerge(doc1, doc2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    /* Forward, back, back. */
    AMitems range_all = AMstackItems(stack_ptr, AMmapRange(doc1, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_STR));
    /* First */
    AMitem* next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    AMbyteSpan key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    AMbyteSpan str = AMitemToStr(next);
    assert_int_equal(str.count, 2);
    assert_memory_equal(str.src, "aa", str.count);
    AMobjId const* next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    AMitems range_back_all = AMitemsReversed(&range_all);
    range_back_all = AMitemsRewound(&range_back_all);
    AMitem* next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    AMbyteSpan str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 2);
    assert_memory_equal(str_back.src, "cc", str_back.count);
    AMobjId const* next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 2);
    assert_memory_equal(str_back.src, "bb", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);

    /* Forward, back, forward. */
    range_all = AMitemsRewound(&range_all);
    range_back_all = AMitemsRewound(&range_back_all);
    /* First */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 2);
    assert_memory_equal(str.src, "aa", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 2);
    assert_memory_equal(str_back.src, "cc", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 2);
    assert_memory_equal(str.src, "bb", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);

    /* Forward, forward, forward. */
    range_all = AMitemsRewound(&range_all);
    /* First */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 2);
    assert_memory_equal(str.src, "aa", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Second */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 2);
    assert_memory_equal(str.src, "bb", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 2);
    assert_memory_equal(str.src, "cc", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Forward stop */
    assert_null(AMitemsNext(&range_all, 1));

    /* Back, back, back. */
    range_back_all = AMitemsRewound(&range_back_all);
    /* Third */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 2);
    assert_memory_equal(str_back.src, "cc", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 2);
    assert_memory_equal(str_back.src, "bb", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* First */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 2);
    assert_memory_equal(str_back.src, "aa", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Back stop */
    assert_null(AMitemsNext(&range_back_all, 1));
}

static void test_map_range_at_back_and_forth_single(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    AMdoc* const doc = AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));
    AMactorId const* const actor_id =
        AMitemToActorId(AMstackItem(stack_ptr, AMgetActorId(doc), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)));

    AMstackItem(NULL, AMmapPutStr(doc, AM_ROOT, AMstr("1"), AMstr("a")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc, AM_ROOT, AMstr("2"), AMstr("b")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc, AM_ROOT, AMstr("3"), AMstr("c")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    AMitems const heads = AMstackItems(stack_ptr, AMgetHeads(doc), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    /* Forward, back, back. */
    AMitems range_all = AMstackItems(stack_ptr, AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr(NULL), &heads), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_STR));
    /* First */
    AMitem* next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    AMbyteSpan key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    AMbyteSpan str = AMitemToStr(next);
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "a", str.count);
    AMobjId const* next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    AMitems range_back_all = AMitemsReversed(&range_all);
    range_back_all = AMitemsRewound(&range_back_all);
    AMitem* next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    AMbyteSpan str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 1);
    assert_memory_equal(str_back.src, "c", str_back.count);
    AMobjId const* next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 1);
    assert_memory_equal(str_back.src, "b", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);

    /* Forward, back, forward. */
    range_all = AMitemsRewound(&range_all);
    range_back_all = AMitemsRewound(&range_back_all);
    /* First */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "a", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 1);
    assert_memory_equal(str_back.src, "c", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "b", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);

    /* Forward, forward, forward. */
    range_all = AMitemsRewound(&range_all);
    /* First */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "a", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Second */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "b", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 1);
    assert_memory_equal(str.src, "c", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Forward stop */
    assert_null(AMitemsNext(&range_all, 1));

    /* Back, back, back. */
    range_back_all = AMitemsRewound(&range_back_all);
    /* Third */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 1);
    assert_memory_equal(str_back.src, "c", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 1);
    assert_memory_equal(str_back.src, "b", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* First */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 1);
    assert_memory_equal(str_back.src, "a", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Back stop */
    assert_null(AMitemsNext(&range_back_all, 1));
}

static void test_map_range_at_back_and_forth_double(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    AMdoc* const doc1 = AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));
    AMactorId const* const actor_id1 =
        AMitemToActorId(AMstackItem(stack_ptr, AMactorIdFromBytes("\0", 1), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)));
    AMstackItem(NULL, AMsetActorId(doc1, actor_id1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    AMstackItem(NULL, AMmapPutStr(doc1, AM_ROOT, AMstr("1"), AMstr("a")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc1, AM_ROOT, AMstr("2"), AMstr("b")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc1, AM_ROOT, AMstr("3"), AMstr("c")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    /* The second actor should win all conflicts here. */
    AMdoc* const doc2 = AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));
    AMactorId const* const actor_id2 =
        AMitemToActorId(AMstackItem(stack_ptr, AMactorIdFromBytes("\1", 1), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)));
    AMstackItem(NULL, AMsetActorId(doc2, actor_id2), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc2, AM_ROOT, AMstr("1"), AMstr("aa")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc2, AM_ROOT, AMstr("2"), AMstr("bb")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc2, AM_ROOT, AMstr("3"), AMstr("cc")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    AMstackItem(NULL, AMmerge(doc1, doc2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMitems const heads = AMstackItems(stack_ptr, AMgetHeads(doc1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    /* Forward, back, back. */
    AMitems range_all = AMstackItems(stack_ptr, AMmapRange(doc1, AM_ROOT, AMstr(NULL), AMstr(NULL), &heads), cmocka_cb,
                                     AMexpect(AM_VAL_TYPE_STR));
    /* First */
    AMitem* next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    AMbyteSpan key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    AMbyteSpan str = AMitemToStr(next);
    assert_int_equal(str.count, 2);
    assert_memory_equal(str.src, "aa", str.count);
    AMobjId const* next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    AMitems range_back_all = AMitemsReversed(&range_all);
    range_back_all = AMitemsRewound(&range_back_all);
    AMitem* next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    AMbyteSpan str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 2);
    assert_memory_equal(str_back.src, "cc", str_back.count);
    AMobjId const* next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 2);
    assert_memory_equal(str_back.src, "bb", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);

    /* Forward, back, forward. */
    range_all = AMitemsRewound(&range_all);
    range_back_all = AMitemsRewound(&range_back_all);
    /* First */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 2);
    assert_memory_equal(str.src, "aa", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 2);
    assert_memory_equal(str_back.src, "cc", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 2);
    assert_memory_equal(str.src, "bb", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);

    /* Forward, forward, forward. */
    range_all = AMitemsRewound(&range_all);
    /* First */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 2);
    assert_memory_equal(str.src, "aa", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Second */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 2);
    assert_memory_equal(str.src, "bb", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    next = AMitemsNext(&range_all, 1);
    assert_non_null(next);
    assert_int_equal(AMitemIdxType(next), AM_IDX_TYPE_KEY);
    key = AMitemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next), AM_VAL_TYPE_STR);
    str = AMitemToStr(next);
    assert_int_equal(str.count, 2);
    assert_memory_equal(str.src, "cc", str.count);
    next_obj_id = AMitemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Forward stop */
    assert_null(AMitemsNext(&range_all, 1));

    /* Back, back, back. */
    range_back_all = AMitemsRewound(&range_back_all);
    /* Third */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 2);
    assert_memory_equal(str_back.src, "cc", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 2);
    assert_memory_equal(str_back.src, "bb", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* First */
    next_back = AMitemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_int_equal(AMitemIdxType(next_back), AM_IDX_TYPE_KEY);
    key = AMitemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    assert_int_equal(AMitemValType(next_back), AM_VAL_TYPE_STR);
    str_back = AMitemToStr(next_back);
    assert_int_equal(str_back.count, 2);
    assert_memory_equal(str_back.src, "aa", str_back.count);
    next_back_obj_id = AMitemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Back stop */
    assert_null(AMitemsNext(&range_back_all, 1));
}

static void test_get_range_values(void** state) {
    BaseState* base_state = *state;
    AMstack** stack_ptr = &base_state->stack;
    AMdoc* const doc1 = AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));
    AMstackItem(NULL, AMmapPutStr(doc1, AM_ROOT, AMstr("aa"), AMstr("aaa")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc1, AM_ROOT, AMstr("bb"), AMstr("bbb")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc1, AM_ROOT, AMstr("cc"), AMstr("ccc")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutStr(doc1, AM_ROOT, AMstr("dd"), AMstr("ddd")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(doc1, AMstr(NULL), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    AMitems const v1 = AMstackItems(stack_ptr, AMgetHeads(doc1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMdoc* const doc2 = AMitemToDoc(AMstackItem(stack_ptr, AMfork(doc1, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)));

    AMstackItem(NULL, AMmapPutStr(doc1, AM_ROOT, AMstr("cc"), AMstr("ccc V2")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(doc1, AMstr(NULL), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    AMstackItem(NULL, AMmapPutStr(doc2, AM_ROOT, AMstr("cc"), AMstr("ccc V3")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(doc2, AMstr(NULL), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    AMstackItem(NULL, AMmerge(doc1, doc2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));

    /* Forward vs. reverse: complete current map range. */
    AMitems range = AMstackItems(stack_ptr, AMmapRange(doc1, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                                 AMexpect(AM_VAL_TYPE_STR));
    size_t size = AMitemsSize(&range);
    assert_int_equal(size, 4);
    AMitems range_back = AMitemsReversed(&range);
    assert_int_equal(AMitemsSize(&range_back), size);
    AMbyteSpan key = AMitemKey(AMitemsNext(&range, 1));
    assert_memory_equal(key.src, "aa", key.count);
    key = AMitemKey(AMitemsNext(&range_back, 1));
    assert_memory_equal(key.src, "dd", key.count);

    AMitem *item1, *item_back1;
    size_t count, middle = size / 2;
    range = AMitemsRewound(&range);
    range_back = AMitemsRewound(&range_back);
    for (item1 = NULL, item_back1 = NULL, count = 0; item1 && item_back1;
         item1 = AMitemsNext(&range, 1), item_back1 = AMitemsNext(&range_back, 1), ++count) {
        assert_int_equal(AMitemIdxType(item1), AM_IDX_TYPE_KEY);
        assert_int_equal(AMitemIdxType(item_back1), AMitemIdxType(item1));
        bool const indices_match = !AMstrcmp(AMitemKey(item_back1), AMitemKey(item1));
        if ((count == middle) && (middle & 1)) {
            /* The iterators are crossing in the middle. */
            assert_true(indices_match);
            assert_true(AMitemEqual(item1, item_back1));
            assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item_back1)));
        } else {
            assert_false(indices_match);
        }
        AMitem* item2 = AMstackItem(stack_ptr, AMmapGet(doc1, AM_ROOT, AMitemKey(item1), NULL), NULL, NULL);
        AMitem* item_back2 = AMstackItem(stack_ptr, AMmapGet(doc1, AM_ROOT, AMitemKey(item_back1), NULL), NULL, NULL);
        /** \note An item returned from an `AM...Get()` call doesn't include the
                  index used to retrieve it. */
        assert_false(AMitemIdxType(item2));
        assert_false(AMitemIdxType(item_back2));
        assert_true(AMitemEqual(item1, item2));
        assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item2)));
        assert_true(AMitemEqual(item1, item2));
        assert_true(AMobjIdEqual(AMitemObjId(item_back1), AMitemObjId(item_back2)));
        AMfree(AMstackPop(stack_ptr, NULL));
    }

    /* Forward vs. reverse: partial current map range. */
    range = AMstackItems(stack_ptr, AMmapRange(doc1, AM_ROOT, AMstr("aa"), AMstr("dd"), NULL), cmocka_cb,
                         AMexpect(AM_VAL_TYPE_STR));
    size = AMitemsSize(&range);
    assert_int_equal(size, 3);
    range_back = AMitemsReversed(&range);
    assert_int_equal(AMitemsSize(&range_back), size);
    key = AMitemKey(AMitemsNext(&range, 1));
    assert_memory_equal(key.src, "aa", key.count);
    key = AMitemKey(AMitemsNext(&range_back, 1));
    assert_memory_equal(key.src, "cc", key.count);

    middle = size / 2;
    range = AMitemsRewound(&range);
    range_back = AMitemsRewound(&range_back);
    for (item1 = NULL, item_back1 = NULL, count = 0; item1 && item_back1;
         item1 = AMitemsNext(&range, 1), item_back1 = AMitemsNext(&range_back, 1), ++count) {
        assert_int_equal(AMitemIdxType(item1), AM_IDX_TYPE_KEY);
        assert_int_equal(AMitemIdxType(item_back1), AMitemIdxType(item1));
        bool const indices_match = !AMstrcmp(AMitemKey(item_back1), AMitemKey(item1));
        if ((count == middle) && (middle & 1)) {
            /* The iterators are crossing in the middle. */
            assert_true(indices_match);
            assert_true(AMitemEqual(item1, item_back1));
            assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item_back1)));
        } else {
            assert_false(indices_match);
        }
        AMitem* item2 = AMstackItem(stack_ptr, AMmapGet(doc1, AM_ROOT, AMitemKey(item1), NULL), NULL, NULL);
        AMitem* item_back2 = AMstackItem(stack_ptr, AMmapGet(doc1, AM_ROOT, AMitemKey(item_back1), NULL), NULL, NULL);
        /** \note An item returned from an `AM...Get()` call doesn't include the
                  index used to retrieve it. */
        assert_false(AMitemIdxType(item2));
        assert_false(AMitemIdxType(item_back2));
        assert_true(AMitemEqual(item1, item2));
        assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item2)));
        assert_true(AMitemEqual(item_back1, item_back2));
        assert_true(AMobjIdEqual(AMitemObjId(item_back1), AMitemObjId(item_back2)));
        AMfree(AMstackPop(stack_ptr, NULL));
    }

    /* Forward vs. reverse: complete historical map range. */
    range = AMstackItems(stack_ptr, AMmapRange(doc1, AM_ROOT, AMstr(NULL), AMstr(NULL), &v1), cmocka_cb,
                         AMexpect(AM_VAL_TYPE_STR));
    size = AMitemsSize(&range);
    assert_int_equal(size, 4);
    range_back = AMitemsReversed(&range);
    assert_int_equal(AMitemsSize(&range_back), size);
    key = AMitemKey(AMitemsNext(&range, 1));
    assert_memory_equal(key.src, "aa", key.count);
    key = AMitemKey(AMitemsNext(&range_back, 1));
    assert_memory_equal(key.src, "dd", key.count);

    middle = size / 2;
    range = AMitemsRewound(&range);
    range_back = AMitemsRewound(&range_back);
    for (item1 = NULL, item_back1 = NULL, count = 0; item1 && item_back1;
         item1 = AMitemsNext(&range, 1), item_back1 = AMitemsNext(&range_back, 1), ++count) {
        assert_int_equal(AMitemIdxType(item1), AM_IDX_TYPE_KEY);
        assert_int_equal(AMitemIdxType(item_back1), AMitemIdxType(item1));
        bool const indices_match = !AMstrcmp(AMitemKey(item_back1), AMitemKey(item1));
        if ((count == middle) && (middle & 1)) {
            /* The iterators are crossing in the middle. */
            assert_true(indices_match);
            assert_true(AMitemEqual(item1, item_back1));
            assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item_back1)));
        } else {
            assert_false(indices_match);
        }
        AMitem* item2 = AMstackItem(stack_ptr, AMmapGet(doc1, AM_ROOT, AMitemKey(item1), &v1), NULL, NULL);
        AMitem* item_back2 = AMstackItem(stack_ptr, AMmapGet(doc1, AM_ROOT, AMitemKey(item_back1), &v1), NULL, NULL);
        /** \note An item returned from an `AM...Get()` call doesn't include the
                  index used to retrieve it. */
        assert_false(AMitemIdxType(item2));
        assert_false(AMitemIdxType(item_back2));
        assert_true(AMitemEqual(item1, item2));
        assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item2)));
        assert_true(AMitemEqual(item_back1, item_back2));
        assert_true(AMobjIdEqual(AMitemObjId(item_back1), AMitemObjId(item_back2)));
        AMfree(AMstackPop(stack_ptr, NULL));
    }

    /* Forward vs. reverse: partial historical map range. */
    range = AMstackItems(stack_ptr, AMmapRange(doc1, AM_ROOT, AMstr("bb"), AMstr(NULL), &v1), cmocka_cb,
                         AMexpect(AM_VAL_TYPE_STR));
    size = AMitemsSize(&range);
    assert_int_equal(size, 3);
    range_back = AMitemsReversed(&range);
    assert_int_equal(AMitemsSize(&range_back), size);
    key = AMitemKey(AMitemsNext(&range, 1));
    assert_memory_equal(key.src, "bb", key.count);
    key = AMitemKey(AMitemsNext(&range_back, 1));
    assert_memory_equal(key.src, "dd", key.count);

    middle = size / 2;
    range = AMitemsRewound(&range);
    range_back = AMitemsRewound(&range_back);
    for (item1 = NULL, item_back1 = NULL, count = 0; item1 && item_back1;
         item1 = AMitemsNext(&range, 1), item_back1 = AMitemsNext(&range_back, 1), ++count) {
        assert_int_equal(AMitemIdxType(item1), AM_IDX_TYPE_KEY);
        assert_int_equal(AMitemIdxType(item_back1), AMitemIdxType(item1));
        bool const indices_match = !AMstrcmp(AMitemKey(item_back1), AMitemKey(item1));
        if ((count == middle) && (middle & 1)) {
            /* The iterators are crossing in the middle. */
            assert_true(indices_match);
            assert_true(AMitemEqual(item1, item_back1));
            assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item_back1)));
        } else {
            assert_false(indices_match);
        }
        AMitem* item2 = AMstackItem(stack_ptr, AMmapGet(doc1, AM_ROOT, AMitemKey(item1), &v1), NULL, NULL);
        AMitem* item_back2 = AMstackItem(stack_ptr, AMmapGet(doc1, AM_ROOT, AMitemKey(item_back1), &v1), NULL, NULL);
        /** \note An item returned from an `AM...Get()` call doesn't include the
                  index used to retrieve it. */
        assert_false(AMitemIdxType(item2));
        assert_false(AMitemIdxType(item_back2));
        assert_true(AMitemEqual(item1, item2));
        assert_true(AMobjIdEqual(AMitemObjId(item1), AMitemObjId(item2)));
        assert_true(AMitemEqual(item_back1, item_back2));
        assert_true(AMobjIdEqual(AMitemObjId(item_back1), AMitemObjId(item_back2)));
        AMfree(AMstackPop(stack_ptr, NULL));
    }

    /* Map range vs. object range: complete current. */
    range = AMstackItems(stack_ptr, AMmapRange(doc1, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL), cmocka_cb,
                         AMexpect(AM_VAL_TYPE_STR));
    AMitems obj_items = AMstackItems(stack_ptr, AMobjItems(doc1, AM_ROOT, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_int_equal(AMitemsSize(&range), AMitemsSize(&obj_items));

    AMitem *item, *obj_item;
    for (item = NULL, obj_item = NULL; item && obj_item;
         item = AMitemsNext(&range, 1), obj_item = AMitemsNext(&obj_items, 1)) {
        /** \note Object iteration doesn't yield any item indices. */
        assert_true(AMitemIdxType(item));
        assert_false(AMitemIdxType(obj_item));
        assert_true(AMitemEqual(item, obj_item));
        assert_true(AMobjIdEqual(AMitemObjId(item), AMitemObjId(obj_item)));
    }

    /* Map range vs. object range: complete historical. */
    range = AMstackItems(stack_ptr, AMmapRange(doc1, AM_ROOT, AMstr(NULL), AMstr(NULL), &v1), cmocka_cb,
                         AMexpect(AM_VAL_TYPE_STR));
    obj_items = AMstackItems(stack_ptr, AMobjItems(doc1, AM_ROOT, &v1), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_int_equal(AMitemsSize(&range), AMitemsSize(&obj_items));

    for (item = NULL, obj_item = NULL; item && obj_item;
         item = AMitemsNext(&range, 1), obj_item = AMitemsNext(&obj_items, 1)) {
        /** \note Object iteration doesn't yield any item indices. */
        assert_true(AMitemIdxType(item));
        assert_false(AMitemIdxType(obj_item));
        assert_true(AMitemEqual(item, obj_item));
        assert_true(AMobjIdEqual(AMitemObjId(item), AMitemObjId(obj_item)));
    }
}

int run_map_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_AMmapIncrement),
        cmocka_unit_test(test_AMmapPut(Bool)),
        cmocka_unit_test(test_AMmapPutBytes),
        cmocka_unit_test(test_AMmapPut(Counter)),
        cmocka_unit_test(test_AMmapPut(F64)),
        cmocka_unit_test(test_AMmapPut(Int)),
        cmocka_unit_test(test_AMmapPutNull),
        cmocka_unit_test(test_AMmapPutObject(List)),
        cmocka_unit_test(test_AMmapPutObject(Map)),
        cmocka_unit_test(test_AMmapPutObject(Text)),
        cmocka_unit_test(test_AMmapPutStr),
        cmocka_unit_test(test_AMmapPut(Timestamp)),
        cmocka_unit_test(test_AMmapPut(Uint)),
        cmocka_unit_test_setup_teardown(test_get_NUL_key, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_get_NUL_string_value, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_range_iter_map, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_map_range_back_and_forth_single, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_map_range_back_and_forth_double, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_map_range_at_back_and_forth_single, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_map_range_at_back_and_forth_double, setup_base, teardown_base),
        cmocka_unit_test_setup_teardown(test_get_range_values, setup_base, teardown_base),
    };

    return cmocka_run_group_tests(tests, setup_doc, teardown_doc);
}
