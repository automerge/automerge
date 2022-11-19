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
#include "cmocka_utils.h"
#include "group_state.h"
#include "macro_utils.h"
#include "stack_utils.h"

static void test_AMmapIncrement(void** state) {
    GroupState* group_state = *state;
    AMfree(AMmapPutCounter(group_state->doc, AM_ROOT, AMstr("Counter"), 0));
    assert_int_equal(AMpush(&group_state->stack,
                            AMmapGet(group_state->doc, AM_ROOT, AMstr("Counter"), NULL),
                            AM_VALUE_COUNTER,
                            cmocka_cb).counter, 0);
    AMfree(AMpop(&group_state->stack));
    AMfree(AMmapIncrement(group_state->doc, AM_ROOT, AMstr("Counter"), 3));
    assert_int_equal(AMpush(&group_state->stack,
                            AMmapGet(group_state->doc, AM_ROOT, AMstr("Counter"), NULL),
                            AM_VALUE_COUNTER,
                            cmocka_cb).counter, 3);
    AMfree(AMpop(&group_state->stack));
}

#define test_AMmapPut(suffix) test_AMmapPut ## suffix

#define static_void_test_AMmapPut(suffix, member, scalar_value)               \
static void test_AMmapPut ## suffix(void **state) {                           \
    GroupState* group_state = *state;                                         \
    AMfree(AMmapPut ## suffix(group_state->doc,                               \
                              AM_ROOT,                                        \
                              AMstr(#suffix),                                 \
                              scalar_value));                                 \
    assert_true(AMpush(                                                       \
        &group_state->stack,                                                  \
        AMmapGet(group_state->doc, AM_ROOT, AMstr(#suffix), NULL),            \
        AMvalue_discriminant(#suffix),                                        \
        cmocka_cb).member == scalar_value);                                   \
    AMfree(AMpop(&group_state->stack));                                       \
}

static void test_AMmapPutBytes(void **state) {
    static AMbyteSpan const KEY = {"Bytes", 5};
    static uint8_t const BYTES_VALUE[] = {INT8_MIN, INT8_MAX / 2, INT8_MAX};
    static size_t const BYTES_SIZE = sizeof(BYTES_VALUE) / sizeof(uint8_t);

    GroupState* group_state = *state;
    AMfree(AMmapPutBytes(group_state->doc,
                         AM_ROOT,
                         KEY,
                         BYTES_VALUE,
                         BYTES_SIZE));
    AMbyteSpan const bytes = AMpush(&group_state->stack,
                                    AMmapGet(group_state->doc, AM_ROOT, KEY, NULL),
                                    AM_VALUE_BYTES,
                                    cmocka_cb).bytes;
    assert_int_equal(bytes.count, BYTES_SIZE);
    assert_memory_equal(bytes.src, BYTES_VALUE, BYTES_SIZE);
    AMfree(AMpop(&group_state->stack));
}

static void test_AMmapPutNull(void **state) {
    static AMbyteSpan const KEY = {"Null", 4};

    GroupState* group_state = *state;
    AMfree(AMmapPutNull(group_state->doc, AM_ROOT, KEY));
    AMresult* const result = AMmapGet(group_state->doc, AM_ROOT, KEY, NULL);
    if (AMresultStatus(result) != AM_STATUS_OK) {
        fail_msg_view("%s", AMerrorMessage(result));
    }
    assert_int_equal(AMresultSize(result), 1);
    assert_int_equal(AMresultValue(result).tag, AM_VALUE_NULL);
    AMfree(result);
}

#define test_AMmapPutObject(label) test_AMmapPutObject_ ## label

#define static_void_test_AMmapPutObject(label)                                \
static void test_AMmapPutObject_ ## label(void **state) {                     \
    GroupState* group_state = *state;                                         \
    AMobjType const obj_type = AMobjType_tag(#label);                         \
    if (obj_type != AM_OBJ_TYPE_VOID) {                                       \
        AMobjId const* const obj_id = AMpush(                                 \
            &group_state->stack,                                              \
            AMmapPutObject(group_state->doc,                                  \
                           AM_ROOT,                                           \
                           AMstr(#label),                                     \
                           obj_type),                                         \
            AM_VALUE_OBJ_ID,                                                  \
            cmocka_cb).obj_id;                                                \
        assert_non_null(obj_id);                                              \
        assert_int_equal(AMobjObjType(group_state->doc, obj_id), obj_type);   \
        assert_int_equal(AMobjSize(group_state->doc, obj_id, NULL), 0);       \
    }                                                                         \
    else {                                                                    \
        AMpush(&group_state->stack,                                           \
               AMmapPutObject(group_state->doc,                               \
                              AM_ROOT,                                        \
                              AMstr(#label),                                  \
                              obj_type),                                      \
               AM_VALUE_VOID,                                                 \
               NULL);                                                         \
        assert_int_not_equal(AMresultStatus(group_state->stack->result),      \
                                            AM_STATUS_OK);                    \
    }                                                                         \
    AMfree(AMpop(&group_state->stack));                                       \
}

static void test_AMmapPutStr(void **state) {
    GroupState* group_state = *state;
    AMfree(AMmapPutStr(group_state->doc, AM_ROOT, AMstr("Str"), AMstr("Hello, world!")));
    AMbyteSpan const str = AMpush(&group_state->stack,
                                  AMmapGet(group_state->doc, AM_ROOT, AMstr("Str"), NULL),
                                  AM_VALUE_STR,
                                  cmocka_cb).str;
    assert_int_equal(str.count, strlen("Hello, world!"));
    assert_memory_equal(str.src, "Hello, world!", str.count);
    AMfree(AMpop(&group_state->stack));
}

static_void_test_AMmapPut(Bool, boolean, true)

static_void_test_AMmapPut(Counter, counter, INT64_MAX)

static_void_test_AMmapPut(F64, f64, DBL_MAX)

static_void_test_AMmapPut(Int, int_, INT64_MAX)

static_void_test_AMmapPutObject(List)

static_void_test_AMmapPutObject(Map)

static_void_test_AMmapPutObject(Text)

static_void_test_AMmapPutObject(Void)

static_void_test_AMmapPut(Timestamp, timestamp, INT64_MAX)

static_void_test_AMmapPut(Uint, uint, UINT64_MAX)

/** \brief A JavaScript application can introduce NUL (`\0`) characters into a
 *         string which truncates them for a C application.
 */
static void test_get_NUL_string(void** state) {
    /*
    import * as Automerge from "@automerge/automerge"
    let doc = Automerge.init()
    doc = Automerge.change(doc, doc => {
        doc.oops = 'o\0ps'
    })
    const bytes = Automerge.save(doc)
    console.log("static uint8_t const SAVED_DOC[] = {" + Array.apply([], bytes).join(", ") + "};");
    */
    static uint8_t const OOPS_VALUE[] = {'o', '\0', 'p', 's'};
    static size_t const OOPS_SIZE = sizeof(OOPS_VALUE) / sizeof(uint8_t);

    static uint8_t const SAVED_DOC[] = {
        133, 111, 74, 131, 63, 94, 151, 29, 0, 116, 1, 16, 156, 159, 189, 12,
        125, 55, 71, 154, 136, 104, 237, 186, 45, 224, 32, 22, 1, 36, 163,
        164, 222, 81, 42, 1, 247, 231, 156, 54, 222, 76, 6, 109, 18, 172, 75,
        36, 118, 120, 68, 73, 87, 186, 230, 127, 68, 19, 81, 149, 185, 6, 1,
        2, 3, 2, 19, 2, 35, 2, 64, 2, 86, 2, 8, 21, 6, 33, 2, 35, 2, 52, 1,
        66, 2, 86, 2, 87, 4, 128, 1, 2, 127, 0, 127, 1, 127, 1, 127, 0, 127,
        0, 127, 7, 127, 4, 111, 111, 112, 115, 127, 0, 127, 1, 1, 127, 1, 127,
        70, 111, 0, 112, 115, 127, 0, 0
    };
    static size_t const SAVED_DOC_SIZE = sizeof(SAVED_DOC) / sizeof(uint8_t);

    AMresultStack* stack = *state;
    AMdoc* const doc = AMpush(&stack,
                              AMload(SAVED_DOC, SAVED_DOC_SIZE),
                              AM_VALUE_DOC,
                              cmocka_cb).doc;
    AMbyteSpan const str = AMpush(&stack,
                                  AMmapGet(doc, AM_ROOT, AMstr("oops"), NULL),
                                  AM_VALUE_STR,
                                  cmocka_cb).str;
    assert_int_equal(str.count, OOPS_SIZE);
    assert_memory_equal(str.src, OOPS_VALUE, str.count);
}

static void test_range_iter_map(void** state) {
    AMresultStack* stack = *state;
    AMdoc* const doc = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMmapPutUint(doc, AM_ROOT, AMstr("a"), 3));
    AMfree(AMmapPutUint(doc, AM_ROOT, AMstr("b"), 4));
    AMfree(AMmapPutUint(doc, AM_ROOT, AMstr("c"), 5));
    AMfree(AMmapPutUint(doc, AM_ROOT, AMstr("d"), 6));
    AMfree(AMcommit(doc, AMstr(NULL), NULL));
    AMfree(AMmapPutUint(doc, AM_ROOT, AMstr("a"), 7));
    AMfree(AMcommit(doc, AMstr(NULL), NULL));
    AMfree(AMmapPutUint(doc, AM_ROOT, AMstr("a"), 8));
    AMfree(AMmapPutUint(doc, AM_ROOT, AMstr("d"), 9));
    AMfree(AMcommit(doc, AMstr(NULL), NULL));
    AMactorId const* const actor_id = AMpush(&stack,
                                             AMgetActorId(doc),
                                             AM_VALUE_ACTOR_ID,
                                             cmocka_cb).actor_id;
    AMmapItems map_items = AMpush(&stack,
                                  AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL),
                                  AM_VALUE_MAP_ITEMS,
                                  cmocka_cb).map_items;
    assert_int_equal(AMmapItemsSize(&map_items), 4);

    /* ["b"-"d") */
    AMmapItems range = AMpush(&stack,
                              AMmapRange(doc, AM_ROOT, AMstr("b"), AMstr("d"), NULL),
                              AM_VALUE_MAP_ITEMS,
                              cmocka_cb).map_items;
    /* First */
    AMmapItem const* next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    AMbyteSpan key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "b", key.count);
    AMvalue next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_UINT);
    assert_int_equal(next_value.uint, 4);
    AMobjId const* next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Second */
    next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "c", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_UINT);
    assert_int_equal(next_value.uint, 5);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    assert_null(AMmapItemsNext(&range, 1));

    /* ["b"-<key-n>) */
    range = AMpush(&stack,
                   AMmapRange(doc, AM_ROOT, AMstr("b"), AMstr(NULL), NULL),
                   AM_VALUE_MAP_ITEMS,
                   cmocka_cb).map_items;
    /* First */
    next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "b", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_UINT);
    assert_int_equal(next_value.uint, 4);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Second */
    next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "c", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_UINT);
    assert_int_equal(next_value.uint, 5);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "d", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_UINT);
    assert_int_equal(next_value.uint, 9);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 7);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Fourth */
    assert_null(AMmapItemsNext(&range, 1));

    /* [<key-0>-"d") */
    range = AMpush(&stack,
                   AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr("d"), NULL),
                   AM_VALUE_MAP_ITEMS,
                   cmocka_cb).map_items;
    /* First */
    next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "a", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_UINT);
    assert_int_equal(next_value.uint, 8);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 6);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Second */
    next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "b", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_UINT);
    assert_int_equal(next_value.uint, 4);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "c", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_UINT);
    assert_int_equal(next_value.uint, 5);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Fourth */
    assert_null(AMmapItemsNext(&range, 1));

    /* ["a"-<key-n>) */
    range = AMpush(&stack,
                   AMmapRange(doc, AM_ROOT, AMstr("a"), AMstr(NULL), NULL),
                   AM_VALUE_MAP_ITEMS,
                   cmocka_cb).map_items;
    /* First */
    next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "a", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_UINT);
    assert_int_equal(next_value.uint, 8);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 6);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Second */
    next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "b", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_UINT);
    assert_int_equal(next_value.uint, 4);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "c", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_UINT);
    assert_int_equal(next_value.uint, 5);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Fourth */
    next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "d", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_UINT);
    assert_int_equal(next_value.uint, 9);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 7);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Fifth */
    assert_null(AMmapItemsNext(&range, 1));
}

static void test_map_range_back_and_forth_single(void** state) {
    AMresultStack* stack = *state;
    AMdoc* const doc = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMactorId const* const actor_id = AMpush(&stack,
                                             AMgetActorId(doc),
                                             AM_VALUE_ACTOR_ID,
                                             cmocka_cb).actor_id;

    AMfree(AMmapPutStr(doc, AM_ROOT, AMstr("1"), AMstr("a")));
    AMfree(AMmapPutStr(doc, AM_ROOT, AMstr("2"), AMstr("b")));
    AMfree(AMmapPutStr(doc, AM_ROOT, AMstr("3"), AMstr("c")));

    /* Forward, back, back. */
    AMmapItems range_all = AMpush(&stack,
                                  AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL),
                                  AM_VALUE_MAP_ITEMS,
                                  cmocka_cb).map_items;
    /* First */
    AMmapItem const* next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    AMbyteSpan key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    AMvalue next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 1);
    assert_memory_equal(next_value.str.src, "a", next_value.str.count);
    AMobjId const* next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    AMmapItems range_back_all = AMmapItemsReversed(&range_all);
    range_back_all = AMmapItemsRewound(&range_back_all);
    AMmapItem const* next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    AMvalue next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 1);
    assert_memory_equal(next_back_value.str.src, "c", next_back_value.str.count);
    AMobjId const* next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 1);
    assert_memory_equal(next_back_value.str.src, "b", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);

    /* Forward, back, forward. */
    range_all = AMmapItemsRewound(&range_all);
    range_back_all = AMmapItemsRewound(&range_back_all);
    /* First */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 1);
    assert_memory_equal(next_value.str.src, "a", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 1);
    assert_memory_equal(next_back_value.str.src, "c", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 1);
    assert_memory_equal(next_value.str.src, "b", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);

    /* Forward, forward, forward. */
    range_all = AMmapItemsRewound(&range_all);
    /* First */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 1);
    assert_memory_equal(next_value.str.src, "a", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 1);
    assert_memory_equal(next_value.str.src, "b", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 1);
    assert_memory_equal(next_value.str.src, "c", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Forward stop */
    assert_null(AMmapItemsNext(&range_all, 1));

    /* Back, back, back. */
    range_back_all = AMmapItemsRewound(&range_back_all);
    /* Third */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 1);
    assert_memory_equal(next_back_value.str.src, "c", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 1);
    assert_memory_equal(next_back_value.str.src, "b", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* First */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 1);
    assert_memory_equal(next_back_value.str.src, "a", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Back stop */
    assert_null(AMmapItemsNext(&range_back_all, 1));
}

static void test_map_range_back_and_forth_double(void** state) {
    AMresultStack* stack = *state;
    AMdoc* const doc1 = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMactorId const* const actor_id1= AMpush(&stack,
                                             AMactorIdInitBytes("\0", 1),
                                             AM_VALUE_ACTOR_ID,
                                             cmocka_cb).actor_id;
    AMfree(AMsetActorId(doc1, actor_id1));

    AMfree(AMmapPutStr(doc1, AM_ROOT, AMstr("1"), AMstr("a")));
    AMfree(AMmapPutStr(doc1, AM_ROOT, AMstr("2"), AMstr("b")));
    AMfree(AMmapPutStr(doc1, AM_ROOT, AMstr("3"), AMstr("c")));

    /* The second actor should win all conflicts here. */
    AMdoc* const doc2 = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMactorId const* const actor_id2 = AMpush(&stack,
                                              AMactorIdInitBytes("\1", 1),
                                              AM_VALUE_ACTOR_ID,
                                              cmocka_cb).actor_id;
    AMfree(AMsetActorId(doc2, actor_id2));
    AMfree(AMmapPutStr(doc2, AM_ROOT, AMstr("1"), AMstr("aa")));
    AMfree(AMmapPutStr(doc2, AM_ROOT, AMstr("2"), AMstr("bb")));
    AMfree(AMmapPutStr(doc2, AM_ROOT, AMstr("3"), AMstr("cc")));

    AMfree(AMmerge(doc1, doc2));

    /* Forward, back, back. */
    AMmapItems range_all = AMpush(&stack,
                                  AMmapRange(doc1, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL),
                                  AM_VALUE_MAP_ITEMS,
                                  cmocka_cb).map_items;
    /* First */
    AMmapItem const* next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    AMbyteSpan key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    AMvalue next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 2);
    assert_memory_equal(next_value.str.src, "aa", next_value.str.count);
    AMobjId const* next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    AMmapItems range_back_all = AMmapItemsReversed(&range_all);
    range_back_all = AMmapItemsRewound(&range_back_all);
    AMmapItem const* next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    AMvalue next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 2);
    assert_memory_equal(next_back_value.str.src, "cc", next_back_value.str.count);
    AMobjId const* next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 2);
    assert_memory_equal(next_back_value.str.src, "bb", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);

    /* Forward, back, forward. */
    range_all = AMmapItemsRewound(&range_all);
    range_back_all = AMmapItemsRewound(&range_back_all);
    /* First */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 2);
    assert_memory_equal(next_value.str.src, "aa", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 2);
    assert_memory_equal(next_back_value.str.src, "cc", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 2);
    assert_memory_equal(next_value.str.src, "bb", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);

    /* Forward, forward, forward. */
    range_all = AMmapItemsRewound(&range_all);
    /* First */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 2);
    assert_memory_equal(next_value.str.src, "aa", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 2);
    assert_memory_equal(next_value.str.src, "bb", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 2);
    assert_memory_equal(next_value.str.src, "cc", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Forward stop */
    assert_null(AMmapItemsNext(&range_all, 1));

    /* Back, back, back. */
    range_back_all = AMmapItemsRewound(&range_back_all);
    /* Third */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 2);
    assert_memory_equal(next_back_value.str.src, "cc", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 2);
    assert_memory_equal(next_back_value.str.src, "bb", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* First */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 2);
    assert_memory_equal(next_back_value.str.src, "aa", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Back stop */
    assert_null(AMmapItemsNext(&range_back_all, 1));
}

static void test_map_range_at_back_and_forth_single(void** state) {
    AMresultStack* stack = *state;
    AMdoc* const doc = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMactorId const* const actor_id = AMpush(&stack,
                                             AMgetActorId(doc),
                                             AM_VALUE_ACTOR_ID,
                                             cmocka_cb).actor_id;

    AMfree(AMmapPutStr(doc, AM_ROOT, AMstr("1"), AMstr("a")));
    AMfree(AMmapPutStr(doc, AM_ROOT, AMstr("2"), AMstr("b")));
    AMfree(AMmapPutStr(doc, AM_ROOT, AMstr("3"), AMstr("c")));

    AMchangeHashes const heads = AMpush(&stack,
                                        AMgetHeads(doc),
                                        AM_VALUE_CHANGE_HASHES,
                                        cmocka_cb).change_hashes;

    /* Forward, back, back. */
    AMmapItems range_all = AMpush(&stack,
                                  AMmapRange(doc, AM_ROOT, AMstr(NULL), AMstr(NULL), &heads),
                                  AM_VALUE_MAP_ITEMS,
                                  cmocka_cb).map_items;
    /* First */
    AMmapItem const* next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    AMbyteSpan key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    AMvalue next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 1);
    assert_memory_equal(next_value.str.src, "a", next_value.str.count);
    AMobjId const* next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    AMmapItems range_back_all = AMmapItemsReversed(&range_all);
    range_back_all = AMmapItemsRewound(&range_back_all);
    AMmapItem const* next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    AMvalue next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 1);
    assert_memory_equal(next_back_value.str.src, "c", next_back_value.str.count);
    AMobjId const* next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 1);
    assert_memory_equal(next_back_value.str.src, "b", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);

    /* Forward, back, forward. */
    range_all = AMmapItemsRewound(&range_all);
    range_back_all = AMmapItemsRewound(&range_back_all);
    /* First */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 1);
    assert_memory_equal(next_value.str.src, "a", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 1);
    assert_memory_equal(next_back_value.str.src, "c", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 1);
    assert_memory_equal(next_value.str.src, "b", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);

    /* Forward, forward, forward. */
    range_all = AMmapItemsRewound(&range_all);
    /* First */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 1);
    assert_memory_equal(next_value.str.src, "a", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 1);
    assert_memory_equal(next_value.str.src, "b", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 1);
    assert_memory_equal(next_value.str.src, "c", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Forward stop */
    assert_null(AMmapItemsNext(&range_all, 1));

    /* Back, back, back. */
    range_back_all = AMmapItemsRewound(&range_back_all);
    /* Third */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 1);
    assert_memory_equal(next_back_value.str.src, "c", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 1);
    assert_memory_equal(next_back_value.str.src, "b", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* First */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 1);
    assert_memory_equal(next_back_value.str.src, "a", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Back stop */
    assert_null(AMmapItemsNext(&range_back_all, 1));
}

static void test_map_range_at_back_and_forth_double(void** state) {
    AMresultStack* stack = *state;
    AMdoc* const doc1 = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMactorId const* const actor_id1= AMpush(&stack,
                                             AMactorIdInitBytes("\0", 1),
                                             AM_VALUE_ACTOR_ID,
                                             cmocka_cb).actor_id;
    AMfree(AMsetActorId(doc1, actor_id1));

    AMfree(AMmapPutStr(doc1, AM_ROOT, AMstr("1"), AMstr("a")));
    AMfree(AMmapPutStr(doc1, AM_ROOT, AMstr("2"), AMstr("b")));
    AMfree(AMmapPutStr(doc1, AM_ROOT, AMstr("3"), AMstr("c")));

    /* The second actor should win all conflicts here. */
    AMdoc* const doc2 = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMactorId const* const actor_id2= AMpush(&stack,
                                             AMactorIdInitBytes("\1", 1),
                                             AM_VALUE_ACTOR_ID,
                                             cmocka_cb).actor_id;
    AMfree(AMsetActorId(doc2, actor_id2));
    AMfree(AMmapPutStr(doc2, AM_ROOT, AMstr("1"), AMstr("aa")));
    AMfree(AMmapPutStr(doc2, AM_ROOT, AMstr("2"), AMstr("bb")));
    AMfree(AMmapPutStr(doc2, AM_ROOT, AMstr("3"), AMstr("cc")));

    AMfree(AMmerge(doc1, doc2));
    AMchangeHashes const heads = AMpush(&stack,
                                        AMgetHeads(doc1),
                                        AM_VALUE_CHANGE_HASHES,
                                        cmocka_cb).change_hashes;

    /* Forward, back, back. */
    AMmapItems range_all = AMpush(&stack,
                                  AMmapRange(doc1, AM_ROOT, AMstr(NULL), AMstr(NULL), &heads),
                                  AM_VALUE_MAP_ITEMS,
                                  cmocka_cb).map_items;
    /* First */
    AMmapItem const* next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    AMbyteSpan key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    AMvalue next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 2);
    assert_memory_equal(next_value.str.src, "aa", next_value.str.count);
    AMobjId const* next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    AMmapItems range_back_all = AMmapItemsReversed(&range_all);
    range_back_all = AMmapItemsRewound(&range_back_all);
    AMmapItem const* next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    AMvalue next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 2);
    assert_memory_equal(next_back_value.str.src, "cc", next_back_value.str.count);
    AMobjId const* next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 2);
    assert_memory_equal(next_back_value.str.src, "bb", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);

    /* Forward, back, forward. */
    range_all = AMmapItemsRewound(&range_all);
    range_back_all = AMmapItemsRewound(&range_back_all);
    /* First */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 2);
    assert_memory_equal(next_value.str.src, "aa", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 2);
    assert_memory_equal(next_back_value.str.src, "cc", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 2);
    assert_memory_equal(next_value.str.src, "bb", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);

    /* Forward, forward, forward. */
    range_all = AMmapItemsRewound(&range_all);
    /* First */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 2);
    assert_memory_equal(next_value.str.src, "aa", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 2);
    assert_memory_equal(next_value.str.src, "bb", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    key = AMmapItemKey(next);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_int_equal(next_value.str.count, 2);
    assert_memory_equal(next_value.str.src, "cc", next_value.str.count);
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Forward stop */
    assert_null(AMmapItemsNext(&range_all, 1));

    /* Back, back, back. */
    range_back_all = AMmapItemsRewound(&range_back_all);
    /* Third */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "3", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 2);
    assert_memory_equal(next_back_value.str.src, "cc", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "2", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 2);
    assert_memory_equal(next_back_value.str.src, "bb", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* First */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    key = AMmapItemKey(next_back);
    assert_int_equal(key.count, 1);
    assert_memory_equal(key.src, "1", key.count);
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_int_equal(next_back_value.str.count, 2);
    assert_memory_equal(next_back_value.str.src, "aa", next_back_value.str.count);
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Back stop */
    assert_null(AMmapItemsNext(&range_back_all, 1));
}

static void test_get_range_values(void** state) {
    AMresultStack* stack = *state;
    AMdoc* const doc1 = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMmapPutStr(doc1, AM_ROOT, AMstr("aa"), AMstr("aaa")));
    AMfree(AMmapPutStr(doc1, AM_ROOT, AMstr("bb"), AMstr("bbb")));
    AMfree(AMmapPutStr(doc1, AM_ROOT, AMstr("cc"), AMstr("ccc")));
    AMfree(AMmapPutStr(doc1, AM_ROOT, AMstr("dd"), AMstr("ddd")));
    AMfree(AMcommit(doc1, AMstr(NULL), NULL));

    AMchangeHashes const v1 = AMpush(&stack,
                                     AMgetHeads(doc1),
                                     AM_VALUE_CHANGE_HASHES,
                                     cmocka_cb).change_hashes;
    AMdoc* const doc2 = AMpush(&stack, AMfork(doc1, NULL), AM_VALUE_DOC, cmocka_cb).doc;

    AMfree(AMmapPutStr(doc1, AM_ROOT, AMstr("cc"), AMstr("ccc V2")));
    AMfree(AMcommit(doc1, AMstr(NULL), NULL));

    AMfree(AMmapPutStr(doc2, AM_ROOT, AMstr("cc"), AMstr("ccc V3")));
    AMfree(AMcommit(doc2, AMstr(NULL), NULL));

    AMfree(AMmerge(doc1, doc2));

    AMmapItems range = AMpush(&stack,
                              AMmapRange(doc1, AM_ROOT, AMstr("b"), AMstr("d"), NULL),
                              AM_VALUE_MAP_ITEMS,
                              cmocka_cb).map_items;
    AMmapItems range_back = AMmapItemsReversed(&range);
    assert_int_equal(AMmapItemsSize(&range), 2);

    AMmapItem const* map_item = NULL;
    while ((map_item = AMmapItemsNext(&range, 1)) != NULL) {
        AMvalue const val1 = AMmapItemValue(map_item);
        AMresult* result = AMmapGet(doc1, AM_ROOT, AMmapItemKey(map_item), NULL);
        AMvalue const val2 = AMresultValue(result);
        assert_true(AMvalueEqual(&val1, &val2));
        assert_non_null(AMmapItemObjId(map_item));
        AMfree(result);
    }

    assert_int_equal(AMmapItemsSize(&range_back), 2);

    while ((map_item = AMmapItemsNext(&range_back, 1)) != NULL) {
        AMvalue const val1 = AMmapItemValue(map_item);
        AMresult* result = AMmapGet(doc1, AM_ROOT, AMmapItemKey(map_item), NULL);
        AMvalue const val2 = AMresultValue(result);
        assert_true(AMvalueEqual(&val1, &val2));
        assert_non_null(AMmapItemObjId(map_item));
        AMfree(result);
    }

    range = AMpush(&stack,
                   AMmapRange(doc1, AM_ROOT, AMstr("b"), AMstr("d"), &v1),
                   AM_VALUE_MAP_ITEMS,
                   cmocka_cb).map_items;
    range_back = AMmapItemsReversed(&range);
    assert_int_equal(AMmapItemsSize(&range), 2);

    while ((map_item = AMmapItemsNext(&range, 1)) != NULL) {
        AMvalue const val1 = AMmapItemValue(map_item);
        AMresult* result = AMmapGet(doc1, AM_ROOT, AMmapItemKey(map_item), &v1);
        AMvalue const val2 = AMresultValue(result);
        assert_true(AMvalueEqual(&val1, &val2));
        assert_non_null(AMmapItemObjId(map_item));
        AMfree(result);
    }

    assert_int_equal(AMmapItemsSize(&range_back), 2);

    while ((map_item = AMmapItemsNext(&range_back, 1)) != NULL) {
        AMvalue const val1 = AMmapItemValue(map_item);
        AMresult* result = AMmapGet(doc1, AM_ROOT, AMmapItemKey(map_item), &v1);
        AMvalue const val2 = AMresultValue(result);
        assert_true(AMvalueEqual(&val1, &val2));
        assert_non_null(AMmapItemObjId(map_item));
        AMfree(result);
    }

    range = AMpush(&stack,
                   AMmapRange(doc1, AM_ROOT, AMstr(NULL), AMstr(NULL), NULL),
                   AM_VALUE_MAP_ITEMS,
                   cmocka_cb).map_items;
    AMobjItems values = AMpush(&stack,
                               AMobjValues(doc1, AM_ROOT, NULL),
                               AM_VALUE_OBJ_ITEMS,
                               cmocka_cb).obj_items;
    assert_int_equal(AMmapItemsSize(&range), AMobjItemsSize(&values));
    AMobjItem const* value = NULL;
    while ((map_item = AMmapItemsNext(&range, 1)) != NULL &&
           (value = AMobjItemsNext(&values, 1)) != NULL) {
        AMvalue const val1 = AMmapItemValue(map_item);
        AMvalue const val2 = AMobjItemValue(value);
        assert_true(AMvalueEqual(&val1, &val2));
        assert_true(AMobjIdEqual(AMmapItemObjId(map_item), AMobjItemObjId(value)));
    }

    range = AMpush(&stack,
                   AMmapRange(doc1, AM_ROOT, AMstr(NULL), AMstr(NULL), &v1),
                   AM_VALUE_MAP_ITEMS,
                   cmocka_cb).map_items;
    values = AMpush(&stack,
                    AMobjValues(doc1, AM_ROOT, &v1),
                    AM_VALUE_OBJ_ITEMS,
                    cmocka_cb).obj_items;
    assert_int_equal(AMmapItemsSize(&range), AMobjItemsSize(&values));
    while ((map_item = AMmapItemsNext(&range, 1)) != NULL &&
           (value = AMobjItemsNext(&values, 1)) != NULL) {
        AMvalue const val1 = AMmapItemValue(map_item);
        AMvalue const val2 = AMobjItemValue(value);
        assert_true(AMvalueEqual(&val1, &val2));
        assert_true(AMobjIdEqual(AMmapItemObjId(map_item), AMobjItemObjId(value)));
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
        cmocka_unit_test(test_AMmapPutObject(Void)),
        cmocka_unit_test(test_AMmapPutStr),
        cmocka_unit_test(test_AMmapPut(Timestamp)),
        cmocka_unit_test(test_AMmapPut(Uint)),
        cmocka_unit_test_setup_teardown(test_get_NUL_string, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_range_iter_map, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_map_range_back_and_forth_single, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_map_range_back_and_forth_double, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_map_range_at_back_and_forth_single, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_map_range_at_back_and_forth_double, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_get_range_values, setup_stack, teardown_stack),
    };

    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
