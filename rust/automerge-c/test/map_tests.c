#include <float.h>
#include <limits.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

/* third-party */
#include <cmocka.h>

/* local */
#include <automerge-c/automerge.h>
#include "group_state.h"
#include "macro_utils.h"
#include "stack_utils.h"

static void test_AMmapIncrement(void** state) {
    GroupState* group_state = *state;
    AMfree(AMmapPutCounter(group_state->doc, AM_ROOT, "Counter", 0));
    assert_int_equal(AMpush(&group_state->stack,
                            AMmapGet(group_state->doc, AM_ROOT, "Counter", NULL),
                            AM_VALUE_COUNTER,
                            cmocka_cb).counter, 0);
    AMfree(AMpop(&group_state->stack));
    AMfree(AMmapIncrement(group_state->doc, AM_ROOT, "Counter", 3));
    assert_int_equal(AMpush(&group_state->stack,
                            AMmapGet(group_state->doc, AM_ROOT, "Counter", NULL),
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
                              #suffix,                                        \
                              scalar_value));                                 \
    assert_true(AMpush(                                                       \
        &group_state->stack,                                                  \
        AMmapGet(group_state->doc, AM_ROOT, #suffix, NULL),                   \
        AMvalue_discriminant(#suffix),                                        \
        cmocka_cb).member == scalar_value);                                   \
    AMfree(AMpop(&group_state->stack));                                       \
}

static void test_AMmapPutBytes(void **state) {
    static char const* const KEY = "Bytes";
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
    static char const* const KEY = "Null";

    GroupState* group_state = *state;
    AMfree(AMmapPutNull(group_state->doc, AM_ROOT, KEY));
    AMresult* const result = AMmapGet(group_state->doc, AM_ROOT, KEY, NULL);
    if (AMresultStatus(result) != AM_STATUS_OK) {
        fail_msg("%s", AMerrorMessage(result));
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
                           #label,                                            \
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
                              #label,                                         \
                              obj_type),                                      \
               AM_VALUE_VOID,                                                 \
               NULL);                                                         \
        assert_int_not_equal(AMresultStatus(group_state->stack->result),      \
                                            AM_STATUS_OK);                    \
    }                                                                         \
    AMfree(AMpop(&group_state->stack));                                       \
}

static void test_AMmapPutStr(void **state) {
    static char const* const KEY = "Str";
    static char const* const STR_VALUE = "Hello, world!";

    GroupState* group_state = *state;
    AMfree(AMmapPutStr(group_state->doc, AM_ROOT, KEY, STR_VALUE));
    assert_string_equal(AMpush(&group_state->stack,
                               AMmapGet(group_state->doc, AM_ROOT, KEY, NULL),
                               AM_VALUE_STR,
                               cmocka_cb).str, STR_VALUE);
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

static void test_range_iter_map(void** state) {
    AMresultStack* stack = *state;
    AMdoc* const doc = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMfree(AMmapPutUint(doc, AM_ROOT, "a", 3));
    AMfree(AMmapPutUint(doc, AM_ROOT, "b", 4));
    AMfree(AMmapPutUint(doc, AM_ROOT, "c", 5));
    AMfree(AMmapPutUint(doc, AM_ROOT, "d", 6));
    AMfree(AMcommit(doc, NULL, NULL));
    AMfree(AMmapPutUint(doc, AM_ROOT, "a", 7));
    AMfree(AMcommit(doc, NULL, NULL));
    AMfree(AMmapPutUint(doc, AM_ROOT, "a", 8));
    AMfree(AMmapPutUint(doc, AM_ROOT, "d", 9));
    AMfree(AMcommit(doc, NULL, NULL));
    AMactorId const* const actor_id = AMpush(&stack,
                                             AMgetActorId(doc),
                                             AM_VALUE_ACTOR_ID,
                                             cmocka_cb).actor_id;
    AMmapItems map_items = AMpush(&stack,
                                  AMmapRange(doc, AM_ROOT, NULL, NULL, NULL),
                                  AM_VALUE_MAP_ITEMS,
                                  cmocka_cb).map_items;
    assert_int_equal(AMmapItemsSize(&map_items), 4);

    /* ["b"-"d") */
    AMmapItems range = AMpush(&stack,
                              AMmapRange(doc, AM_ROOT, "b", "d", NULL),
                              AM_VALUE_MAP_ITEMS,
                              cmocka_cb).map_items;
    /* First */
    AMmapItem const* next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "b");
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
    assert_string_equal(AMmapItemKey(next), "c");
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
                   AMmapRange(doc, AM_ROOT, "b", NULL, NULL),
                   AM_VALUE_MAP_ITEMS,
                   cmocka_cb).map_items;
    /* First */
    next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "b");
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
    assert_string_equal(AMmapItemKey(next), "c");
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
    assert_string_equal(AMmapItemKey(next), "d");
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
                   AMmapRange(doc, AM_ROOT, NULL, "d", NULL),
                   AM_VALUE_MAP_ITEMS,
                   cmocka_cb).map_items;
    /* First */
    next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "a");
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
    assert_string_equal(AMmapItemKey(next), "b");
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
    assert_string_equal(AMmapItemKey(next), "c");
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
                   AMmapRange(doc, AM_ROOT, "a", NULL, NULL),
                   AM_VALUE_MAP_ITEMS,
                   cmocka_cb).map_items;
    /* First */
    next = AMmapItemsNext(&range, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "a");
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
    assert_string_equal(AMmapItemKey(next), "b");
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
    assert_string_equal(AMmapItemKey(next), "c");
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
    assert_string_equal(AMmapItemKey(next), "d");
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

    AMfree(AMmapPutStr(doc, AM_ROOT, "1", "a"));
    AMfree(AMmapPutStr(doc, AM_ROOT, "2", "b"));
    AMfree(AMmapPutStr(doc, AM_ROOT, "3", "c"));

    /* Forward, back, back. */
    AMmapItems range_all = AMpush(&stack,
                                  AMmapRange(doc, AM_ROOT, NULL, NULL, NULL),
                                  AM_VALUE_MAP_ITEMS,
                                  cmocka_cb).map_items;
    /* First */
    AMmapItem const* next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "1");
    AMvalue next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "a");
    AMobjId const* next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    AMmapItems range_back_all = AMmapItemsReversed(&range_all);
    range_back_all = AMmapItemsRewound(&range_back_all);
    AMmapItem const* next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "3");
    AMvalue next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "c");
    AMobjId const* next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "2");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "b");
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
    assert_string_equal(AMmapItemKey(next), "1");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "a");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "3");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "c");
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "2");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "b");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);

    /* Forward, forward, forward. */
    range_all = AMmapItemsRewound(&range_all);
    /* First */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "1");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "a");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "2");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "b");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "3");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "c");
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
    assert_string_equal(AMmapItemKey(next_back), "3");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "c");
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "2");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "b");
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* First */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "1");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "a");
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

    AMfree(AMmapPutStr(doc1, AM_ROOT, "1", "a"));
    AMfree(AMmapPutStr(doc1, AM_ROOT, "2", "b"));
    AMfree(AMmapPutStr(doc1, AM_ROOT, "3", "c"));

    /* The second actor should win all conflicts here. */
    AMdoc* const doc2 = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMactorId const* const actor_id2 = AMpush(&stack,
                                              AMactorIdInitBytes("\1", 1),
                                              AM_VALUE_ACTOR_ID,
                                              cmocka_cb).actor_id;
    AMfree(AMsetActorId(doc2, actor_id2));
    AMfree(AMmapPutStr(doc2, AM_ROOT, "1", "aa"));
    AMfree(AMmapPutStr(doc2, AM_ROOT, "2", "bb"));
    AMfree(AMmapPutStr(doc2, AM_ROOT, "3", "cc"));

    AMfree(AMmerge(doc1, doc2));

    /* Forward, back, back. */
    AMmapItems range_all = AMpush(&stack,
                                  AMmapRange(doc1, AM_ROOT, NULL, NULL, NULL),
                                  AM_VALUE_MAP_ITEMS,
                                  cmocka_cb).map_items;
    /* First */
    AMmapItem const* next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "1");
    AMvalue next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "aa");
    AMobjId const* next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    AMmapItems range_back_all = AMmapItemsReversed(&range_all);
    range_back_all = AMmapItemsRewound(&range_back_all);
    AMmapItem const* next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "3");
    AMvalue next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "cc");
    AMobjId const* next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "2");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "bb");
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
    assert_string_equal(AMmapItemKey(next), "1");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "aa");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "3");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "cc");
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "2");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "bb");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);

    /* Forward, forward, forward. */
    range_all = AMmapItemsRewound(&range_all);
    /* First */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "1");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "aa");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "2");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "bb");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "3");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "cc");
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
    assert_string_equal(AMmapItemKey(next_back), "3");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "cc");
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "2");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "bb");
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* First */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "1");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "aa");
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

    AMfree(AMmapPutStr(doc, AM_ROOT, "1", "a"));
    AMfree(AMmapPutStr(doc, AM_ROOT, "2", "b"));
    AMfree(AMmapPutStr(doc, AM_ROOT, "3", "c"));

    AMchangeHashes const heads = AMpush(&stack,
                                        AMgetHeads(doc),
                                        AM_VALUE_CHANGE_HASHES,
                                        cmocka_cb).change_hashes;

    /* Forward, back, back. */
    AMmapItems range_all = AMpush(&stack,
                                  AMmapRange(doc, AM_ROOT, NULL, NULL, &heads),
                                  AM_VALUE_MAP_ITEMS,
                                  cmocka_cb).map_items;
    /* First */
    AMmapItem const* next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "1");
    AMvalue next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "a");
    AMobjId const* next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    AMmapItems range_back_all = AMmapItemsReversed(&range_all);
    range_back_all = AMmapItemsRewound(&range_back_all);
    AMmapItem const* next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "3");
    AMvalue next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "c");
    AMobjId const* next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "2");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "b");
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
    assert_string_equal(AMmapItemKey(next), "1");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "a");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "3");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "c");
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "2");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "b");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);

    /* Forward, forward, forward. */
    range_all = AMmapItemsRewound(&range_all);
    /* First */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "1");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "a");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "2");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "b");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 0);
    /* Third */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "3");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "c");
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
    assert_string_equal(AMmapItemKey(next_back), "3");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "c");
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "2");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "b");
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 0);
    /* First */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "1");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "a");
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

    AMfree(AMmapPutStr(doc1, AM_ROOT, "1", "a"));
    AMfree(AMmapPutStr(doc1, AM_ROOT, "2", "b"));
    AMfree(AMmapPutStr(doc1, AM_ROOT, "3", "c"));

    /* The second actor should win all conflicts here. */
    AMdoc* const doc2 = AMpush(&stack, AMcreate(NULL), AM_VALUE_DOC, cmocka_cb).doc;
    AMactorId const* const actor_id2= AMpush(&stack,
                                             AMactorIdInitBytes("\1", 1),
                                             AM_VALUE_ACTOR_ID,
                                             cmocka_cb).actor_id;
    AMfree(AMsetActorId(doc2, actor_id2));
    AMfree(AMmapPutStr(doc2, AM_ROOT, "1", "aa"));
    AMfree(AMmapPutStr(doc2, AM_ROOT, "2", "bb"));
    AMfree(AMmapPutStr(doc2, AM_ROOT, "3", "cc"));

    AMfree(AMmerge(doc1, doc2));
    AMchangeHashes const heads = AMpush(&stack,
                                        AMgetHeads(doc1),
                                        AM_VALUE_CHANGE_HASHES,
                                        cmocka_cb).change_hashes;

    /* Forward, back, back. */
    AMmapItems range_all = AMpush(&stack,
                                  AMmapRange(doc1, AM_ROOT, NULL, NULL, &heads),
                                  AM_VALUE_MAP_ITEMS,
                                  cmocka_cb).map_items;
    /* First */
    AMmapItem const* next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "1");
    AMvalue next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "aa");
    AMobjId const* next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    AMmapItems range_back_all = AMmapItemsReversed(&range_all);
    range_back_all = AMmapItemsRewound(&range_back_all);
    AMmapItem const* next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "3");
    AMvalue next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "cc");
    AMobjId const* next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "2");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "bb");
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
    assert_string_equal(AMmapItemKey(next), "1");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "aa");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "3");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "cc");
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "2");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "bb");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);

    /* Forward, forward, forward. */
    range_all = AMmapItemsRewound(&range_all);
    /* First */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "1");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "aa");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 1);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Second */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "2");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "bb");
    next_obj_id = AMmapItemObjId(next);
    assert_int_equal(AMobjIdCounter(next_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_obj_id), 1);
    /* Third */
    next = AMmapItemsNext(&range_all, 1);
    assert_non_null(next);
    assert_string_equal(AMmapItemKey(next), "3");
    next_value = AMmapItemValue(next);
    assert_int_equal(next_value.tag, AM_VALUE_STR);
    assert_string_equal(next_value.str, "cc");
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
    assert_string_equal(AMmapItemKey(next_back), "3");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "cc");
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 3);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* Second */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "2");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "bb");
    next_back_obj_id = AMmapItemObjId(next_back);
    assert_int_equal(AMobjIdCounter(next_back_obj_id), 2);
    assert_int_equal(AMactorIdCmp(AMobjIdActorId(next_back_obj_id), actor_id2), 0);
    assert_int_equal(AMobjIdIndex(next_back_obj_id), 1);
    /* First */
    next_back = AMmapItemsNext(&range_back_all, 1);
    assert_non_null(next_back);
    assert_string_equal(AMmapItemKey(next_back), "1");
    next_back_value = AMmapItemValue(next_back);
    assert_int_equal(next_back_value.tag, AM_VALUE_STR);
    assert_string_equal(next_back_value.str, "aa");
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
    AMfree(AMmapPutStr(doc1, AM_ROOT, "aa", "aaa"));
    AMfree(AMmapPutStr(doc1, AM_ROOT, "bb", "bbb"));
    AMfree(AMmapPutStr(doc1, AM_ROOT, "cc", "ccc"));
    AMfree(AMmapPutStr(doc1, AM_ROOT, "dd", "ddd"));
    AMfree(AMcommit(doc1, NULL, NULL));

    AMchangeHashes const v1 = AMpush(&stack,
                                     AMgetHeads(doc1),
                                     AM_VALUE_CHANGE_HASHES,
                                     cmocka_cb).change_hashes;
    AMdoc* const doc2 = AMpush(&stack, AMfork(doc1, NULL), AM_VALUE_DOC, cmocka_cb).doc;

    AMfree(AMmapPutStr(doc1, AM_ROOT, "cc", "ccc V2"));
    AMfree(AMcommit(doc1, NULL, NULL));

    AMfree(AMmapPutStr(doc2, AM_ROOT, "cc", "ccc V3"));
    AMfree(AMcommit(doc2, NULL, NULL));

    AMfree(AMmerge(doc1, doc2));

    AMmapItems range = AMpush(&stack,
                              AMmapRange(doc1, AM_ROOT, "b", "d", NULL),
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
                   AMmapRange(doc1, AM_ROOT, "b", "d", &v1),
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
                   AMmapRange(doc1, AM_ROOT, NULL, NULL, NULL),
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
                   AMmapRange(doc1, AM_ROOT, NULL, NULL, &v1),
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
        cmocka_unit_test_setup_teardown(test_range_iter_map, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_map_range_back_and_forth_single, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_map_range_back_and_forth_double, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_map_range_at_back_and_forth_single, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_map_range_at_back_and_forth_double, setup_stack, teardown_stack),
        cmocka_unit_test_setup_teardown(test_get_range_values, setup_stack, teardown_stack),
    };

    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
