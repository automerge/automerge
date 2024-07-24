#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

/* third-party */
#include <cmocka.h>

/* local */
#include <automerge-c/automerge.h>
#include <automerge-c/utils/stack_callback_data.h>
#include "../base_state.h"
#include "../cmocka_utils.h"

typedef struct {
    BaseState* base_state;
    AMdoc* n1;
    AMdoc* n2;
    AMsyncState* s1;
    AMsyncState* s2;
} TestState;

static int setup(void** state) {
    TestState* test_state = test_calloc(1, sizeof(TestState));
    setup_base((void**)&test_state->base_state);
    AMstack** stack_ptr = &test_state->base_state->stack;
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("01234567")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    assert_true(
        AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &test_state->n1));
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("89abcdef")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    assert_true(
        AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &test_state->n2));
    assert_true(AMitemToSyncState(
        AMstackItem(stack_ptr, AMsyncStateInit(), cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_STATE)), &test_state->s1));
    assert_true(AMitemToSyncState(
        AMstackItem(stack_ptr, AMsyncStateInit(), cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_STATE)), &test_state->s2));
    *state = test_state;
    return 0;
}

static int teardown(void** state) {
    TestState* test_state = *state;
    teardown_base((void**)&test_state->base_state);
    test_free(test_state);
    return 0;
}

static void sync(AMdoc* a, AMdoc* b, AMsyncState* a_sync_state, AMsyncState* b_sync_state) {
    static size_t const MAX_ITER = 10;

    AMsyncMessage const* a2b_msg = NULL;
    AMsyncMessage const* b2a_msg = NULL;
    size_t iter = 0;
    do {
        AMresult* a2b_msg_result = AMgenerateSyncMessage(a, a_sync_state);
        AMresult* b2a_msg_result = AMgenerateSyncMessage(b, b_sync_state);
        AMitem* item = AMresultItem(a2b_msg_result);
        switch (AMitemValType(item)) {
            case AM_VAL_TYPE_SYNC_MESSAGE: {
                AMitemToSyncMessage(item, &a2b_msg);
                AMstackResult(NULL, AMreceiveSyncMessage(b, b_sync_state, a2b_msg), cmocka_cb,
                              AMexpect(AM_VAL_TYPE_VOID));
                break;
            }
            case AM_VAL_TYPE_VOID: {
                a2b_msg = NULL;
                break;
            }
        }
        item = AMresultItem(b2a_msg_result);
        switch (AMitemValType(item)) {
            case AM_VAL_TYPE_SYNC_MESSAGE: {
                AMitemToSyncMessage(item, &b2a_msg);
                AMstackResult(NULL, AMreceiveSyncMessage(a, a_sync_state, b2a_msg), cmocka_cb,
                              AMexpect(AM_VAL_TYPE_VOID));
                break;
            }
            case AM_VAL_TYPE_VOID: {
                b2a_msg = NULL;
                break;
            }
        }
        if (++iter > MAX_ITER) {
            fail_msg(
                "Did not synchronize within %d iterations. "
                "Do you have a bug causing an infinite loop?",
                MAX_ITER);
        }
    } while (a2b_msg || b2a_msg);
}

static time_t const TIME_0 = 0;

/**
 * \brief should send a sync message implying no local data
 */
static void test_should_send_a_sync_message_implying_no_local_data(void** state) {
    /* const doc = create()
       const s1 = initSyncState()                                                                                     */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    /* const m1 = doc.generateSyncMessage(s1)
       if (m1 === null) { throw new RangeError("message should not be null") }                                        */
    AMsyncMessage const* m1;
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s1),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &m1));
    /* const message: DecodedSyncMessage = decodeSyncMessage(m1)
       assert.deepStrictEqual(message.heads, [])                                                                      */
    AMitems heads = AMstackItems(stack_ptr, AMsyncMessageHeads(m1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_int_equal(AMitemsSize(&heads), 0);
    /* assert.deepStrictEqual(message.need, [])                                                                       */
    AMitems needs = AMstackItems(stack_ptr, AMsyncMessageNeeds(m1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_int_equal(AMitemsSize(&needs), 0);
    /* assert.deepStrictEqual(message.have.length, 1)                                                                 */
    AMitems haves = AMstackItems(stack_ptr, AMsyncMessageHaves(m1), cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_HAVE));
    assert_int_equal(AMitemsSize(&haves), 1);
    /* assert.deepStrictEqual(message.have[0].lastSync, [])                                                           */
    AMsyncHave const* have0;
    assert_true(AMitemToSyncHave(AMitemsNext(&haves, 1), &have0));
    AMitems last_sync =
        AMstackItems(stack_ptr, AMsyncHaveLastSync(have0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_int_equal(AMitemsSize(&last_sync), 0);
    /* assert.deepStrictEqual(message.have[0].bloom.byteLength, 0)
       assert.deepStrictEqual(message.changes, [])                                                                    */
}

/**
 * \brief should not reply if we have no data as well after the first round
 */
static void test_should_not_reply_if_we_have_no_data_as_well_after_the_first_round(void** state) {
    /* const n1 = create(), n2 = create()
       const s1 = initSyncState(), s2 = initSyncState()                                                               */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    /* let m1 = n1.generateSyncMessage(s1)
       if (m1 === null) { throw new RangeError("message should not be null") }                                        */
    AMsyncMessage const* m1;
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s1),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &m1));
    /* n2.receiveSyncMessage(s2, m1)                                                                                  */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n2, test_state->s2, m1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* let m2 = n2.generateSyncMessage(s2)
       // We should always send a message on the first round to advertise our heads
       assert.notStrictEqual(m2, null)                                                                                */
    AMsyncMessage const* m2;
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n2, test_state->s2),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &m2));
    /* n2.receiveSyncMessage(s2, m2!)                                                                                 */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n2, test_state->s2, m2), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*
       // now make a change on n1 so we generate another sync message to send
       n1.put("_root", "x", 1)                                                                                        */
    AMstackItem(NULL, AMmapPutInt(test_state->n1, AM_ROOT, AMstr("x"), 1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* m1 = n1.generateSyncMessage(s1)                                                                                */
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s1),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &m1));
    /* n2.receiveSyncMessage(s2, m2!)                                                                                 */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n2, test_state->s2, m2), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*
       m2 = n2.generateSyncMessage(s2)
       assert.deepStrictEqual(m2, null)                                                                               */
    assert_false(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n2, test_state->s2),
                                                 cmocka_cb, AMexpect(AM_VAL_TYPE_VOID)),
                                     &m2));
}

/**
 * \brief repos with equal heads do not need a reply message after the first
 *        round
 */
static void test_repos_with_equal_heads_do_not_need_a_reply_message_after_the_first_round(void** state) {
    /* const n1 = create(), n2 = create()
       const s1 = initSyncState(), s2 = initSyncState()                                                               */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    /*
       // make two nodes with the same changes
       const list = n1.putObject("_root", "n", [])                                                                    */
    AMobjId const* const list =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(test_state->n1, AM_ROOT, AMstr("n"), AM_OBJ_TYPE_LIST),
                                cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* n1.commit("", 0)                                                                                               */
    AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* for (let i = 0; i < 10; i++) {                                                                                 */
    for (size_t i = 0; i != 10; ++i) {
        /* n1.insert(list, i, i)                                                                                      */
        AMstackItem(NULL, AMlistPutUint(test_state->n1, list, i, true, i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /* n2.applyChanges(n1.getChanges([]))                                                                             */
    AMitems const items =
        AMstackItems(stack_ptr, AMgetChanges(test_state->n1, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE));
    AMstackItem(NULL, AMapplyChanges(test_state->n2, &items), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())                                                     */
    assert_true(AMequal(test_state->n1, test_state->n2));
    /*
       // generate a naive sync message
       let m1 = n1.generateSyncMessage(s1)
       if (m1 === null) { throw new RangeError("message should not be null") }                                        */
    AMsyncMessage const* m1;
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s1),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &m1));
    /* assert.deepStrictEqual(s1.lastSentHeads, n1.getHeads())                                                        */
    AMitems const last_sent_heads =
        AMstackItems(stack_ptr, AMsyncStateLastSentHeads(test_state->s1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMitems const heads =
        AMstackItems(stack_ptr, AMgetHeads(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_true(AMitemsEqual(&last_sent_heads, &heads));
    /*
       // process the first response (which is always generated so we know the other ends heads)
       n2.receiveSyncMessage(s2, m1)                                                                                  */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n2, test_state->s2, m1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const m2 = n2.generateSyncMessage(s2)                                                                          */
    AMsyncMessage const* m2;
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n2, test_state->s2),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &m2));
    /* n1.receiveSyncMessage(s1, m2!)                                                                                 */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n1, test_state->s1, m2), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*
       // heads are equal so this message should be null
       m1 = n1.generateSyncMessage(s2)
       assert.strictEqual(m1, null)                                                                                   */
    assert_false(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s2),
                                                 cmocka_cb, AMexpect(AM_VAL_TYPE_VOID)),
                                     &m1));
}

/**
 * \brief n1 should offer all changes to n2 when starting from nothing
 */
static void test_n1_should_offer_all_changes_to_n2_when_starting_from_nothing(void** state) {
    /* const n1 = create(), n2 = create()                                                                             */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    /*
       // make changes for n1 that n2 should request
       const list = n1.putObject("_root", "n", [])                                                                    */
    AMobjId const* const list =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(test_state->n1, AM_ROOT, AMstr("n"), AM_OBJ_TYPE_LIST),
                                cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* n1.commit("", 0)                                                                                               */
    AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* for (let i = 0; i < 10; i++) {                                                                                 */
    for (size_t i = 0; i != 10; ++i) {
        /* n1.insert(list, i, i)                                                                                      */
        AMstackItem(NULL, AMlistPutUint(test_state->n1, list, i, true, i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       assert.notDeepStrictEqual(n1.materialize(), n2.materialize())                                                  */
    assert_false(AMequal(test_state->n1, test_state->n2));
    /* sync(n1, n2)                                                                                                   */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())                                                     */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should sync peers where one has commits the other does not
 */
static void test_should_sync_peers_where_one_has_commits_the_other_does_not(void** state) {
    /* const n1 = create(), n2 = create()                                                                             */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    /*
       // make changes for n1 that n2 should request
       const list = n1.putObject("_root", "n", [])                                                                    */
    AMobjId const* const list =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(test_state->n1, AM_ROOT, AMstr("n"), AM_OBJ_TYPE_LIST),
                                cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* n1.commit("", 0)                                                                                               */
    AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* for (let i = 0; i < 10; i++) {                                                                                 */
    for (size_t i = 0; i != 10; ++i) {
        /* n1.insert(list, i, i)                                                                                      */
        AMstackItem(NULL, AMlistPutUint(test_state->n1, list, i, true, i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       assert.notDeepStrictEqual(n1.materialize(), n2.materialize())                                                  */
    assert_false(AMequal(test_state->n1, test_state->n2));
    /* sync(n1, n2)                                                                                                   */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())                                                     */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should work with prior sync state
 */
static void test_should_work_with_prior_sync_state(void** state) {
    /* // create & synchronize two nodes
       const n1 = create(), n2 = create()
       const s1 = initSyncState(), s2 = initSyncState()                                                               */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    /*
       for (let i = 0; i < 5; i++) {                                                                                  */
    for (size_t i = 0; i != 5; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       sync(n1, n2, s1, s2)
                                                                                                                      */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*
       // modify the first node further
       for (let i = 5; i < 10; i++) {                                                                                 */
    for (size_t i = 5; i != 10; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       assert.notDeepStrictEqual(n1.materialize(), n2.materialize())                                                  */
    assert_false(AMequal(test_state->n1, test_state->n2));
    /* sync(n1, n2, s1, s2)                                                                                           */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())                                                     */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should not generate messages once synced
 */
static void test_should_not_generate_messages_once_synced(void** state) {
    /* // create & synchronize two nodes
       const n1 = create({ actor: 'abc123'}), n2 = create({ actor: 'def456'})
       const s1 = initSyncState(), s2 = initSyncState()                                                               */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("abc123")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMstackItem(NULL, AMsetActorId(test_state->n1, actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("def456")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMstackItem(NULL, AMsetActorId(test_state->n2, actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*
       let message                                                                                                    */
    AMsyncMessage const* message = NULL;
    /* for (let i = 0; i < 5; i++) {                                                                                  */
    for (size_t i = 0; i != 5; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /* for (let i = 0; i < 5; i++) {                                                                                  */
    for (size_t i = 0; i != 5; ++i) {
        /* n2.put("_root", "y", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n2, AM_ROOT, AMstr("y"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n2.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n2, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       // n1 reports what it has
       message = n1.generateSyncMessage(s1)
       if (message === null) { throw new RangeError("message should not be null") }                                   */
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s1),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &message));
    /*
       // n2 receives that message and sends changes along with what it has
       n2.receiveSyncMessage(s2, message)                                                                             */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n2, test_state->s2, message), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* message = n2.generateSyncMessage(s2)
       if (message === null) { throw new RangeError("message should not be null") }                                   */
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n2, test_state->s2),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &message));
    /* assert(decodeSyncMessage(message).changes.length > 0)
       //assert.deepStrictEqual(patch, null) // no changes arrived

       // n1 receives the changes and replies with the changes it now knows that n2 needs
       n1.receiveSyncMessage(s1, message)                                                                             */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n1, test_state->s1, message), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* message = n1.generateSyncMessage(s1)
       if (message == null) { throw new RangeError("message should not be null") }                                    */
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s1),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &message));
    /* assert(decodeSyncMessage(message).changes.length > 0)

       // n2 applies the changes and sends confirmation ending the exchange
       n2.receiveSyncMessage(s2, message)                                                                             */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n2, test_state->s2, message), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* message = n2.generateSyncMessage(s2)
       if (message === null) { throw new RangeError("message should not be null") }                                   */
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n2, test_state->s2),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &message));
    /*
       // n1 receives the message and has nothing more to say
       n1.receiveSyncMessage(s1, message)                                                                             */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n1, test_state->s1, message), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* message = n1.generateSyncMessage(s1)
       assert.deepStrictEqual(message, null)
       //assert.deepStrictEqual(patch, null) // no changes arrived                                                    */
    AMstackItem(NULL, AMgenerateSyncMessage(test_state->n1, test_state->s1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*
       // n2 also has nothing left to say
       message = n2.generateSyncMessage(s2)
       assert.deepStrictEqual(message, null)                                                                          */
    assert_false(AMitemToSyncMessage(
        AMstackItem(NULL, AMgenerateSyncMessage(test_state->n2, test_state->s2), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID)),
        &message));
}

/**
 * \brief should allow simultaneous messages during synchronization
 */
static void test_should_allow_simultaneous_messages_during_synchronization(void** state) {
    /* // create & synchronize two nodes
       const n1 = create({ actor: 'abc123'}), n2 = create({ actor: 'def456'})
       const s1 = initSyncState(), s2 = initSyncState()                                                               */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("abc123")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMstackItem(NULL, AMsetActorId(test_state->n1, actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("def456")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMstackItem(NULL, AMsetActorId(test_state->n2, actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*
       for (let i = 0; i < 5; i++) {                                                                                  */
    for (size_t i = 0; i != 5; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /* for (let i = 0; i < 5; i++) {                                                                                  */
    for (size_t i = 0; i != 5; ++i) {
        /* n2.put("_root", "y", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n2, AM_ROOT, AMstr("y"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n2.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n2, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       const head1 = n1.getHeads()[0], head2 = n2.getHeads()[0]                                                       */
    AMitems heads1 = AMstackItems(stack_ptr, AMgetHeads(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMbyteSpan head1;
    assert_true(AMitemToChangeHash(AMitemsNext(&heads1, 1), &head1));
    AMitems heads2 = AMstackItems(stack_ptr, AMgetHeads(test_state->n2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMbyteSpan head2;
    assert_true(AMitemToChangeHash(AMitemsNext(&heads2, 1), &head2));
    /*
       // both sides report what they have but have no shared peer state
       let msg1to2, msg2to1                                                                                           */
    AMsyncMessage const* msg1to2;
    AMsyncMessage const* msg2to1;
    /* msg1to2 = n1.generateSyncMessage(s1)
       msg2to1 = n2.generateSyncMessage(s2)
       if (msg1to2 === null) { throw new RangeError("message should not be null") }
       if (msg2to1 === null) { throw new RangeError("message should not be null") }                                   */
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s1),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &msg1to2));
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n2, test_state->s2),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &msg2to1));
    /* assert.deepStrictEqual(decodeSyncMessage(msg1to2).changes.length, 0)
       assert.deepStrictEqual(decodeSyncMessage(msg1to2).have[0].lastSync.length, 0)                                  */
    AMitems msg1to2_haves =
        AMstackItems(stack_ptr, AMsyncMessageHaves(msg1to2), cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_HAVE));
    AMsyncHave const* msg1to2_have;
    assert_true(AMitemToSyncHave(AMitemsNext(&msg1to2_haves, 1), &msg1to2_have));
    AMitems msg1to2_last_sync =
        AMstackItems(stack_ptr, AMsyncHaveLastSync(msg1to2_have), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_int_equal(AMitemsSize(&msg1to2_last_sync), 0);
    /* assert.deepStrictEqual(decodeSyncMessage(msg2to1).changes.length, 0)
       assert.deepStrictEqual(decodeSyncMessage(msg2to1).have[0].lastSync.length, 0)                                  */
    AMitems msg2to1_haves =
        AMstackItems(stack_ptr, AMsyncMessageHaves(msg2to1), cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_HAVE));
    AMsyncHave const* msg2to1_have;
    assert_true(AMitemToSyncHave(AMitemsNext(&msg2to1_haves, 1), &msg2to1_have));
    AMitems msg2to1_last_sync =
        AMstackItems(stack_ptr, AMsyncHaveLastSync(msg2to1_have), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_int_equal(AMitemsSize(&msg2to1_last_sync), 0);
    /*
       // n1 and n2 receive that message and update sync state but make no patch
       n1.receiveSyncMessage(s1, msg2to1)                                                                             */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n1, test_state->s1, msg2to1), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* n2.receiveSyncMessage(s2, msg1to2)                                                                             */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n2, test_state->s2, msg1to2), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /*
       // now both reply with their local changes the other lacks
       // (standard warning that 1% of the time this will result in a "need" message)
       msg1to2 = n1.generateSyncMessage(s1)
       if (msg1to2 === null) { throw new RangeError("message should not be null") }                                   */
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s1),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &msg1to2));
    /* assert(decodeSyncMessage(msg1to2).changes.length > 0)
       msg2to1 = n2.generateSyncMessage(s2)
       if (msg2to1 === null) { throw new RangeError("message should not be null") }                                   */
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n2, test_state->s2),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &msg2to1));
    /* assert(decodeSyncMessage(msg2to1).changes.length > 0)

       // both should now apply the changes and update the frontend
       n1.receiveSyncMessage(s1, msg2to1)                                                                             */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n1, test_state->s1, msg2to1), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepStrictEqual(n1.getMissingDeps(), [])                                                                */
    AMitems missing_deps =
        AMstackItems(stack_ptr, AMgetMissingDeps(test_state->n1, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_int_equal(AMitemsSize(&missing_deps), 0);
    /* //assert.notDeepStrictEqual(patch1, null)
       assert.deepStrictEqual(n1.materialize(), { x: 4, y: 4 })                                                       */
    uint64_t uint;
    assert_true(AMitemToUint(AMstackItem(stack_ptr, AMmapGet(test_state->n1, AM_ROOT, AMstr("x"), NULL), cmocka_cb,
                                         AMexpect(AM_VAL_TYPE_UINT)),
                             &uint));
    assert_int_equal(uint, 4);
    assert_true(AMitemToUint(AMstackItem(stack_ptr, AMmapGet(test_state->n1, AM_ROOT, AMstr("y"), NULL), cmocka_cb,
                                         AMexpect(AM_VAL_TYPE_UINT)),
                             &uint));
    assert_int_equal(uint, 4);
    /*
       n2.receiveSyncMessage(s2, msg1to2)                                                                             */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n2, test_state->s2, msg1to2), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepStrictEqual(n2.getMissingDeps(), [])                                                                */
    missing_deps =
        AMstackItems(stack_ptr, AMgetMissingDeps(test_state->n2, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_int_equal(AMitemsSize(&missing_deps), 0);
    /* //assert.notDeepStrictEqual(patch2, null)
       assert.deepStrictEqual(n2.materialize(), { x: 4, y: 4 })                                                       */
    assert_true(AMitemToUint(AMstackItem(stack_ptr, AMmapGet(test_state->n2, AM_ROOT, AMstr("x"), NULL), cmocka_cb,
                                         AMexpect(AM_VAL_TYPE_UINT)),
                             &uint));
    assert_int_equal(uint, 4);
    assert_true(AMitemToUint(AMstackItem(stack_ptr, AMmapGet(test_state->n2, AM_ROOT, AMstr("y"), NULL), cmocka_cb,
                                         AMexpect(AM_VAL_TYPE_UINT)),
                             &uint));
    assert_int_equal(uint, 4);
    /*
       // The response acknowledges the changes received and sends no further changes
       msg1to2 = n1.generateSyncMessage(s1)
       if (msg1to2 === null) { throw new RangeError("message should not be null") }                                   */
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s1),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &msg1to2));
    /* assert.deepStrictEqual(decodeSyncMessage(msg1to2).changes.length, 0)
       msg2to1 = n2.generateSyncMessage(s2)
       if (msg2to1 === null) { throw new RangeError("message should not be null") }                                   */
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n2, test_state->s2),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &msg2to1));
    /* assert.deepStrictEqual(decodeSyncMessage(msg2to1).changes.length, 0)

       // After receiving acknowledgements, their shared heads should be equal
       n1.receiveSyncMessage(s1, msg2to1)                                                                             */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n1, test_state->s1, msg2to1), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* n2.receiveSyncMessage(s2, msg1to2)                                                                             */
    AMstackItem(NULL, AMreceiveSyncMessage(test_state->n2, test_state->s2, msg1to2), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* assert.deepStrictEqual(s1.sharedHeads, [head1, head2].sort())                                                  */
    AMitems s1_shared_heads =
        AMstackItems(stack_ptr, AMsyncStateSharedHeads(test_state->s1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMbyteSpan s1_shared_change_hash;
    assert_true(AMitemToChangeHash(AMitemsNext(&s1_shared_heads, 1), &s1_shared_change_hash));
    assert_memory_equal(s1_shared_change_hash.src, head1.src, head1.count);
    assert_true(AMitemToChangeHash(AMitemsNext(&s1_shared_heads, 1), &s1_shared_change_hash));
    assert_memory_equal(s1_shared_change_hash.src, head2.src, head2.count);
    /* assert.deepStrictEqual(s2.sharedHeads, [head1, head2].sort())                                                  */
    AMitems s2_shared_heads =
        AMstackItems(stack_ptr, AMsyncStateSharedHeads(test_state->s2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMbyteSpan s2_shared_change_hash;
    assert_true(AMitemToChangeHash(AMitemsNext(&s2_shared_heads, 1), &s2_shared_change_hash));
    assert_memory_equal(s2_shared_change_hash.src, head1.src, head1.count);
    assert_true(AMitemToChangeHash(AMitemsNext(&s2_shared_heads, 1), &s2_shared_change_hash));
    assert_memory_equal(s2_shared_change_hash.src, head2.src, head2.count);
    /* //assert.deepStrictEqual(patch1, null)
       //assert.deepStrictEqual(patch2, null)

       // We're in sync, no more messages required
       msg1to2 = n1.generateSyncMessage(s1)
       msg2to1 = n2.generateSyncMessage(s2)
       assert.deepStrictEqual(msg1to2, null)
       assert.deepStrictEqual(msg2to1, null)                                                                          */
    assert_false(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s1),
                                                 cmocka_cb, AMexpect(AM_VAL_TYPE_VOID)),
                                     &msg1to2));
    assert_false(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n2, test_state->s2),
                                                 cmocka_cb, AMexpect(AM_VAL_TYPE_VOID)),
                                     &msg2to1));
    /*
       // If we make one more change and start another sync then its lastSync should be updated
       n1.put("_root", "x", 5)                                                                                        */
    AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), 5), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* msg1to2 = n1.generateSyncMessage(s1)
       if (msg1to2 === null) { throw new RangeError("message should not be null") }                                   */
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s1),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &msg1to2));
    /* assert.deepStrictEqual(decodeSyncMessage(msg1to2).have[0].lastSync, [head1, head2].sort())                     */
    msg1to2_haves = AMstackItems(stack_ptr, AMsyncMessageHaves(msg1to2), cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_HAVE));
    assert_true(AMitemToSyncHave(AMitemsNext(&msg1to2_haves, 1), &msg1to2_have));
    msg1to2_last_sync =
        AMstackItems(stack_ptr, AMsyncHaveLastSync(msg1to2_have), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMbyteSpan msg1to2_last_sync_next;
    assert_true(AMitemToChangeHash(AMitemsNext(&msg1to2_last_sync, 1), &msg1to2_last_sync_next));
    assert_int_equal(msg1to2_last_sync_next.count, head1.count);
    assert_memory_equal(msg1to2_last_sync_next.src, head1.src, head1.count);
    assert_true(AMitemToChangeHash(AMitemsNext(&msg1to2_last_sync, 1), &msg1to2_last_sync_next));
    assert_int_equal(msg1to2_last_sync_next.count, head2.count);
    assert_memory_equal(msg1to2_last_sync_next.src, head2.src, head2.count);
}

/**
 * \brief should assume sent changes were received until we hear otherwise
 */
static void test_should_assume_sent_changes_were_received_until_we_hear_otherwise(void** state) {
    /* const n1 = create({ actor: '01234567'}), n2 = create({ actor: '89abcdef'})
       const s1 = initSyncState(), s2 = initSyncState()                                                               */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    /* let message = null

       const items = n1.putObject("_root", "items", [])                                                               */
    AMobjId const* const items =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(test_state->n1, AM_ROOT, AMstr("items"), AM_OBJ_TYPE_LIST),
                                cmocka_cb, AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    /* n1.commit("", 0)                                                                                               */
    AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /*
       sync(n1, n2, s1, s2)                                                                                           */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*
       n1.push(items, "x")                                                                                            */
    AMstackItem(NULL, AMlistPutStr(test_state->n1, items, SIZE_MAX, true, AMstr("x")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* n1.commit("", 0)                                                                                               */
    AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* message = n1.generateSyncMessage(s1)
      if (message === null) { throw new RangeError("message should not be null") }                                    */
    AMsyncMessage const* message;
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s1),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &message));
    /* assert.deepStrictEqual(decodeSyncMessage(message).changes.length, 1)

       n1.push(items, "y")                                                                                            */
    AMstackItem(NULL, AMlistPutStr(test_state->n1, items, SIZE_MAX, true, AMstr("y")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* n1.commit("", 0)                                                                                               */
    AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* message = n1.generateSyncMessage(s1)
       if (message === null) { throw new RangeError("message should not be null") }                                   */
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s1),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &message));
    /* assert.deepStrictEqual(decodeSyncMessage(message).changes.length, 1)

       n1.push(items, "z")                                                                                            */
    AMstackItem(NULL, AMlistPutStr(test_state->n1, items, SIZE_MAX, true, AMstr("z")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    /* n1.commit("", 0)                                                                                               */
    AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /*
       message = n1.generateSyncMessage(s1)
       if (message === null) { throw new RangeError("message should not be null") }                                   */
    assert_true(AMitemToSyncMessage(AMstackItem(stack_ptr, AMgenerateSyncMessage(test_state->n1, test_state->s1),
                                                cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_MESSAGE)),
                                    &message));
    /* assert.deepStrictEqual(decodeSyncMessage(message).changes.length, 1)                                           */
}

/**
 * \brief should work regardless of who initiates the exchange
 */
static void test_should_work_regardless_of_who_initiates_the_exchange(void** state) {
    /* // create & synchronize two nodes
       const n1 = create(), n2 = create()
       const s1 = initSyncState(), s2 = initSyncState()                                                               */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    /*
       for (let i = 0; i < 5; i++) {                                                                                  */
    for (size_t i = 0; i != 5; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       sync(n1, n2, s1, s2)                                                                                           */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*
       // modify the first node further
       for (let i = 5; i < 10; i++) {                                                                                 */
    for (size_t i = 5; i != 10; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       assert.notDeepStrictEqual(n1.materialize(), n2.materialize())                                                  */
    assert_false(AMequal(test_state->n1, test_state->n2));
    /* sync(n1, n2, s1, s2)                                                                                           */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())                                                     */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should work without prior sync state
 */
static void test_should_work_without_prior_sync_state(void** state) {
    /* // Scenario:                                                            ,-- c10 <-- c11 <-- c12 <-- c13 <-- c14
       // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
       //                                                                      `-- c15 <-- c16 <-- c17
       // lastSync is undefined.

       // create two peers both with divergent commits
       const n1 = create({ actor: '01234567'}), n2 = create({ actor: '89abcdef'})
       //const s1 = initSyncState(), s2 = initSyncState()                                                             */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    /*
       for (let i = 0; i < 10; i++) {                                                                                 */
    for (size_t i = 0; i != 10; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       sync(n1, n2)                                                                                                   */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*
       for (let i = 10; i < 15; i++) {                                                                                */
    for (size_t i = 10; i != 15; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       for (let i = 15; i < 18; i++) {                                                                                */
    for (size_t i = 15; i != 18; ++i) {
        /* n2.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n2, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n2.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n2, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       assert.notDeepStrictEqual(n1.materialize(), n2.materialize())                                                  */
    assert_false(AMequal(test_state->n1, test_state->n2));
    /* sync(n1, n2)                                                                                                   */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /* assert.deepStrictEqual(n1.getHeads(), n2.getHeads())                                                           */
    AMitems heads1 = AMstackItems(stack_ptr, AMgetHeads(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMitems heads2 = AMstackItems(stack_ptr, AMgetHeads(test_state->n2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_true(AMitemsEqual(&heads1, &heads2));
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())                                                     */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should work with prior sync state
 */
static void test_should_work_with_prior_sync_state_2(void** state) {
    /* // Scenario:                                                            ,-- c10 <-- c11 <-- c12 <-- c13 <-- c14
       // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
       //                                                                      `-- c15 <-- c16 <-- c17
       // lastSync is c9.

       // create two peers both with divergent commits
       const n1 = create({ actor: '01234567'}), n2 = create({ actor: '89abcdef'})
       let s1 = initSyncState(), s2 = initSyncState()                                                                 */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    /*
       for (let i = 0; i < 10; i++) {                                                                                 */
    for (size_t i = 0; i != 10; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       sync(n1, n2, s1, s2)                                                                                           */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*
       for (let i = 10; i < 15; i++) {                                                                                */
    for (size_t i = 10; i != 15; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /* for (let i = 15; i < 18; i++) {                                                                                */
    for (size_t i = 15; i != 18; ++i) {
        /* n2.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n2, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n2.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n2, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       s1 = decodeSyncState(encodeSyncState(s1))                                                                      */
    AMbyteSpan encoded;
    assert_true(AMitemToBytes(
        AMstackItem(stack_ptr, AMsyncStateEncode(test_state->s1), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &encoded));
    AMsyncState* s1;
    assert_true(AMitemToSyncState(AMstackItem(stack_ptr, AMsyncStateDecode(encoded.src, encoded.count), cmocka_cb,
                                              AMexpect(AM_VAL_TYPE_SYNC_STATE)),
                                  &s1));
    /* s2 = decodeSyncState(encodeSyncState(s2))                                                                      */
    assert_true(AMitemToBytes(
        AMstackItem(stack_ptr, AMsyncStateEncode(test_state->s2), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &encoded));
    AMsyncState* s2;
    assert_true(AMitemToSyncState(AMstackItem(stack_ptr, AMsyncStateDecode(encoded.src, encoded.count), cmocka_cb,
                                              AMexpect(AM_VAL_TYPE_SYNC_STATE)),
                                  &s2));
    /*
       assert.notDeepStrictEqual(n1.materialize(), n2.materialize())                                                  */
    assert_false(AMequal(test_state->n1, test_state->n2));
    /* sync(n1, n2, s1, s2)                                                                                           */
    sync(test_state->n1, test_state->n2, s1, s2);
    /* assert.deepStrictEqual(n1.getHeads(), n2.getHeads())                                                           */
    AMitems heads1 = AMstackItems(stack_ptr, AMgetHeads(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMitems heads2 = AMstackItems(stack_ptr, AMgetHeads(test_state->n2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_true(AMitemsEqual(&heads1, &heads2));
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())                                                     */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should ensure non-empty state after sync
 */
static void test_should_ensure_non_empty_state_after_sync(void** state) {
    /* const n1 = create({ actor: '01234567'}), n2 = create({ actor: '89abcdef'})
       const s1 = initSyncState(), s2 = initSyncState()                                                               */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    /*
       for (let i = 0; i < 3; i++) {                                                                                  */
    for (size_t i = 0; i != 3; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       sync(n1, n2, s1, s2)                                                                                           */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*
       assert.deepStrictEqual(s1.sharedHeads, n1.getHeads())                                                          */
    AMitems heads1 = AMstackItems(stack_ptr, AMgetHeads(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMitems shared_heads1 =
        AMstackItems(stack_ptr, AMsyncStateSharedHeads(test_state->s1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_true(AMitemsEqual(&shared_heads1, &heads1));
    /* assert.deepStrictEqual(s2.sharedHeads, n1.getHeads())                                                          */
    AMitems shared_heads2 =
        AMstackItems(stack_ptr, AMsyncStateSharedHeads(test_state->s2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_true(AMitemsEqual(&shared_heads2, &heads1));
}

/**
 * \brief should re-sync after one node crashed with data loss
 */
static void test_should_resync_after_one_node_crashed_with_data_loss(void** state) {
    /* // Scenario:     (r)                  (n2)                 (n1)
       // c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8
       // n2 has changes {c0, c1, c2}, n1's lastSync is c5, and n2's lastSync is c2.
       // we want to successfully sync (n1) with (r), even though (n1) believes it's talking to (n2)
       const n1 = create({ actor: '01234567'}), n2 = create({ actor: '89abcdef'})
       let s1 = initSyncState()
       const s2 = initSyncState()                                                                                     */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    /*
       // n1 makes three changes, which we sync to n2
       for (let i = 0; i < 3; i++) {                                                                                  */
    for (size_t i = 0; i != 3; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       sync(n1, n2, s1, s2)                                                                                           */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*
       // save a copy of n2 as "r" to simulate recovering from a crash
       let r
       let rSyncState
       ;[r, rSyncState] = [n2.clone(), s2.clone()]                                                                    */
    AMdoc* r;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMclone(test_state->n2), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &r));
    AMbyteSpan encoded_s2;
    assert_true(
        AMitemToBytes(AMstackItem(stack_ptr, AMsyncStateEncode(test_state->s2), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)),
                      &encoded_s2));
    AMsyncState* sync_state_r;
    assert_true(AMitemToSyncState(AMstackItem(stack_ptr, AMsyncStateDecode(encoded_s2.src, encoded_s2.count), cmocka_cb,
                                              AMexpect(AM_VAL_TYPE_SYNC_STATE)),
                                  &sync_state_r));
    /*
       // sync another few commits
       for (let i = 3; i < 6; i++) {                                                                                  */
    for (size_t i = 3; i != 6; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       sync(n1, n2, s1, s2)                                                                                           */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*
       // everyone should be on the same page here
       assert.deepStrictEqual(n1.getHeads(), n2.getHeads())                                                           */
    AMitems heads1 = AMstackItems(stack_ptr, AMgetHeads(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMitems heads2 = AMstackItems(stack_ptr, AMgetHeads(test_state->n2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_true(AMitemsEqual(&heads1, &heads2));
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())                                                     */
    assert_true(AMequal(test_state->n1, test_state->n2));
    /*
       // now make a few more changes and then attempt to sync the fully up-to-date n1 with with the confused r
       for (let i = 6; i < 9; i++) {                                                                                  */
    for (size_t i = 6; i != 9; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       s1 = decodeSyncState(encodeSyncState(s1))                                                                      */
    AMbyteSpan encoded_s1;
    assert_true(
        AMitemToBytes(AMstackItem(stack_ptr, AMsyncStateEncode(test_state->s1), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)),
                      &encoded_s1));
    AMsyncState* s1;
    assert_true(AMitemToSyncState(AMstackItem(stack_ptr, AMsyncStateDecode(encoded_s1.src, encoded_s1.count), cmocka_cb,
                                              AMexpect(AM_VAL_TYPE_SYNC_STATE)),
                                  &s1));
    /* rSyncState = decodeSyncState(encodeSyncState(rSyncState))                                                      */
    AMbyteSpan encoded_r;
    assert_true(AMitemToBytes(
        AMstackItem(stack_ptr, AMsyncStateEncode(sync_state_r), cmocka_cb, AMexpect(AM_VAL_TYPE_BYTES)), &encoded_r));
    assert_true(AMitemToSyncState(AMstackItem(stack_ptr, AMsyncStateDecode(encoded_r.src, encoded_r.count), cmocka_cb,
                                              AMexpect(AM_VAL_TYPE_SYNC_STATE)),
                                  &sync_state_r));
    /*
       assert.notDeepStrictEqual(n1.getHeads(), r.getHeads())                                                         */
    heads1 = AMstackItems(stack_ptr, AMgetHeads(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMitems heads_r = AMstackItems(stack_ptr, AMgetHeads(r), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_false(AMitemsEqual(&heads1, &heads_r));
    /* assert.notDeepStrictEqual(n1.materialize(), r.materialize())                                                   */
    assert_false(AMequal(test_state->n1, r));
    /* assert.deepStrictEqual(n1.materialize(), { x: 8 })                                                             */
    uint64_t uint;
    assert_true(AMitemToUint(AMstackItem(stack_ptr, AMmapGet(test_state->n1, AM_ROOT, AMstr("x"), NULL), cmocka_cb,
                                         AMexpect(AM_VAL_TYPE_UINT)),
                             &uint));
    assert_int_equal(uint, 8);
    /* assert.deepStrictEqual(r.materialize(), { x: 2 })                                                              */
    assert_true(AMitemToUint(
        AMstackItem(stack_ptr, AMmapGet(r, AM_ROOT, AMstr("x"), NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_UINT)), &uint));
    assert_int_equal(uint, 2);
    /* sync(n1, r, s1, rSyncState)                                                                                    */
    sync(test_state->n1, r, test_state->s1, sync_state_r);
    /* assert.deepStrictEqual(n1.getHeads(), r.getHeads())                                                            */
    heads1 = AMstackItems(stack_ptr, AMgetHeads(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    heads_r = AMstackItems(stack_ptr, AMgetHeads(r), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_true(AMitemsEqual(&heads1, &heads_r));
    /* assert.deepStrictEqual(n1.materialize(), r.materialize())                                                      */
    assert_true(AMequal(test_state->n1, r));
    /* r = null                                                                                                       */
    r = NULL;
}

/**
 * \brief should re-sync after one node experiences data loss without disconnecting
 */
static void test_should_resync_after_one_node_experiences_data_loss_without_disconnecting(void** state) {
    /* const n1 = create({ actor: '01234567'}), n2 = create({ actor: '89abcdef'})
       const s1 = initSyncState(), s2 = initSyncState()                                                               */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    /*
       // n1 makes three changes, which we sync to n2
       for (let i = 0; i < 3; i++) {                                                                                  */
    for (size_t i = 0; i != 3; ++i) {
        /* n1.put("_root", "x", i)                                                                                    */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n1.commit("", 0)                                                                                           */
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* }                                                                                                          */
    }
    /*
       sync(n1, n2, s1, s2)                                                                                           */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*
       assert.deepStrictEqual(n1.getHeads(), n2.getHeads())                                                           */
    AMitems heads1 = AMstackItems(stack_ptr, AMgetHeads(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMitems heads2 = AMstackItems(stack_ptr, AMgetHeads(test_state->n2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_true(AMitemsEqual(&heads1, &heads2));
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())                                                     */
    assert_true(AMequal(test_state->n1, test_state->n2));
    /*
       const n2AfterDataLoss = create({ actor: '89abcdef'})                                                           */
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("89abcdef")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMdoc* n2_after_data_loss;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)),
                            &n2_after_data_loss));
    /*
       // "n2" now has no data, but n1 still thinks it does. Note we don't do
       // decodeSyncState(encodeSyncState(s1)) in order to simulate data loss without disconnecting
       sync(n1, n2AfterDataLoss, s1, initSyncState())                                                                 */
    AMsyncState* s2_after_data_loss;
    assert_true(AMitemToSyncState(
        AMstackItem(stack_ptr, AMsyncStateInit(), cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_STATE)), &s2_after_data_loss));
    sync(test_state->n1, n2_after_data_loss, test_state->s1, s2_after_data_loss);
    /* assert.deepStrictEqual(n1.getHeads(), n2.getHeads())                                                           */
    heads1 = AMstackItems(stack_ptr, AMgetHeads(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    heads2 = AMstackItems(stack_ptr, AMgetHeads(test_state->n2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_true(AMitemsEqual(&heads1, &heads2));
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())                                                     */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should handle changes concurrent to the last sync heads
 */
static void test_should_handle_changes_concurrrent_to_the_last_sync_heads(void** state) {
    /* const n1 = create({ actor: '01234567'}), n2 = create({ actor: '89abcdef'}), n3 = create({ actor: 'fedcba98'})  */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("fedcba98")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMdoc* n3;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &n3));
    /* const s12 = initSyncState(), s21 = initSyncState(), s23 = initSyncState(), s32 = initSyncState()               */
    AMsyncState* s12 = test_state->s1;
    AMsyncState* s21 = test_state->s2;
    AMsyncState* s23;
    assert_true(AMitemToSyncState(
        AMstackItem(stack_ptr, AMsyncStateInit(), cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_STATE)), &s23));
    AMsyncState* s32;
    assert_true(AMitemToSyncState(
        AMstackItem(stack_ptr, AMsyncStateInit(), cmocka_cb, AMexpect(AM_VAL_TYPE_SYNC_STATE)), &s32));
    /*
       // Change 1 is known to all three nodes
       //n1 = Automerge.change(n1, {time: 0}, doc => doc.x = 1)
       n1.put("_root", "x", 1); n1.commit("", 0)                                                                      */
    AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), 1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /*
       sync(n1, n2, s12, s21)                                                                                         */
    sync(test_state->n1, test_state->n2, s12, s21);
    /* sync(n2, n3, s23, s32)                                                                                         */
    sync(test_state->n2, n3, s23, s32);
    /*
       // Change 2 is known to n1 and n2
       n1.put("_root", "x", 2); n1.commit("", 0)                                                                      */
    AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), 2), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /*
       sync(n1, n2, s12, s21)                                                                                         */
    sync(test_state->n1, test_state->n2, s12, s21);
    /*
       // Each of the three nodes makes one change (changes 3, 4, 5)
       n1.put("_root", "x", 3); n1.commit("", 0)                                                                      */
    AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), 3), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* n2.put("_root", "x", 4); n2.commit("", 0)                                                                      */
    AMstackItem(NULL, AMmapPutUint(test_state->n2, AM_ROOT, AMstr("x"), 4), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(test_state->n2, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* n3.put("_root", "x", 5); n3.commit("", 0)                                                                      */
    AMstackItem(NULL, AMmapPutUint(n3, AM_ROOT, AMstr("x"), 5), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(n3, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /*
       // Apply n3's latest change to n2. If running in Node, turn the Uint8Array into a Buffer, to
       // simulate transmission over a network (see https://github.com/automerge/automerge/pull/362)
       let change = n3.getLastLocalChange()
       if (change === null) throw new RangeError("no local change")                                                   */
    AMitems changes = AMstackItems(stack_ptr, AMgetLastLocalChange(n3), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE));
    /* //ts-ignore
       if (typeof Buffer === 'function') change = Buffer.from(change)
       if (change === undefined) { throw new RangeError("last local change failed") }
       n2.applyChanges([change])                                                                                      */
    AMstackItem(NULL, AMapplyChanges(test_state->n2, &changes), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /*
       // Now sync n1 and n2. n3's change is concurrent to n1 and n2's last sync heads
       sync(n1, n2, s12, s21)                                                                                         */
    sync(test_state->n1, test_state->n2, s12, s21);
    /* assert.deepStrictEqual(n1.getHeads(), n2.getHeads())                                                           */
    AMitems heads1 = AMstackItems(stack_ptr, AMgetHeads(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMitems heads2 = AMstackItems(stack_ptr, AMgetHeads(test_state->n2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_true(AMitemsEqual(&heads1, &heads2));
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())                                                     */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should handle histories with lots of branching and merging
 */
static void test_should_handle_histories_with_lots_of_branching_and_merging(void** state) {
    /* const n1 = create({ actor: '01234567'}), n2 = create({ actor: '89abcdef'}), n3 = create({ actor: 'fedcba98'})  */
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->base_state->stack;
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(AMstr("fedcba98")), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMdoc* n3;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &n3));
    /* n1.put("_root", "x", 0); n1.commit("", 0)                                                                      */
    AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), 0), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* const change1 = n1.getLastLocalChange()
       if (change1 === null) throw new RangeError("no local change")                                                  */
    AMitems change1 =
        AMstackItems(stack_ptr, AMgetLastLocalChange(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE));
    /* n2.applyChanges([change1])                                                                                     */
    AMstackItem(NULL, AMapplyChanges(test_state->n2, &change1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* const change2 = n1.getLastLocalChange()
       if (change2 === null) throw new RangeError("no local change")                                                  */
    AMitems change2 =
        AMstackItems(stack_ptr, AMgetLastLocalChange(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE));
    /* n3.applyChanges([change2])                                                                                     */
    AMstackItem(NULL, AMapplyChanges(n3, &change2), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* n3.put("_root", "x", 1); n3.commit("", 0)                                                                      */
    AMstackItem(NULL, AMmapPutUint(n3, AM_ROOT, AMstr("x"), 1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(n3, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /*
       //        - n1c1 <------ n1c2 <------ n1c3 <-- etc. <-- n1c20 <------ n1c21
       //       /          \/           \/                              \/
       //      /           /\           /\                              /\
       // c0 <---- n2c1 <------ n2c2 <------ n2c3 <-- etc. <-- n2c20 <------ n2c21
       //      \                                                          /
       //       ---------------------------------------------- n3c1 <-----
       for (let i = 1; i < 20; i++) {                                                                                 */
    for (size_t i = 1; i != 20; ++i) {
        /* n1.put("_root", "n1", i); n1.commit("", 0)                                                                 */
        AMstackItem(NULL, AMmapPutUint(test_state->n1, AM_ROOT, AMstr("n1"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* n2.put("_root", "n2", i); n2.commit("", 0)                                                                 */
        AMstackItem(NULL, AMmapPutUint(test_state->n2, AM_ROOT, AMstr("n2"), i), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        AMstackItem(NULL, AMcommit(test_state->n2, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
        /* const change1 = n1.getLastLocalChange()
           if (change1 === null) throw new RangeError("no local change")                                              */
        AMitems change1 =
            AMstackItems(stack_ptr, AMgetLastLocalChange(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE));
        /* const change2 = n2.getLastLocalChange()
           if (change2 === null) throw new RangeError("no local change")                                              */
        AMitems change2 =
            AMstackItems(stack_ptr, AMgetLastLocalChange(test_state->n2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE));
        /* n1.applyChanges([change2])                                                                                 */
        AMstackItem(NULL, AMapplyChanges(test_state->n1, &change2), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* n2.applyChanges([change1])                                                                                 */
        AMstackItem(NULL, AMapplyChanges(test_state->n2, &change1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
        /* }                                                                                                          */
    }
    /*
       const s1 = initSyncState(), s2 = initSyncState()
       sync(n1, n2, s1, s2)                                                                                           */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*
       // Having n3's last change concurrent to the last sync heads forces us into the slower code path
       const change3 = n2.getLastLocalChange()
       if (change3 === null) throw new RangeError("no local change")                                                  */
    AMitems change3 = AMstackItems(stack_ptr, AMgetLastLocalChange(n3), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE));
    /* n2.applyChanges([change3])                                                                                     */
    AMstackItem(NULL, AMapplyChanges(test_state->n2, &change3), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    /* n1.put("_root", "n1", "final"); n1.commit("", 0)                                                               */
    AMstackItem(NULL, AMmapPutStr(test_state->n1, AM_ROOT, AMstr("n1"), AMstr("final")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(test_state->n1, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /* n2.put("_root", "n2", "final"); n2.commit("", 0)                                                               */
    AMstackItem(NULL, AMmapPutStr(test_state->n2, AM_ROOT, AMstr("n2"), AMstr("final")), cmocka_cb,
                AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMcommit(test_state->n2, AMstr(""), &TIME_0), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    /*
       sync(n1, n2, s1, s2)                                                                                           */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /* assert.deepStrictEqual(n1.getHeads(), n2.getHeads())                                                           */
    AMitems heads1 = AMstackItems(stack_ptr, AMgetHeads(test_state->n1), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    AMitems heads2 = AMstackItems(stack_ptr, AMgetHeads(test_state->n2), cmocka_cb, AMexpect(AM_VAL_TYPE_CHANGE_HASH));
    assert_true(AMitemsEqual(&heads1, &heads2));
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())                                                     */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

int run_ported_wasm_sync_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_should_send_a_sync_message_implying_no_local_data, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_not_reply_if_we_have_no_data_as_well_after_the_first_round, setup,
                                        teardown),
        cmocka_unit_test_setup_teardown(test_repos_with_equal_heads_do_not_need_a_reply_message_after_the_first_round,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_n1_should_offer_all_changes_to_n2_when_starting_from_nothing, setup,
                                        teardown),
        cmocka_unit_test_setup_teardown(test_should_sync_peers_where_one_has_commits_the_other_does_not, setup,
                                        teardown),
        cmocka_unit_test_setup_teardown(test_should_work_with_prior_sync_state, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_not_generate_messages_once_synced, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_allow_simultaneous_messages_during_synchronization, setup,
                                        teardown),
        cmocka_unit_test_setup_teardown(test_should_assume_sent_changes_were_received_until_we_hear_otherwise, setup,
                                        teardown),
        cmocka_unit_test_setup_teardown(test_should_work_regardless_of_who_initiates_the_exchange, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_work_without_prior_sync_state, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_work_with_prior_sync_state_2, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_ensure_non_empty_state_after_sync, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_resync_after_one_node_crashed_with_data_loss, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_resync_after_one_node_experiences_data_loss_without_disconnecting,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_handle_changes_concurrrent_to_the_last_sync_heads, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_handle_histories_with_lots_of_branching_and_merging, setup,
                                        teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
