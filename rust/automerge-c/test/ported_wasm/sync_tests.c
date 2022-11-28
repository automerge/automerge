#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

/* third-party */
#include <cmocka.h>

/* local */
#include <automerge-c/automerge.h>
#include "../stack_utils.h"

typedef struct {
    AMresultStack* stack;
    AMdoc* n1;
    AMdoc* n2;
    AMsyncState* s1;
    AMsyncState* s2;
} TestState;

static int setup(void** state) {
    TestState* test_state = test_calloc(1, sizeof(TestState));
    test_state->n1 = AMpush(&test_state->stack,
                            AMcreate(AMpush(&test_state->stack,
                                            AMactorIdInitStr(AMstr("01234567")),
                                            AM_VALUE_ACTOR_ID,
                                            cmocka_cb).actor_id),
                            AM_VALUE_DOC,
                            cmocka_cb).doc;
    test_state->n2 = AMpush(&test_state->stack,
                            AMcreate(AMpush(&test_state->stack,
                                            AMactorIdInitStr(AMstr("89abcdef")),
                                            AM_VALUE_ACTOR_ID,
                                            cmocka_cb).actor_id),
                            AM_VALUE_DOC,
                            cmocka_cb).doc;
    test_state->s1 = AMpush(&test_state->stack,
                            AMsyncStateInit(),
                            AM_VALUE_SYNC_STATE,
                            cmocka_cb).sync_state;
    test_state->s2 = AMpush(&test_state->stack,
                            AMsyncStateInit(),
                            AM_VALUE_SYNC_STATE,
                            cmocka_cb).sync_state;
    *state = test_state;
    return 0;
}

static int teardown(void** state) {
    TestState* test_state = *state;
    AMfreeStack(&test_state->stack);
    test_free(test_state);
    return 0;
}

static void sync(AMdoc* a,
                 AMdoc* b,
                 AMsyncState* a_sync_state,
                 AMsyncState* b_sync_state) {
    static size_t const MAX_ITER = 10;

    AMsyncMessage const* a2b_msg = NULL;
    AMsyncMessage const* b2a_msg = NULL;
    size_t iter = 0;
    do {
        AMresult* a2b_msg_result = AMgenerateSyncMessage(a, a_sync_state);
        AMresult* b2a_msg_result = AMgenerateSyncMessage(b, b_sync_state);
        AMvalue value = AMresultValue(a2b_msg_result);
        switch (value.tag) {
            case AM_VALUE_SYNC_MESSAGE: {
                a2b_msg = value.sync_message;
                AMfree(AMreceiveSyncMessage(b, b_sync_state, a2b_msg));
            }
            break;
            case AM_VALUE_VOID: a2b_msg = NULL; break;
        }
        value = AMresultValue(b2a_msg_result);
        switch (value.tag) {
            case AM_VALUE_SYNC_MESSAGE: {
                b2a_msg = value.sync_message;
                AMfree(AMreceiveSyncMessage(a, a_sync_state, b2a_msg));
            }
            break;
            case AM_VALUE_VOID: b2a_msg = NULL; break;
        }
        if (++iter > MAX_ITER) {
            fail_msg("Did not synchronize within %d iterations. "
                     "Do you have a bug causing an infinite loop?", MAX_ITER);
        }
    } while(a2b_msg || b2a_msg);
}

static time_t const TIME_0 = 0;

/**
 * \brief should send a sync message implying no local data
 */
static void test_should_send_a_sync_message_implying_no_local_data(void **state) {
    /* const doc = create()
       const s1 = initSyncState()                                            */
    TestState* test_state = *state;
    /* const m1 = doc.generateSyncMessage(s1)
       if (m1 === null) { throw new RangeError("message should not be null") }
       const message: DecodedSyncMessage = decodeSyncMessage(m1)             */
    AMsyncMessage const* const m1 = AMpush(&test_state->stack,
                                           AMgenerateSyncMessage(
                                               test_state->n1,
                                               test_state->s1),
                                           AM_VALUE_SYNC_MESSAGE,
                                           cmocka_cb).sync_message;
    /* assert.deepStrictEqual(message.heads, [])                             */
    AMchangeHashes heads = AMsyncMessageHeads(m1);
    assert_int_equal(AMchangeHashesSize(&heads), 0);
    /* assert.deepStrictEqual(message.need, [])                              */
    AMchangeHashes needs = AMsyncMessageNeeds(m1);
    assert_int_equal(AMchangeHashesSize(&needs), 0);
    /* assert.deepStrictEqual(message.have.length, 1)                        */
    AMsyncHaves haves = AMsyncMessageHaves(m1);
    assert_int_equal(AMsyncHavesSize(&haves), 1);
    /* assert.deepStrictEqual(message.have[0].lastSync, [])                  */
    AMsyncHave const* have0 = AMsyncHavesNext(&haves, 1);
    AMchangeHashes last_sync = AMsyncHaveLastSync(have0);
    assert_int_equal(AMchangeHashesSize(&last_sync), 0);
    /* assert.deepStrictEqual(message.have[0].bloom.byteLength, 0)
       assert.deepStrictEqual(message.changes, [])                           */
    AMchanges changes = AMsyncMessageChanges(m1);
    assert_int_equal(AMchangesSize(&changes), 0);
}

/**
 * \brief should not reply if we have no data as well
 */
static void test_should_not_reply_if_we_have_no_data_as_well(void **state) {
    /* const n1 = create(), n2 = create()
       const s1 = initSyncState(), s2 = initSyncState()                      */
    TestState* test_state = *state;
    /* const m1 = n1.generateSyncMessage(s1)
       if (m1 === null) { throw new RangeError("message should not be null")  */
    AMsyncMessage const* const m1 = AMpush(&test_state->stack,
                                           AMgenerateSyncMessage(
                                               test_state->n1,
                                               test_state->s1),
                                           AM_VALUE_SYNC_MESSAGE,
                                           cmocka_cb).sync_message;
    /* n2.receiveSyncMessage(s2, m1)                                         */
    AMfree(AMreceiveSyncMessage(test_state->n2, test_state->s2, m1));
    /* const m2 = n2.generateSyncMessage(s2)
       assert.deepStrictEqual(m2, null)                                      */
    AMpush(&test_state->stack,
           AMgenerateSyncMessage(test_state->n2, test_state->s2),
           AM_VALUE_VOID,
           cmocka_cb);
}

/**
 * \brief repos with equal heads do not need a reply message
 */
static void test_repos_with_equal_heads_do_not_need_a_reply_message(void **state) {
    /* const n1 = create(), n2 = create()
       const s1 = initSyncState(), s2 = initSyncState()                      */
    TestState* test_state = *state;
    /*                                                                       */
    /* make two nodes with the same changes */
    /* const list = n1.putObject("_root", "n", [])                           */
    AMobjId const* const list = AMpush(&test_state->stack,
                                       AMmapPutObject(test_state->n1,
                                                      AM_ROOT,
                                                      AMstr("n"),
                                                      AM_OBJ_TYPE_LIST),
                                       AM_VALUE_OBJ_ID,
                                       cmocka_cb).obj_id;
    /* n1.commit("", 0)                                                      */
    AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* for (let i = 0; i < 10; i++) {                                        */
    for (size_t i = 0; i != 10; ++i) {
        /* n1.insert(list, i, i)                                             */
        AMfree(AMlistPutUint(test_state->n1, AM_ROOT, i, true, i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /* n2.applyChanges(n1.getChanges([]))                                    */
    AMchanges const changes = AMpush(&test_state->stack,
                                     AMgetChanges(test_state->n1, NULL),
                                     AM_VALUE_CHANGES,
                                     cmocka_cb).changes;
    AMfree(AMapplyChanges(test_state->n2, &changes));
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())            */
    assert_true(AMequal(test_state->n1, test_state->n2));
    /*                                                                       */
    /* generate a naive sync message */
    /* const m1 = n1.generateSyncMessage(s1)
       if (m1 === null) { throw new RangeError("message should not be null")  */
    AMsyncMessage const* m1 = AMpush(&test_state->stack,
                                     AMgenerateSyncMessage(test_state->n1,
                                                           test_state->s1),
                                     AM_VALUE_SYNC_MESSAGE,
                                     cmocka_cb).sync_message;
    /* assert.deepStrictEqual(s1.lastSentHeads, n1.getHeads())               */
    AMchangeHashes const last_sent_heads = AMsyncStateLastSentHeads(
        test_state->s1
    );
    AMchangeHashes const heads = AMpush(&test_state->stack,
                                        AMgetHeads(test_state->n1),
                                        AM_VALUE_CHANGE_HASHES,
                                        cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&last_sent_heads, &heads), 0);
    /*                                                                       */
    /* heads are equal so this message should be null */
    /* n2.receiveSyncMessage(s2, m1)                                         */
    AMfree(AMreceiveSyncMessage(test_state->n2, test_state->s2, m1));
    /* const m2 = n2.generateSyncMessage(s2)
       assert.strictEqual(m2, null)                                          */
    AMpush(&test_state->stack,
           AMgenerateSyncMessage(test_state->n2, test_state->s2),
           AM_VALUE_VOID,
           cmocka_cb);
}

/**
 * \brief n1 should offer all changes to n2 when starting from nothing
 */
static void test_n1_should_offer_all_changes_to_n2_when_starting_from_nothing(void **state) {
    /* const n1 = create(), n2 = create()                                    */
    TestState* test_state = *state;

    /* make changes for n1 that n2 should request */
    /* const list = n1.putObject("_root", "n", [])                           */
    AMobjId const* const list = AMpush(
        &test_state->stack,
        AMmapPutObject(test_state->n1, AM_ROOT, AMstr("n"), AM_OBJ_TYPE_LIST),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    /* n1.commit("", 0)                                                      */
    AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* for (let i = 0; i < 10; i++) {                                        */
    for (size_t i = 0; i != 10; ++i) {
        /* n1.insert(list, i, i)                                             */
        AMfree(AMlistPutUint(test_state->n1, AM_ROOT, i, true, i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* assert.notDeepStrictEqual(n1.materialize(), n2.materialize())         */
    assert_false(AMequal(test_state->n1, test_state->n2));
    /* sync(n1, n2)                                                          */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())            */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should sync peers where one has commits the other does not
 */
static void test_should_sync_peers_where_one_has_commits_the_other_does_not(void **state) {
    /* const n1 = create(), n2 = create()                                    */
    TestState* test_state = *state;

    /* make changes for n1 that n2 should request */
    /* const list = n1.putObject("_root", "n", [])                           */
    AMobjId const* const list = AMpush(
        &test_state->stack,
        AMmapPutObject(test_state->n1, AM_ROOT, AMstr("n"), AM_OBJ_TYPE_LIST),
        AM_VALUE_OBJ_ID,
        cmocka_cb).obj_id;
    /* n1.commit("", 0)                                                      */
    AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* for (let i = 0; i < 10; i++) {                                        */
    for (size_t i = 0; i != 10; ++i) {
        /* n1.insert(list, i, i)                                             */
        AMfree(AMlistPutUint(test_state->n1, AM_ROOT, i, true, i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* assert.notDeepStrictEqual(n1.materialize(), n2.materialize())         */
    assert_false(AMequal(test_state->n1, test_state->n2));
    /* sync(n1, n2)                                                          */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())            */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should work with prior sync state
 */
static void test_should_work_with_prior_sync_state(void **state) {
    /* create & synchronize two nodes */
    /* const n1 = create(), n2 = create()
       const s1 = initSyncState(), s2 = initSyncState()                      */
    TestState* test_state = *state;
    /*                                                                       */
    /* for (let i = 0; i < 5; i++) {                                         */
    for (size_t i = 0; i != 5; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*                                                                       */
    /* modify the first node further */
    /* for (let i = 5; i < 10; i++) {                                        */
    for (size_t i = 5; i != 10; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* assert.notDeepStrictEqual(n1.materialize(), n2.materialize())         */
    assert_false(AMequal(test_state->n1, test_state->n2));
    /* sync(n1, n2, s1, s2)                                                  */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())            */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should not generate messages once synced
 */
static void test_should_not_generate_messages_once_synced(void **state) {
    /* create & synchronize two nodes */
    /* const n1 = create('abc123'), n2 = create('def456')
       const s1 = initSyncState(), s2 = initSyncState()                      */
    TestState* test_state = *state;
    AMfree(AMsetActorId(test_state->n1, AMpush(&test_state->stack,
                                               AMactorIdInitStr(AMstr("abc123")),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    AMfree(AMsetActorId(test_state->n2, AMpush(&test_state->stack,
                                               AMactorIdInitStr(AMstr("def456")),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    /*                                                                       */
    /* let message, patch
       for (let i = 0; i < 5; i++) {                                         */
    for (size_t i = 0; i != 5; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /* for (let i = 0; i < 5; i++) {                                         */
    for (size_t i = 0; i != 5; ++i) {
        /* n2.put("_root", "y", i)                                           */
        AMfree(AMmapPutUint(test_state->n2, AM_ROOT, AMstr("y"), i));
        /* n2.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n2, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* n1 reports what it has */
    /* message = n1.generateSyncMessage(s1)
       if (message === null) { throw new RangeError("message should not be null")  */
    AMsyncMessage const* message = AMpush(&test_state->stack,
                                          AMgenerateSyncMessage(test_state->n1,
                                                                test_state->s1),
                                          AM_VALUE_SYNC_MESSAGE,
                                          cmocka_cb).sync_message;
    /*                                                                       */
    /* n2 receives that message and sends changes along with what it has */
    /* n2.receiveSyncMessage(s2, message)                                    */
    AMfree(AMreceiveSyncMessage(test_state->n2, test_state->s2, message));
    /* message = n2.generateSyncMessage(s2)
       if (message === null) { throw new RangeError("message should not be null")  */
    message = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->n2, test_state->s2),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    AMchanges message_changes = AMsyncMessageChanges(message);
    assert_int_equal(AMchangesSize(&message_changes), 5);
    /*                                                                       */
    /* n1 receives the changes and replies with the changes it now knows that
     * n2 needs */
    /* n1.receiveSyncMessage(s1, message)                                    */
    AMfree(AMreceiveSyncMessage(test_state->n1, test_state->s1, message));
    /* message = n2.generateSyncMessage(s2)
       if (message === null) { throw new RangeError("message should not be null")  */
    message = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->n1, test_state->s1),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    message_changes = AMsyncMessageChanges(message);
    assert_int_equal(AMchangesSize(&message_changes), 5);
    /*                                                                       */
    /* n2 applies the changes and sends confirmation ending the exchange */
    /* n2.receiveSyncMessage(s2, message)                                    */
    AMfree(AMreceiveSyncMessage(test_state->n2, test_state->s2, message));
    /* message = n2.generateSyncMessage(s2)
       if (message === null) { throw new RangeError("message should not be null")  */
    message = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->n2, test_state->s2),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    /*                                                                       */
    /* n1 receives the message and has nothing more to say */
    /* n1.receiveSyncMessage(s1, message)                                    */
    AMfree(AMreceiveSyncMessage(test_state->n1, test_state->s1, message));
    /* message = n1.generateSyncMessage(s1)
       assert.deepStrictEqual(message, null)                                 */
    AMpush(&test_state->stack,
           AMgenerateSyncMessage(test_state->n1, test_state->s1),
           AM_VALUE_VOID,
           cmocka_cb);
    /* //assert.deepStrictEqual(patch, null) // no changes arrived           */
    /*                                                                       */
    /* n2 also has nothing left to say */
    /* message = n2.generateSyncMessage(s2)
       assert.deepStrictEqual(message, null)                                 */
    AMpush(&test_state->stack,
           AMgenerateSyncMessage(test_state->n2, test_state->s2),
           AM_VALUE_VOID,
           cmocka_cb);
}

/**
 * \brief should allow simultaneous messages during synchronization
 */
static void test_should_allow_simultaneous_messages_during_synchronization(void **state) {
    /* create & synchronize two nodes */
    /* const n1 = create('abc123'), n2 = create('def456')
       const s1 = initSyncState(), s2 = initSyncState()                      */
    TestState* test_state = *state;
    AMfree(AMsetActorId(test_state->n1, AMpush(&test_state->stack,
                                               AMactorIdInitStr(AMstr("abc123")),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    AMfree(AMsetActorId(test_state->n2, AMpush(&test_state->stack,
                                               AMactorIdInitStr(AMstr("def456")),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    /*                                                                       */
    /*  for (let i = 0; i < 5; i++) {                                        */
    for (size_t i = 0; i != 5; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /* for (let i = 0; i < 5; i++) {                                         */
    for (size_t i = 0; i != 5; ++i) {
        /* n2.put("_root", "y", i)                                           */
        AMfree(AMmapPutUint(test_state->n2, AM_ROOT, AMstr("y"), i));
        /* n2.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n2, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /* const head1 = n1.getHeads()[0], head2 = n2.getHeads()[0]              */
    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMbyteSpan const head1 = AMchangeHashesNext(&heads1, 1);
    AMchangeHashes heads2 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n2),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMbyteSpan const head2 = AMchangeHashesNext(&heads2, 1);
    /*                                                                       */
    /* both sides report what they have but have no shared peer state */
    /* let msg1to2, msg2to1
       msg1to2 = n1.generateSyncMessage(s1)
       if (msg1to2 === null) { throw new RangeError("message should not be null")  */
    AMsyncMessage const* msg1to2 = AMpush(&test_state->stack,
                                          AMgenerateSyncMessage(test_state->n1,
                                                                test_state->s1),
                                          AM_VALUE_SYNC_MESSAGE,
                                          cmocka_cb).sync_message;
    /* msg2to1 = n2.generateSyncMessage(s2)
       if (msg2to1 === null) { throw new RangeError("message should not be null")  */
    AMsyncMessage const* msg2to1 = AMpush(&test_state->stack,
                                          AMgenerateSyncMessage(test_state->n2,
                                                                test_state->s2),
                                          AM_VALUE_SYNC_MESSAGE,
                                          cmocka_cb).sync_message;
    /* assert.deepStrictEqual(decodeSyncMessage(msg1to2).changes.length, 0)  */
    AMchanges msg1to2_changes = AMsyncMessageChanges(msg1to2);
    assert_int_equal(AMchangesSize(&msg1to2_changes), 0);
    /* assert.deepStrictEqual(decodeSyncMessage(msg1to2).have[0].lastSync.length, 0 */
    AMsyncHaves msg1to2_haves = AMsyncMessageHaves(msg1to2);
    AMsyncHave const* msg1to2_have = AMsyncHavesNext(&msg1to2_haves, 1);
    AMchangeHashes msg1to2_last_sync = AMsyncHaveLastSync(msg1to2_have);
    assert_int_equal(AMchangeHashesSize(&msg1to2_last_sync), 0);
    /* assert.deepStrictEqual(decodeSyncMessage(msg2to1).changes.length, 0)  */
    AMchanges msg2to1_changes = AMsyncMessageChanges(msg2to1);
    assert_int_equal(AMchangesSize(&msg2to1_changes), 0);
    /* assert.deepStrictEqual(decodeSyncMessage(msg2to1).have[0].lastSync.length, 0 */
    AMsyncHaves msg2to1_haves = AMsyncMessageHaves(msg2to1);
    AMsyncHave const* msg2to1_have = AMsyncHavesNext(&msg2to1_haves, 1);
    AMchangeHashes msg2to1_last_sync = AMsyncHaveLastSync(msg2to1_have);
    assert_int_equal(AMchangeHashesSize(&msg2to1_last_sync), 0);
    /*                                                                       */
    /* n1 and n2 receive that message and update sync state but make no patc */
    /* n1.receiveSyncMessage(s1, msg2to1)                                    */
    AMfree(AMreceiveSyncMessage(test_state->n1, test_state->s1, msg2to1));
    /* n2.receiveSyncMessage(s2, msg1to2)                                    */
    AMfree(AMreceiveSyncMessage(test_state->n2, test_state->s2, msg1to2));
    /*                                                                       */
    /* now both reply with their local changes that the other lacks
     * (standard warning that 1% of the time this will result in a "needs"
     * message) */
    /* msg1to2 = n1.generateSyncMessage(s1)
       if (msg1to2 === null) { throw new RangeError("message should not be null")  */
    msg1to2 = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->n1, test_state->s1),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    /* assert.deepStrictEqual(decodeSyncMessage(msg1to2).changes.length, 5)  */
    msg1to2_changes = AMsyncMessageChanges(msg1to2);
    assert_int_equal(AMchangesSize(&msg1to2_changes), 5);
    /* msg2to1 = n2.generateSyncMessage(s2)
       if (msg2to1 === null) { throw new RangeError("message should not be null")  */
    msg2to1 = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->n2, test_state->s2),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    /* assert.deepStrictEqual(decodeSyncMessage(msg2to1).changes.length, 5)  */
    msg2to1_changes = AMsyncMessageChanges(msg2to1);
    assert_int_equal(AMchangesSize(&msg2to1_changes), 5);
    /*                                                                       */
    /* both should now apply the changes and update the frontend */
    /* n1.receiveSyncMessage(s1, msg2to1)                                    */
    AMfree(AMreceiveSyncMessage(test_state->n1,
                                test_state->s1,
                                msg2to1));
    /* assert.deepStrictEqual(n1.getMissingDeps(), [])                       */
    AMchangeHashes missing_deps = AMpush(&test_state->stack,
                                         AMgetMissingDeps(test_state->n1, NULL),
                                         AM_VALUE_CHANGE_HASHES,
                                         cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesSize(&missing_deps), 0);
    /* //assert.notDeepStrictEqual(patch1, null)
       assert.deepStrictEqual(n1.materialize(), { x: 4, y: 4 })              */
    assert_int_equal(AMpush(&test_state->stack,
                            AMmapGet(test_state->n1, AM_ROOT, AMstr("x"), NULL),
                            AM_VALUE_UINT,
                            cmocka_cb).uint, 4);
    assert_int_equal(AMpush(&test_state->stack,
                            AMmapGet(test_state->n1, AM_ROOT, AMstr("y"), NULL),
                            AM_VALUE_UINT,
                            cmocka_cb).uint, 4);
    /*                                                                       */
    /* n2.receiveSyncMessage(s2, msg1to2)                                    */
    AMfree(AMreceiveSyncMessage(test_state->n2, test_state->s2, msg1to2));
    /* assert.deepStrictEqual(n2.getMissingDeps(), [])                       */
    missing_deps = AMpush(&test_state->stack,
                          AMgetMissingDeps(test_state->n2, NULL),
                          AM_VALUE_CHANGE_HASHES,
                          cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesSize(&missing_deps), 0);
    /* //assert.notDeepStrictEqual(patch2, null)
       assert.deepStrictEqual(n2.materialize(), { x: 4, y: 4 })              */
    assert_int_equal(AMpush(&test_state->stack,
                            AMmapGet(test_state->n2, AM_ROOT, AMstr("x"), NULL),
                            AM_VALUE_UINT,
                            cmocka_cb).uint, 4);
    assert_int_equal(AMpush(&test_state->stack,
                            AMmapGet(test_state->n2, AM_ROOT, AMstr("y"), NULL),
                            AM_VALUE_UINT,
                            cmocka_cb).uint, 4);
    /*                                                                       */
    /* The response acknowledges the changes received and sends no further
     * changes */
    /* msg1to2 = n1.generateSyncMessage(s1)
       if (msg1to2 === null) { throw new RangeError("message should not be null")  */
    msg1to2 = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->n1, test_state->s1),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    /* assert.deepStrictEqual(decodeSyncMessage(msg1to2).changes.length, 0)  */
    msg1to2_changes = AMsyncMessageChanges(msg1to2);
    assert_int_equal(AMchangesSize(&msg1to2_changes), 0);
    /* msg2to1 = n2.generateSyncMessage(s2)
       if (msg2to1 === null) { throw new RangeError("message should not be null")  */
    msg2to1 = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->n2, test_state->s2),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    /* assert.deepStrictEqual(decodeSyncMessage(msg2to1).changes.length, 0)  */
    msg2to1_changes = AMsyncMessageChanges(msg2to1);
    assert_int_equal(AMchangesSize(&msg2to1_changes), 0);
    /*                                                                       */
    /* After receiving acknowledgements, their shared heads should be equal  */
    /* n1.receiveSyncMessage(s1, msg2to1)                                    */
    AMfree(AMreceiveSyncMessage(test_state->n1, test_state->s1, msg2to1));
    /* n2.receiveSyncMessage(s2, msg1to2)                                    */
    AMfree(AMreceiveSyncMessage(test_state->n2, test_state->s2, msg1to2));
    /* assert.deepStrictEqual(s1.sharedHeads, [head1, head2].sort())         */
    AMchangeHashes s1_shared_heads = AMsyncStateSharedHeads(test_state->s1);
    assert_memory_equal(AMchangeHashesNext(&s1_shared_heads, 1).src,
                        head1.src,
                        head1.count);
    assert_memory_equal(AMchangeHashesNext(&s1_shared_heads, 1).src,
                        head2.src,
                        head2.count);
    /* assert.deepStrictEqual(s2.sharedHeads, [head1, head2].sort())         */
    AMchangeHashes s2_shared_heads = AMsyncStateSharedHeads(test_state->s2);
    assert_memory_equal(AMchangeHashesNext(&s2_shared_heads, 1).src,
                        head1.src,
                        head1.count);
    assert_memory_equal(AMchangeHashesNext(&s2_shared_heads, 1).src,
                        head2.src,
                        head2.count);
    /* //assert.deepStrictEqual(patch1, null)
       //assert.deepStrictEqual(patch2, null)                                */
    /*                                                                       */
    /* We're in sync, no more messages required */
    /* msg1to2 = n1.generateSyncMessage(s1)
       assert.deepStrictEqual(msg1to2, null)                                 */
    AMpush(&test_state->stack,
           AMgenerateSyncMessage(test_state->n1, test_state->s1),
           AM_VALUE_VOID,
           cmocka_cb);
    /* msg2to1 = n2.generateSyncMessage(s2)
       assert.deepStrictEqual(msg2to1, null)                                 */
    AMpush(&test_state->stack,
           AMgenerateSyncMessage(test_state->n2, test_state->s2),
           AM_VALUE_VOID,
           cmocka_cb);
    /*                                                                       */
    /* If we make one more change and start another sync then its lastSync
     * should be updated */
    /* n1.put("_root", "x", 5)                                               */
    AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), 5));
    /* msg1to2 = n1.generateSyncMessage(s1)
       if (msg1to2 === null) { throw new RangeError("message should not be null")  */
    msg1to2 = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->n1, test_state->s1),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    /* assert.deepStrictEqual(decodeSyncMessage(msg1to2).have[0].lastSync, [head1, head2].sort( */
    msg1to2_haves = AMsyncMessageHaves(msg1to2);
    msg1to2_have = AMsyncHavesNext(&msg1to2_haves, 1);
    msg1to2_last_sync = AMsyncHaveLastSync(msg1to2_have);
    AMbyteSpan msg1to2_last_sync_next = AMchangeHashesNext(&msg1to2_last_sync, 1);
    assert_int_equal(msg1to2_last_sync_next.count, head1.count);
    assert_memory_equal(msg1to2_last_sync_next.src, head1.src, head1.count);
    msg1to2_last_sync_next = AMchangeHashesNext(&msg1to2_last_sync, 1);
    assert_int_equal(msg1to2_last_sync_next.count, head2.count);
    assert_memory_equal(msg1to2_last_sync_next.src, head2.src, head2.count);
}

/**
 * \brief should assume sent changes were received until we hear otherwise
 */
static void test_should_assume_sent_changes_were_received_until_we_hear_otherwise(void **state) {
    /* const n1 = create('01234567'), n2 = create('89abcdef')
       const s1 = initSyncState(), s2 = initSyncState()                      */
    TestState* test_state = *state;
    /* let message = null                                                    */
    /*                                                                       */
    /* const items = n1.putObject("_root", "items", [])                      */
    AMobjId const* items = AMpush(&test_state->stack,
                                  AMmapPutObject(test_state->n1,
                                                 AM_ROOT,
                                                 AMstr("items"),
                                                 AM_OBJ_TYPE_LIST),
                                  AM_VALUE_OBJ_ID,
                                  cmocka_cb).obj_id;
    /* n1.commit("", 0)                                                      */
    AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /*                                                                       */
    /* sync(n1, n2, s1, s2)                                                  */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*                                                                       */
    /* n1.push(items, "x")                                                   */
    AMfree(AMlistPutStr(test_state->n1, items, SIZE_MAX, true, AMstr("x")));
    /* n1.commit("", 0)                                                      */
    AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* message = n1.generateSyncMessage(s1)
      if (message === null) { throw new RangeError("message should not be null")  */
    AMsyncMessage const* message = AMpush(&test_state->stack,
                                          AMgenerateSyncMessage(test_state->n1,
                                                                test_state->s1),
                                          AM_VALUE_SYNC_MESSAGE,
                                          cmocka_cb).sync_message;
    /* assert.deepStrictEqual(decodeSyncMessage(message).changes.length, 1)  */
    AMchanges message_changes = AMsyncMessageChanges(message);
    assert_int_equal(AMchangesSize(&message_changes), 1);
    /*                                                                       */
    /* n1.push(items, "y")                                                   */
    AMfree(AMlistPutStr(test_state->n1, items, SIZE_MAX, true, AMstr("y")));
    /* n1.commit("", 0)                                                      */
    AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* message = n1.generateSyncMessage(s1)
       if (message === null) { throw new RangeError("message should not be null")  */
    message = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->n1, test_state->s1),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    /* assert.deepStrictEqual(decodeSyncMessage(message).changes.length, 1)  */
    message_changes = AMsyncMessageChanges(message);
    assert_int_equal(AMchangesSize(&message_changes), 1);
    /*                                                                       */
    /* n1.push(items, "z")                                                   */
    AMfree(AMlistPutStr(test_state->n1, items, SIZE_MAX, true, AMstr("z")));
    /* n1.commit("", 0)                                                      */
    AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /*                                                                       */
    /* message = n1.generateSyncMessage(s1)
       if (message === null) { throw new RangeError("message should not be null")  */
    message = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->n1, test_state->s1),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    /* assert.deepStrictEqual(decodeSyncMessage(message).changes.length, 1)  */
    message_changes = AMsyncMessageChanges(message);
    assert_int_equal(AMchangesSize(&message_changes), 1);
}

/**
 * \brief should work regardless of who initiates the exchange
 */
static void test_should_work_regardless_of_who_initiates_the_exchange(void **state) {
    /* create & synchronize two nodes */
    /* const n1 = create(), n2 = create()
      const s1 = initSyncState(), s2 = initSyncState()                       */
    TestState* test_state = *state;
    /*                                                                       */
    /* for (let i = 0; i < 5; i++) {                                         */
    for (size_t i = 0; i != 5; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* sync(n1, n2, s1, s2)                                                  */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*                                                                       */
    /* modify the first node further */
    /* for (let i = 5; i < 10; i++) {                                        */
    for (size_t i = 5; i != 10; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* assert.notDeepStrictEqual(n1.materialize(), n2.materialize())         */
    assert_false(AMequal(test_state->n1, test_state->n2));
    /* sync(n1, n2, s1, s2)                                                  */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())            */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should work without prior sync state
 */
static void test_should_work_without_prior_sync_state(void **state) {
    /* Scenario:                                                            ,-- c10 <-- c11 <-- c12 <-- c13 <-- c14
     * c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
     *                                                                      `-- c15 <-- c16 <-- c17
     * lastSync is undefined. */
    /*                                                                       */
    /* create two peers both with divergent commits */
    /* const n1 = create('01234567'), n2 = create('89abcdef')
       const s1 = initSyncState(), s2 = initSyncState()                      */
    TestState* test_state = *state;
    /*                                                                       */
    /* for (let i = 0; i < 10; i++) {                                        */
    for (size_t i = 0; i != 10; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* sync(n1, n2)                                                          */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*                                                                       */
    /* for (let i = 10; i < 15; i++) {                                       */
    for (size_t i = 10; i != 15; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* for (let i = 15; i < 18; i++) {                                       */
    for (size_t i = 15; i != 18; ++i) {
        /* n2.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n2, AM_ROOT, AMstr("x"), i));
        /* n2.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n2, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* assert.notDeepStrictEqual(n1.materialize(), n2.materialize())         */
    assert_false(AMequal(test_state->n1, test_state->n2));
    /* sync(n1, n2)                                                          */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /* assert.deepStrictEqual(n1.getHeads(), n2.getHeads())                  */
    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMchangeHashes heads2 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n2),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads2), 0);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())            */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should work with prior sync state
 */
static void test_should_work_with_prior_sync_state_2(void **state) {
    /* Scenario:
     *                                                                      ,-- c10 <-- c11 <-- c12 <-- c13 <-- c14
     * c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
     *                                                                      `-- c15 <-- c16 <-- c17
     * lastSync is c9. */
    /*                                                                       */
    /* create two peers both with divergent commits */
    /* const n1 = create('01234567'), n2 = create('89abcdef')
       let s1 = initSyncState(), s2 = initSyncState()                        */
    TestState* test_state = *state;
    /*                                                                       */
    /* for (let i = 0; i < 10; i++) {                                        */
    for (size_t i = 0; i != 10; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* sync(n1, n2, s1, s2)                                                  */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*                                                                       */
    /* for (let i = 10; i < 15; i++) {                                       */
    for (size_t i = 10; i != 15; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /* for (let i = 15; i < 18; i++) {                                       */
    for (size_t i = 15; i != 18; ++i) {
        /* n2.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n2, AM_ROOT, AMstr("x"), i));
        /* n2.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n2, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* s1 = decodeSyncState(encodeSyncState(s1))                             */
    AMbyteSpan encoded = AMpush(&test_state->stack,
                                AMsyncStateEncode(test_state->s1),
                                AM_VALUE_BYTES,
                                cmocka_cb).bytes;
    AMsyncState* s1 = AMpush(&test_state->stack,
                             AMsyncStateDecode(encoded.src, encoded.count),
                             AM_VALUE_SYNC_STATE,
                             cmocka_cb).sync_state;
    /* s2 = decodeSyncState(encodeSyncState(s2))                             */
    encoded = AMpush(&test_state->stack,
                     AMsyncStateEncode(test_state->s2),
                     AM_VALUE_BYTES,
                     cmocka_cb).bytes;
    AMsyncState* s2 = AMpush(&test_state->stack,
                                      AMsyncStateDecode(encoded.src,
                                                        encoded.count),
                                      AM_VALUE_SYNC_STATE,
                                      cmocka_cb).sync_state;
    /*                                                                       */
    /* assert.notDeepStrictEqual(n1.materialize(), n2.materialize())         */
    assert_false(AMequal(test_state->n1, test_state->n2));
    /* sync(n1, n2, s1, s2)                                                  */
    sync(test_state->n1, test_state->n2, s1, s2);
    /* assert.deepStrictEqual(n1.getHeads(), n2.getHeads())                  */
    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMchangeHashes heads2 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n2),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads2), 0);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())            */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should ensure non-empty state after sync
 */
static void test_should_ensure_non_empty_state_after_sync(void **state) {
    /* const n1 = create('01234567'), n2 = create('89abcdef')
       const s1 = initSyncState(), s2 = initSyncState()                      */
    TestState* test_state = *state;
    /*                                                                       */
    /* for (let i = 0; i < 3; i++) {                                         */
    for (size_t i = 0; i != 3; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* sync(n1, n2, s1, s2)                                                  */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*                                                                       */
    /* assert.deepStrictEqual(s1.sharedHeads, n1.getHeads())                 */
    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMchangeHashes shared_heads1 = AMsyncStateSharedHeads(test_state->s1);
    assert_int_equal(AMchangeHashesCmp(&shared_heads1, &heads1), 0);
    /* assert.deepStrictEqual(s2.sharedHeads, n1.getHeads())                 */
    AMchangeHashes shared_heads2 = AMsyncStateSharedHeads(test_state->s2);
    assert_int_equal(AMchangeHashesCmp(&shared_heads2, &heads1), 0);
}

/**
 * \brief should re-sync after one node crashed with data loss
 */
static void test_should_resync_after_one_node_crashed_with_data_loss(void **state) {
    /* Scenario:     (r)                  (n2)                 (n1)
     * c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8
     * n2 has changes {c0, c1, c2}, n1's lastSync is c5, and n2's lastSync
     * is c2
     * we want to successfully sync (n1) with (r), even though (n1) believes
     * it's talking to (n2) */
    /* const n1 = create('01234567'), n2 = create('89abcdef')
       let s1 = initSyncState()
       const s2 = initSyncState()                                            */
    TestState* test_state = *state;
    /*                                                                       */
    /* n1 makes three changes, which we sync to n2 */
    /* for (let i = 0; i < 3; i++) {                                         */
    for (size_t i = 0; i != 3; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*                                                                       */
    /* save a copy of n2 as "r" to simulate recovering from a crash */
    /* let r
       let rSyncState
       ;[r, rSyncState] = [n2.clone(), s2.clone()]                           */
    AMdoc* r = AMpush(&test_state->stack,
                      AMclone(test_state->n2),
                      AM_VALUE_DOC,
                      cmocka_cb).doc;
    AMbyteSpan const encoded_s2 = AMpush(&test_state->stack,
                                         AMsyncStateEncode(test_state->s2),
                                         AM_VALUE_BYTES,
                                         cmocka_cb).bytes;
    AMsyncState* sync_state_r = AMpush(&test_state->stack,
                                       AMsyncStateDecode(encoded_s2.src,
                                                         encoded_s2.count),
                                       AM_VALUE_SYNC_STATE,
                                       cmocka_cb).sync_state;
    /*                                                                       */
    /* sync another few commits */
    /* for (let i = 3; i < 6; i++) {                                         */
    for (size_t i = 3; i != 6; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* sync(n1, n2, s1, s2)                                                  */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*                                                                       */
    /* everyone should be on the same page here */
    /* assert.deepStrictEqual(n1.getHeads(), n2.getHeads())                  */
    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMchangeHashes heads2 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n2),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads2), 0);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())            */
    assert_true(AMequal(test_state->n1, test_state->n2));
    /*                                                                       */
    /* now make a few more changes and then attempt to sync the fully
     * up-to-date n1 with with the confused r */
    /* for (let i = 6; i < 9; i++) {                                         */
    for (size_t i = 6; i != 9; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* s1 = decodeSyncState(encodeSyncState(s1))                             */
    AMbyteSpan const encoded_s1 = AMpush(&test_state->stack,
                                         AMsyncStateEncode(test_state->s1),
                                         AM_VALUE_BYTES,
                                         cmocka_cb).bytes;
    AMsyncState* const s1 = AMpush(&test_state->stack,
                                   AMsyncStateDecode(encoded_s1.src,
                                                     encoded_s1.count),
                                   AM_VALUE_SYNC_STATE,
                                   cmocka_cb).sync_state;
    /* rSyncState = decodeSyncState(encodeSyncState(rSyncState))             */
    AMbyteSpan const encoded_r = AMpush(&test_state->stack,
                                        AMsyncStateEncode(sync_state_r),
                                        AM_VALUE_BYTES,
                                        cmocka_cb).bytes;
    sync_state_r = AMpush(&test_state->stack,
                          AMsyncStateDecode(encoded_r.src, encoded_r.count),
                          AM_VALUE_SYNC_STATE,
                          cmocka_cb).sync_state;
    /*                                                                       */
    /* assert.notDeepStrictEqual(n1.getHeads(), r.getHeads())                */
    heads1 = AMpush(&test_state->stack,
                    AMgetHeads(test_state->n1),
                    AM_VALUE_CHANGE_HASHES,
                    cmocka_cb).change_hashes;
    AMchangeHashes heads_r = AMpush(&test_state->stack,
                                    AMgetHeads(r),
                                    AM_VALUE_CHANGE_HASHES,
                                    cmocka_cb).change_hashes;
    assert_int_not_equal(AMchangeHashesCmp(&heads1, &heads_r), 0);
    /* assert.notDeepStrictEqual(n1.materialize(), r.materialize())          */
    assert_false(AMequal(test_state->n1, r));
    /* assert.deepStrictEqual(n1.materialize(), { x: 8 })                    */
    assert_int_equal(AMpush(&test_state->stack,
                            AMmapGet(test_state->n1, AM_ROOT, AMstr("x"), NULL),
                            AM_VALUE_UINT,
                            cmocka_cb).uint, 8);
    /* assert.deepStrictEqual(r.materialize(), { x: 2 })                     */
    assert_int_equal(AMpush(&test_state->stack,
                            AMmapGet(r, AM_ROOT, AMstr("x"), NULL),
                            AM_VALUE_UINT,
                            cmocka_cb).uint, 2);
    /* sync(n1, r, s1, rSyncState)                                           */
    sync(test_state->n1, r, test_state->s1, sync_state_r);
    /* assert.deepStrictEqual(n1.getHeads(), r.getHeads())                   */
    heads1 = AMpush(&test_state->stack,
                    AMgetHeads(test_state->n1),
                    AM_VALUE_CHANGE_HASHES,
                    cmocka_cb).change_hashes;
    heads_r = AMpush(&test_state->stack,
                     AMgetHeads(r),
                     AM_VALUE_CHANGE_HASHES,
                     cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads_r), 0);
    /* assert.deepStrictEqual(n1.materialize(), r.materialize())             */
    assert_true(AMequal(test_state->n1, r));
}

/**
 * \brief should re-sync after one node experiences data loss without disconnecting
 */
static void test_should_resync_after_one_node_experiences_data_loss_without_disconnecting(void **state) {
    /* const n1 = create('01234567'), n2 = create('89abcdef')
       const s1 = initSyncState(), s2 = initSyncState()                      */
    TestState* test_state = *state;
    /*                                                                       */
    /* n1 makes three changes which we sync to n2 */
    /* for (let i = 0; i < 3; i++) {                                         */
    for (size_t i = 0; i != 3; ++i) {
        /* n1.put("_root", "x", i)                                           */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), i));
        /* n1.commit("", 0)                                                  */
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* {                                                                     */
    }
    /*                                                                       */
    /* sync(n1, n2, s1, s2)                                                  */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*                                                                       */
    /* assert.deepStrictEqual(n1.getHeads(), n2.getHeads())                  */
    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMchangeHashes heads2 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n2),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads2), 0);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())            */
    assert_true(AMequal(test_state->n1, test_state->n2));
    /*                                                                       */
    /* const n2AfterDataLoss = create('89abcdef')                            */
    AMdoc* n2_after_data_loss = AMpush(&test_state->stack,
                                       AMcreate(AMpush(&test_state->stack,
                                                       AMactorIdInitStr(AMstr("89abcdef")),
                                                       AM_VALUE_ACTOR_ID,
                                                       cmocka_cb).actor_id),
                                       AM_VALUE_DOC,
                                       cmocka_cb).doc;
    /*                                                                       */
    /* "n2" now has no data, but n1 still thinks it does. Note we don't do
     * decodeSyncState(encodeSyncState(s1)) in order to simulate data loss
     * without disconnecting */
    /* sync(n1, n2AfterDataLoss, s1, initSyncState())                        */
    AMsyncState* s2_after_data_loss = AMpush(&test_state->stack,
                                             AMsyncStateInit(),
                                             AM_VALUE_SYNC_STATE,
                                             cmocka_cb).sync_state;
    sync(test_state->n1, n2_after_data_loss, test_state->s1, s2_after_data_loss);
    /* assert.deepStrictEqual(n1.getHeads(), n2.getHeads())                  */
    heads1 = AMpush(&test_state->stack,
                    AMgetHeads(test_state->n1),
                    AM_VALUE_CHANGE_HASHES,
                    cmocka_cb).change_hashes;
    heads2 = AMpush(&test_state->stack,
                    AMgetHeads(test_state->n2),
                    AM_VALUE_CHANGE_HASHES,
                    cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads2), 0);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())            */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should handle changes concurrent to the last sync heads
 */
static void test_should_handle_changes_concurrrent_to_the_last_sync_heads(void **state) {
    /* const n1 = create('01234567'), n2 = create('89abcdef'), n3 = create('fedcba98' */
    TestState* test_state = *state;
    AMdoc* n3 = AMpush(&test_state->stack,
                       AMcreate(AMpush(&test_state->stack,
                                       AMactorIdInitStr(AMstr("fedcba98")),
                                       AM_VALUE_ACTOR_ID,
                                       cmocka_cb).actor_id),
                       AM_VALUE_DOC,
                       cmocka_cb).doc;
    /* const s12 = initSyncState(), s21 = initSyncState(), s23 = initSyncState(), s32 = initSyncState( */
    AMsyncState* s12 = test_state->s1;
    AMsyncState* s21 = test_state->s2;
    AMsyncState* s23 = AMpush(&test_state->stack,
                              AMsyncStateInit(),
                              AM_VALUE_SYNC_STATE,
                              cmocka_cb).sync_state;
    AMsyncState* s32 = AMpush(&test_state->stack,
                              AMsyncStateInit(),
                              AM_VALUE_SYNC_STATE,
                              cmocka_cb).sync_state;
    /*                                                                       */
    /* Change 1 is known to all three nodes */
    /* //n1 = Automerge.change(n1, {time: 0}, doc => doc.x = 1)              */
    /* n1.put("_root", "x", 1); n1.commit("", 0)                             */
    AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), 1));
    AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /*                                                                       */
    /* sync(n1, n2, s12, s21)                                                */
    sync(test_state->n1, test_state->n2, s12, s21);
    /* sync(n2, n3, s23, s32)                                                */
    sync(test_state->n2, n3, s23, s32);
    /*                                                                       */
    /* Change 2 is known to n1 and n2 */
    /* n1.put("_root", "x", 2); n1.commit("", 0)                             */
    AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), 2));
    AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /*                                                                       */
    /* sync(n1, n2, s12, s21)                                                */
    sync(test_state->n1, test_state->n2, s12, s21);
    /*                                                                       */
    /* Each of the three nodes makes one change (changes 3, 4, 5) */
    /* n1.put("_root", "x", 3); n1.commit("", 0)                             */
    AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), 3));
    AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* n2.put("_root", "x", 4); n2.commit("", 0)                             */
    AMfree(AMmapPutUint(test_state->n2, AM_ROOT, AMstr("x"), 4));
    AMfree(AMcommit(test_state->n2, AMstr(""), &TIME_0));
    /* n3.put("_root", "x", 5); n3.commit("", 0)                             */
    AMfree(AMmapPutUint(n3, AM_ROOT, AMstr("x"), 5));
    AMfree(AMcommit(n3, AMstr(""), &TIME_0));
    /*                                                                       */
    /* Apply n3's latest change to n2. */
    /* let change = n3.getLastLocalChange()
       if (change === null) throw new RangeError("no local change")          */
    AMchanges changes = AMpush(&test_state->stack,
                               AMgetLastLocalChange(n3),
                               AM_VALUE_CHANGES,
                               cmocka_cb).changes;
    /* n2.applyChanges([change])                                             */
    AMfree(AMapplyChanges(test_state->n2, &changes));
    /*                                                                       */
    /* Now sync n1 and n2. n3's change is concurrent to n1 and n2's last sync
     * heads */
    /* sync(n1, n2, s12, s21)                                                */
    sync(test_state->n1, test_state->n2, s12, s21);
    /* assert.deepStrictEqual(n1.getHeads(), n2.getHeads())                  */
    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMchangeHashes heads2 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n2),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads2), 0);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())            */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

/**
 * \brief should handle histories with lots of branching and merging
 */
static void test_should_handle_histories_with_lots_of_branching_and_merging(void **state) {
    /* const n1 = create('01234567'), n2 = create('89abcdef'), n3 = create('fedcba98')
       const s1 = initSyncState(), s2 = initSyncState()                      */
    TestState* test_state = *state;
    AMdoc* n3 = AMpush(&test_state->stack,
                       AMcreate(AMpush(&test_state->stack,
                                       AMactorIdInitStr(AMstr("fedcba98")),
                                       AM_VALUE_ACTOR_ID,
                                       cmocka_cb).actor_id),
                       AM_VALUE_DOC,
                       cmocka_cb).doc;
    /* n1.put("_root", "x", 0); n1.commit("", 0)                             */
    AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("x"), 0));
    AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* let change1 = n1.getLastLocalChange()
       if (change1 === null) throw new RangeError("no local change")         */
    AMchanges change1 = AMpush(&test_state->stack,
                               AMgetLastLocalChange(test_state->n1),
                               AM_VALUE_CHANGES,
                               cmocka_cb).changes;
    /* n2.applyChanges([change1])                                            */
    AMfree(AMapplyChanges(test_state->n2, &change1));
    /* let change2 = n1.getLastLocalChange()
       if (change2 === null) throw new RangeError("no local change")         */
    AMchanges change2 = AMpush(&test_state->stack,
                               AMgetLastLocalChange(test_state->n1),
                               AM_VALUE_CHANGES,
                               cmocka_cb).changes;
    /* n3.applyChanges([change2])                                            */
    AMfree(AMapplyChanges(n3, &change2));
    /* n3.put("_root", "x", 1); n3.commit("", 0)                             */
    AMfree(AMmapPutUint(n3, AM_ROOT, AMstr("x"), 1));
    AMfree(AMcommit(n3, AMstr(""), &TIME_0));
    /*                                                                       */
    /*        - n1c1 <------ n1c2 <------ n1c3 <-- etc. <-- n1c20 <------ n1c21
     *       /          \/           \/                              \/
     *      /           /\           /\                              /\
     * c0 <---- n2c1 <------ n2c2 <------ n2c3 <-- etc. <-- n2c20 <------ n2c21
     *      \                                                          /
     *       ---------------------------------------------- n3c1 <-----
     */
    /* for (let i = 1; i < 20; i++) {                                        */
    for (size_t i = 1; i != 20; ++i) {
        /* n1.put("_root", "n1", i); n1.commit("", 0)                        */
        AMfree(AMmapPutUint(test_state->n1, AM_ROOT, AMstr("n1"), i));
        AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
        /* n2.put("_root", "n2", i); n2.commit("", 0)                        */
        AMfree(AMmapPutUint(test_state->n2, AM_ROOT, AMstr("n2"), i));
        AMfree(AMcommit(test_state->n2, AMstr(""), &TIME_0));
        /* const change1 = n1.getLastLocalChange()
           if (change1 === null) throw new RangeError("no local change")     */
        AMchanges change1 = AMpush(&test_state->stack,
                                   AMgetLastLocalChange(test_state->n1),
                                   AM_VALUE_CHANGES,
                                   cmocka_cb).changes;
        /* const change2 = n2.getLastLocalChange()
           if (change2 === null) throw new RangeError("no local change")     */
        AMchanges change2 = AMpush(&test_state->stack,
                                   AMgetLastLocalChange(test_state->n2),
                                   AM_VALUE_CHANGES,
                                   cmocka_cb).changes;
        /* n1.applyChanges([change2])                                        */
        AMfree(AMapplyChanges(test_state->n1, &change2));
        /* n2.applyChanges([change1])                                        */
        AMfree(AMapplyChanges(test_state->n2, &change1));
    /* {                                                                     */
    }
    /*                                                                       */
    /* sync(n1, n2, s1, s2)                                                  */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /*                                                                       */
    /* Having n3's last change concurrent to the last sync heads forces us into
     * the slower code path */
    /* const change3 = n2.getLastLocalChange()
       if (change3 === null) throw new RangeError("no local change")         */
    AMchanges change3 = AMpush(&test_state->stack,
                               AMgetLastLocalChange(n3),
                               AM_VALUE_CHANGES,
                               cmocka_cb).changes;
    /* n2.applyChanges([change3])                                            */
    AMfree(AMapplyChanges(test_state->n2, &change3));
    /* n1.put("_root", "n1", "final"); n1.commit("", 0)                      */
    AMfree(AMmapPutStr(test_state->n1, AM_ROOT, AMstr("n1"), AMstr("final")));
    AMfree(AMcommit(test_state->n1, AMstr(""), &TIME_0));
    /* n2.put("_root", "n2", "final"); n2.commit("", 0)                      */
    AMfree(AMmapPutStr(test_state->n2, AM_ROOT, AMstr("n2"), AMstr("final")));
    AMfree(AMcommit(test_state->n2, AMstr(""), &TIME_0));
    /*                                                                       */
    /* sync(n1, n2, s1, s2)                                                  */
    sync(test_state->n1, test_state->n2, test_state->s1, test_state->s2);
    /* assert.deepStrictEqual(n1.getHeads(), n2.getHeads())                  */
    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMchangeHashes heads2 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->n2),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads2), 0);
    /* assert.deepStrictEqual(n1.materialize(), n2.materialize())            */
    assert_true(AMequal(test_state->n1, test_state->n2));
}

int run_ported_wasm_sync_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_should_send_a_sync_message_implying_no_local_data, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_not_reply_if_we_have_no_data_as_well, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repos_with_equal_heads_do_not_need_a_reply_message, setup, teardown),
        cmocka_unit_test_setup_teardown(test_n1_should_offer_all_changes_to_n2_when_starting_from_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_sync_peers_where_one_has_commits_the_other_does_not, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_work_with_prior_sync_state, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_not_generate_messages_once_synced, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_allow_simultaneous_messages_during_synchronization, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_assume_sent_changes_were_received_until_we_hear_otherwise, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_work_regardless_of_who_initiates_the_exchange, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_work_without_prior_sync_state, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_work_with_prior_sync_state_2, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_ensure_non_empty_state_after_sync, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_resync_after_one_node_crashed_with_data_loss, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_resync_after_one_node_experiences_data_loss_without_disconnecting, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_handle_changes_concurrrent_to_the_last_sync_heads, setup, teardown),
        cmocka_unit_test_setup_teardown(test_should_handle_histories_with_lots_of_branching_and_merging, setup, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
