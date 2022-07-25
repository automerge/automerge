#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

/* third-party */
#include <cmocka.h>

/* local */
#include "automerge.h"
#include "stack_utils.h"

typedef struct {
    AMresultStack* stack;
    AMdoc* doc1;
    AMdoc* doc2;
    AMsyncState* sync_state1;
    AMsyncState* sync_state2;
} TestState;

static int setup(void** state) {
    TestState* test_state = test_calloc(1, sizeof(TestState));
    test_state->doc1 = AMpush(&test_state->stack,
                              AMcreate(),
                              AM_VALUE_DOC,
                              cmocka_cb).doc;
    test_state->doc2 = AMpush(&test_state->stack,
                              AMcreate(),
                              AM_VALUE_DOC,
                              cmocka_cb).doc;
    test_state->sync_state1 = AMpush(&test_state->stack,
                                     AMsyncStateInit(),
                                     AM_VALUE_SYNC_STATE,
                                     cmocka_cb).sync_state;
    test_state->sync_state2 = AMpush(&test_state->stack,
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

/**
 * \brief Data sync protocol with docs already in sync, an empty local doc
 *        should send a sync message implying no local data.
 */
static void test_converged_empty_local_doc_reply_no_local_data(void **state) {
    TestState* test_state = *state;
    AMsyncMessage const* const sync_message = AMpush(&test_state->stack,
                                                     AMgenerateSyncMessage(
                                                         test_state->doc1,
                                                         test_state->sync_state1),
                                                     AM_VALUE_SYNC_MESSAGE,
                                                     cmocka_cb).sync_message;
    AMchangeHashes heads = AMsyncMessageHeads(sync_message);
    assert_int_equal(AMchangeHashesSize(&heads), 0);
    AMchangeHashes needs = AMsyncMessageNeeds(sync_message);
    assert_int_equal(AMchangeHashesSize(&needs), 0);
    AMsyncHaves haves = AMsyncMessageHaves(sync_message);
    assert_int_equal(AMsyncHavesSize(&haves), 1);
    AMsyncHave const* have0 = AMsyncHavesNext(&haves, 1);
    AMchangeHashes last_sync = AMsyncHaveLastSync(have0);
    assert_int_equal(AMchangeHashesSize(&last_sync), 0);
    AMchanges changes = AMsyncMessageChanges(sync_message);
    assert_int_equal(AMchangesSize(&changes), 0);
}

/**
 * \brief Data sync protocol with docs already in sync, an empty local doc
 *        should not reply if we have no data as well.
 */
static void test_converged_empty_local_doc_no_reply(void **state) {
    TestState* test_state = *state;
    AMsyncMessage const* const sync_message1 = AMpush(&test_state->stack,
                                                      AMgenerateSyncMessage(
                                                          test_state->doc1,
                                                          test_state->sync_state1),
                                                      AM_VALUE_SYNC_MESSAGE,
                                                      cmocka_cb).sync_message;
    AMfree(AMreceiveSyncMessage(test_state->doc2,
                                test_state->sync_state2,
                                sync_message1));
    AMpush(&test_state->stack,
           AMgenerateSyncMessage(test_state->doc2, test_state->sync_state2),
           AM_VALUE_VOID,
           cmocka_cb);
}

/**
 * \brief Data sync protocol with docs already in sync, documents with data and
 *        repos with equal heads do not need a reply message.
 */
static void test_converged_equal_heads_no_reply(void **state) {
    TestState* test_state = *state;

    /* Make two nodes with the same changes. */
    time_t const time = 0;
    for (size_t index = 0; index != 10; ++index) {
        AMfree(AMlistPutUint(test_state->doc1, AM_ROOT, index, true, index));
        AMcommit(test_state->doc1, NULL, &time);
    }
    AMchanges const changes = AMpush(&test_state->stack,
                                     AMgetChanges(test_state->doc1, NULL),
                                     AM_VALUE_CHANGES,
                                     cmocka_cb).changes;
    AMfree(AMapplyChanges(test_state->doc2, &changes));
    assert_true(AMequal(test_state->doc1, test_state->doc2));

    /* Generate a naive sync message. */
    AMsyncMessage const* sync_message1 = AMpush(&test_state->stack,
                                                AMgenerateSyncMessage(
                                                    test_state->doc1,
                                                    test_state->sync_state1),
                                                AM_VALUE_SYNC_MESSAGE,
                                                cmocka_cb).sync_message;
    AMchangeHashes const last_sent_heads = AMsyncStateLastSentHeads(
        test_state->sync_state1
    );
    AMchangeHashes const heads = AMpush(&test_state->stack,
                                        AMgetHeads(test_state->doc1),
                                        AM_VALUE_CHANGE_HASHES,
                                        cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&last_sent_heads, &heads), 0);

    /* Heads are equal so this message should be void. */
    AMfree(AMreceiveSyncMessage(test_state->doc2,
                                test_state->sync_state2,
                                sync_message1));
    AMpush(&test_state->stack,
           AMgenerateSyncMessage(test_state->doc2, test_state->sync_state2),
           AM_VALUE_VOID,
           cmocka_cb);
}

/**
 * \brief Data sync protocol with docs already in sync, documents with data and
 *        the first node should offer all changes to the second node when
 *        starting from nothing.
 */
static void test_converged_offer_all_changes_from_nothing(void **state) {
    TestState* test_state = *state;

    /* Make changes for the first node that the second node should request. */
    time_t const time = 0;
    for (size_t index = 0; index != 10; ++index) {
        AMfree(AMlistPutUint(test_state->doc1, AM_ROOT, index, true, index));
        AMcommit(test_state->doc1, NULL, &time);
    }

    assert_false(AMequal(test_state->doc1, test_state->doc2));
    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);
    assert_true(AMequal(test_state->doc1, test_state->doc2));
}

/**
 * \brief Data sync protocol with docs already in sync, documents with data and
 *        it should sync peers where one has commits the other does not.
 */
static void test_converged_sync_peers_with_uneven_commits(void **state) {
    TestState* test_state = *state;

    /* Make changes for the first node that the second node should request. */
    time_t const time = 0;
    for (size_t index = 0; index != 10; ++index) {
        AMfree(AMlistPutUint(test_state->doc1, AM_ROOT, index, true, index));
        AMcommit(test_state->doc1, NULL, &time);
    }

    assert_false(AMequal(test_state->doc1, test_state->doc2));
    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);
    assert_true(AMequal(test_state->doc1, test_state->doc2));
}

/**
 * \brief Data sync protocol with docs already in sync, documents with data and
 *        it should work with prior sync state.
 */
static void test_converged_works_with_prior_sync_state(void **state) {
    /* Create & synchronize two nodes. */
    TestState* test_state = *state;

    time_t const time = 0;
    for (size_t value = 0; value != 5; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
    }
    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);

    /* Modify the first node further. */
    for (size_t value = 5; value != 10; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
    }

    assert_false(AMequal(test_state->doc1, test_state->doc2));
    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);
    assert_true(AMequal(test_state->doc1, test_state->doc2));
}

/**
 * \brief Data sync protocol with docs already in sync, documents with data and
 *        it should not generate messages once synced.
 */
static void test_converged_no_message_once_synced(void **state) {
    /* Create & synchronize two nodes. */
    TestState* test_state = *state;
    AMfree(AMsetActor(test_state->doc1, AMpush(&test_state->stack,
                                               AMactorIdInitStr("abc123"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    AMfree(AMsetActor(test_state->doc2, AMpush(&test_state->stack,
                                               AMactorIdInitStr("def456"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));

    time_t const time = 0;
    for (size_t value = 0; value != 5; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
        AMfree(AMmapPutUint(test_state->doc2, AM_ROOT, "y", value));
        AMcommit(test_state->doc2, NULL, &time);
    }

    /* The first node reports what it has. */
    AMsyncMessage const* message = AMpush(&test_state->stack,
                                          AMgenerateSyncMessage(
                                            test_state->doc1,
                                            test_state->sync_state1),
                                          AM_VALUE_SYNC_MESSAGE,
                                          cmocka_cb).sync_message;

    /* The second node receives that message and sends changes along with what
     * it has. */
    AMfree(AMreceiveSyncMessage(test_state->doc2,
                                test_state->sync_state2,
                                message));
    message = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->doc2,
                                           test_state->sync_state2),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    AMchanges message_changes = AMsyncMessageChanges(message);
    assert_int_equal(AMchangesSize(&message_changes), 5);

    /* The first node receives the changes and replies with the changes it now
     * knows that the second node needs. */
    AMfree(AMreceiveSyncMessage(test_state->doc1,
                                test_state->sync_state1,
                                message));
    message = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->doc1,
                                           test_state->sync_state1),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    message_changes = AMsyncMessageChanges(message);
    assert_int_equal(AMchangesSize(&message_changes), 5);

    /* The second node applies the changes and sends confirmation ending the
     * exchange. */
    AMfree(AMreceiveSyncMessage(test_state->doc2,
                                test_state->sync_state2,
                                message));
    message = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->doc2,
                                           test_state->sync_state2),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;

    /* The first node receives the message and has nothing more to say. */
    AMfree(AMreceiveSyncMessage(test_state->doc1,
                                test_state->sync_state1,
                                message));
    AMpush(&test_state->stack,
           AMgenerateSyncMessage(test_state->doc1, test_state->sync_state1),
           AM_VALUE_VOID,
           cmocka_cb);

    /* The second node also has nothing left to say. */
    AMpush(&test_state->stack,
           AMgenerateSyncMessage(test_state->doc2, test_state->sync_state2),
           AM_VALUE_VOID,
           cmocka_cb);
}

/**
 * \brief Data sync protocol with docs already in sync, documents with data and
 *        it should allow simultaneous messages during synchronization.
 */
static void test_converged_allow_simultaneous_messages(void **state) {
    /* Create & synchronize two nodes. */
    TestState* test_state = *state;
    AMfree(AMsetActor(test_state->doc1, AMpush(&test_state->stack,
                                               AMactorIdInitStr("abc123"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    AMfree(AMsetActor(test_state->doc2, AMpush(&test_state->stack,
                                               AMactorIdInitStr("def456"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));

    time_t const time = 0;
    for (size_t value = 0; value != 5; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
        AMfree(AMmapPutUint(test_state->doc2, AM_ROOT, "y", value));
        AMcommit(test_state->doc2, NULL, &time);
    }
    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMbyteSpan head1 = AMchangeHashesNext(&heads1, 1);
    AMchangeHashes heads2 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc2),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMbyteSpan head2 = AMchangeHashesNext(&heads2, 1);

    /* Both sides report what they have but have no shared peer state. */
    AMsyncMessage const* msg1to2 = AMpush(&test_state->stack,
                                          AMgenerateSyncMessage(
                                              test_state->doc1,
                                              test_state->sync_state1),
                                          AM_VALUE_SYNC_MESSAGE,
                                          cmocka_cb).sync_message;
    AMsyncMessage const* msg2to1 = AMpush(&test_state->stack,
                                          AMgenerateSyncMessage(
                                              test_state->doc2,
                                              test_state->sync_state2),
                                          AM_VALUE_SYNC_MESSAGE,
                                          cmocka_cb).sync_message;
    AMchanges msg1to2_changes = AMsyncMessageChanges(msg1to2);
    assert_int_equal(AMchangesSize(&msg1to2_changes), 0);
    AMsyncHaves msg1to2_haves = AMsyncMessageHaves(msg1to2);
    AMsyncHave const* msg1to2_have = AMsyncHavesNext(&msg1to2_haves, 1);
    AMchangeHashes msg1to2_last_sync = AMsyncHaveLastSync(msg1to2_have);
    assert_int_equal(AMchangeHashesSize(&msg1to2_last_sync), 0);
    AMchanges msg2to1_changes = AMsyncMessageChanges(msg2to1);
    assert_int_equal(AMchangesSize(&msg2to1_changes), 0);
    AMsyncHaves msg2to1_haves = AMsyncMessageHaves(msg2to1);
    AMsyncHave const* msg2to1_have = AMsyncHavesNext(&msg2to1_haves, 1);
    AMchangeHashes msg2to1_last_sync = AMsyncHaveLastSync(msg2to1_have);
    assert_int_equal(AMchangeHashesSize(&msg2to1_last_sync), 0);

    /* Both nodes receive messages from each other and update their
     * synchronization states. */
    AMfree(AMreceiveSyncMessage(test_state->doc1,
                                test_state->sync_state1,
                                msg2to1));
    AMfree(AMreceiveSyncMessage(test_state->doc2,
                                test_state->sync_state2,
                                msg1to2));

    /* Now both reply with their local changes that the other lacks
     * (standard warning that 1% of the time this will result in a "needs"
     * message). */
    msg1to2 = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->doc1,
                                           test_state->sync_state1),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    msg1to2_changes = AMsyncMessageChanges(msg1to2);
    assert_int_equal(AMchangesSize(&msg1to2_changes), 5);
    msg2to1 = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->doc2,
                                           test_state->sync_state2),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    msg2to1_changes = AMsyncMessageChanges(msg2to1);
    assert_int_equal(AMchangesSize(&msg2to1_changes), 5);

    /* Both should now apply the changes. */
    AMfree(AMreceiveSyncMessage(test_state->doc1,
                                test_state->sync_state1,
                                msg2to1));
    AMchangeHashes missing_deps = AMpush(&test_state->stack,
                                         AMgetMissingDeps(test_state->doc1,
                                                          NULL),
                                         AM_VALUE_CHANGE_HASHES,
                                         cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesSize(&missing_deps), 0);
    assert_int_equal(AMpush(&test_state->stack,
                            AMmapGet(test_state->doc1, AM_ROOT, "x", NULL),
                            AM_VALUE_UINT,
                            cmocka_cb).uint, 4);
    assert_int_equal(AMpush(&test_state->stack,
                            AMmapGet(test_state->doc1, AM_ROOT, "y", NULL),
                            AM_VALUE_UINT,
                            cmocka_cb).uint, 4);

    AMfree(AMreceiveSyncMessage(test_state->doc2,
                                test_state->sync_state2,
                                msg1to2));
    missing_deps = AMpush(&test_state->stack,
                          AMgetMissingDeps(test_state->doc2, NULL),
                          AM_VALUE_CHANGE_HASHES,
                          cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesSize(&missing_deps), 0);
    assert_int_equal(AMpush(&test_state->stack,
                            AMmapGet(test_state->doc2, AM_ROOT, "x", NULL),
                            AM_VALUE_UINT,
                            cmocka_cb).uint, 4);
    assert_int_equal(AMpush(&test_state->stack,
                            AMmapGet(test_state->doc2, AM_ROOT, "y", NULL),
                            AM_VALUE_UINT,
                            cmocka_cb).uint, 4);

    /* The response acknowledges that the changes were received and sends no
     * further changes. */
    msg1to2 = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->doc1,
                                           test_state->sync_state1),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    msg1to2_changes = AMsyncMessageChanges(msg1to2);
    assert_int_equal(AMchangesSize(&msg1to2_changes), 0);
    msg2to1 = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->doc2,
                                           test_state->sync_state2),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    msg2to1_changes = AMsyncMessageChanges(msg2to1);
    assert_int_equal(AMchangesSize(&msg2to1_changes), 0);

    /* After receiving acknowledgements their shared heads should be equal. */
    AMfree(AMreceiveSyncMessage(test_state->doc1,
                                test_state->sync_state1,
                                msg2to1));
    AMfree(AMreceiveSyncMessage(test_state->doc2,
                                test_state->sync_state2,
                                msg1to2));

    /* They're synchronized so no more messages are required. */
    AMpush(&test_state->stack,
           AMgenerateSyncMessage(test_state->doc1, test_state->sync_state1),
           AM_VALUE_VOID,
           cmocka_cb);
    AMpush(&test_state->stack,
           AMgenerateSyncMessage(test_state->doc2, test_state->sync_state2),
           AM_VALUE_VOID,
           cmocka_cb);

    /* If we make one more change and start synchronizing then its "last
     * sync" property should be updated. */
    AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", 5));
    AMcommit(test_state->doc1, NULL, &time);
    msg1to2 = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->doc1,
                                           test_state->sync_state1),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
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
 * \brief Data sync protocol with docs already in sync, documents with data and
 *        it should assume sent changes were received until we hear otherwise.
 */
static void test_converged_assume_sent_changes_were_received(void **state) {
    TestState* test_state = *state;
    AMfree(AMsetActor(test_state->doc1, AMpush(&test_state->stack,
                                               AMactorIdInitStr("01234567"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    AMfree(AMsetActor(test_state->doc2, AMpush(&test_state->stack,
                                               AMactorIdInitStr("89abcdef"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));

    AMobjId const* items = AMpush(&test_state->stack,
                                  AMmapPutObject(test_state->doc1,
                                                 AM_ROOT,
                                                 "items",
                                                 AM_OBJ_TYPE_LIST),
                                  AM_VALUE_OBJ_ID,
                                  cmocka_cb).obj_id;
    time_t const time = 0;
    AMcommit(test_state->doc1, NULL, &time);
    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);

    AMfree(AMlistPutStr(test_state->doc1, items, 0, true, "x"));
    AMcommit(test_state->doc1, NULL, &time);
    AMsyncMessage const* message = AMpush(&test_state->stack,
                                          AMgenerateSyncMessage(
                                              test_state->doc1,
                                              test_state->sync_state1),
                                          AM_VALUE_SYNC_MESSAGE,
                                          cmocka_cb).sync_message;
    AMchanges message_changes = AMsyncMessageChanges(message);
    assert_int_equal(AMchangesSize(&message_changes), 1);

    AMfree(AMlistPutStr(test_state->doc1, items, 1, true, "y"));
    AMcommit(test_state->doc1, NULL, &time);
    message = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->doc1,
                                           test_state->sync_state1),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    message_changes = AMsyncMessageChanges(message);
    assert_int_equal(AMchangesSize(&message_changes), 1);

    AMfree(AMlistPutStr(test_state->doc1, items, 2, true, "z"));
    AMcommit(test_state->doc1, NULL, &time);
    message = AMpush(&test_state->stack,
                     AMgenerateSyncMessage(test_state->doc1,
                                           test_state->sync_state1),
                     AM_VALUE_SYNC_MESSAGE,
                     cmocka_cb).sync_message;
    message_changes = AMsyncMessageChanges(message);
    assert_int_equal(AMchangesSize(&message_changes), 1);
}

/**
 * \brief Data sync protocol with docs already in sync, documents with data and
 *        it should work regardless of who initiates the exchange.
 */
static void test_converged_works_regardless_of_who_initiates(void **state) {
    /* Create & synchronize two nodes. */
    TestState* test_state = *state;

    time_t const time = 0;
    for (size_t value = 0; value != 5; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
    }
    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);

    /* Modify the first node further. */
    for (size_t value = 5; value != 10; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
    }

    assert_false(AMequal(test_state->doc1, test_state->doc2));
    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);
    assert_true(AMequal(test_state->doc1, test_state->doc2));
}

/**
 * \brief Data sync protocol with diverged documents and it should work without
 *        prior sync state.
 */
static void test_diverged_works_without_prior_sync_state(void **state) {
    /* Scenario:
     *                                                                      ,-- c10 <-- c11 <-- c12 <-- c13 <-- c14
     * c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
     *                                                                      `-- c15 <-- c16 <-- c17
     * lastSync is undefined. */

    /* Create two peers both with divergent commits. */
    TestState* test_state = *state;
    AMfree(AMsetActor(test_state->doc1, AMpush(&test_state->stack,
                                               AMactorIdInitStr("01234567"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    AMfree(AMsetActor(test_state->doc2, AMpush(&test_state->stack,
                                               AMactorIdInitStr("89abcdef"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    time_t const time = 0;
    for (size_t value = 0; value != 10; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
    }

    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);

    for (size_t value = 10; value != 15; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
    }
    for (size_t value = 15; value != 18; ++value) {
        AMfree(AMmapPutUint(test_state->doc2, AM_ROOT, "x", value));
        AMcommit(test_state->doc2, NULL, &time);
    }

    assert_false(AMequal(test_state->doc1, test_state->doc2));
    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);
    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMchangeHashes heads2 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc2),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads2), 0);
    assert_true(AMequal(test_state->doc1, test_state->doc2));
}

/**
 * \brief Data sync protocol with diverged documents and it should work with
 *        prior sync state.
 */
static void test_diverged_works_with_prior_sync_state(void **state) {
    /* Scenario:
     *                                                                      ,-- c10 <-- c11 <-- c12 <-- c13 <-- c14
     * c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8 <-- c9 <-+
     *                                                                      `-- c15 <-- c16 <-- c17
     * lastSync is c9. */

    /* Create two peers both with divergent commits. */
    TestState* test_state = *state;
    AMfree(AMsetActor(test_state->doc1, AMpush(&test_state->stack,
                                               AMactorIdInitStr("01234567"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    AMfree(AMsetActor(test_state->doc2, AMpush(&test_state->stack,
                                               AMactorIdInitStr("89abcdef"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    time_t const time = 0;
    for (size_t value = 0; value != 10; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
    }
    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);

    for (size_t value = 10; value != 15; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
    }
    for (size_t value = 15; value != 18; ++value) {
        AMfree(AMmapPutUint(test_state->doc2, AM_ROOT, "x", value));
        AMcommit(test_state->doc2, NULL, &time);
    }
    AMbyteSpan encoded = AMpush(&test_state->stack,
                                AMsyncStateEncode(test_state->sync_state1),
                                AM_VALUE_BYTES,
                                cmocka_cb).bytes;
    AMsyncState* sync_state1 = AMpush(&test_state->stack,
                                      AMsyncStateDecode(encoded.src,
                                                        encoded.count),
                                      AM_VALUE_SYNC_STATE,
                                      cmocka_cb).sync_state;
    encoded = AMpush(&test_state->stack,
                     AMsyncStateEncode(test_state->sync_state2),
                     AM_VALUE_BYTES,
                     cmocka_cb).bytes;
    AMsyncState* sync_state2 = AMpush(&test_state->stack,
                                      AMsyncStateDecode(encoded.src,
                                                        encoded.count),
                                      AM_VALUE_SYNC_STATE,
                                      cmocka_cb).sync_state;

    assert_false(AMequal(test_state->doc1, test_state->doc2));
    sync(test_state->doc1, test_state->doc2, sync_state1, sync_state2);
    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMchangeHashes heads2 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc2),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads2), 0);
    assert_true(AMequal(test_state->doc1, test_state->doc2));
}

/**
 * \brief Data sync protocol with diverged documents and it should ensure
 *        non-empty state after synchronization.
 */
static void test_diverged_ensure_not_empty_after_sync(void **state) {
    TestState* test_state = *state;
    AMfree(AMsetActor(test_state->doc1, AMpush(&test_state->stack,
                                               AMactorIdInitStr("01234567"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    AMfree(AMsetActor(test_state->doc2, AMpush(&test_state->stack,
                                               AMactorIdInitStr("89abcdef"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));

    time_t const time = 0;
    for (size_t value = 0; value != 3; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
    }
    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);

    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMchangeHashes shared_heads1 = AMsyncStateSharedHeads(test_state->sync_state1);
    assert_int_equal(AMchangeHashesCmp(&shared_heads1, &heads1), 0);
    AMchangeHashes shared_heads2 = AMsyncStateSharedHeads(test_state->sync_state2);
    assert_int_equal(AMchangeHashesCmp(&shared_heads2, &heads1), 0);
}

/**
 * \brief Data sync protocol with diverged documents and it should
 *        re-synchronize after one node crashed with data loss.
 */
static void test_diverged_resync_after_node_crash_with_data_loss(void **state) {
      /* Scenario:
       *               (r)                  (n2)                 (n1)
       * c0 <-- c1 <-- c2 <-- c3 <-- c4 <-- c5 <-- c6 <-- c7 <-- c8
       * n2 has changes {c0, c1, c2}, n1's lastSync is c5, and n2's lastSync
       * is c2.
       * We want to successfully sync (n1) with (r), even though (n1) believes
       * it's talking to (n2). */
    TestState* test_state = *state;
    AMfree(AMsetActor(test_state->doc1, AMpush(&test_state->stack,
                                               AMactorIdInitStr("01234567"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    AMfree(AMsetActor(test_state->doc2, AMpush(&test_state->stack,
                                               AMactorIdInitStr("89abcdef"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));

    /* n1 makes three changes which we synchronize to n2. */
    time_t const time = 0;
    for (size_t value = 0; value != 3; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
    }
    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);

    /* Save a copy of n2 as "r" to simulate recovering from a crash. */
    AMdoc* r = AMpush(&test_state->stack,
                      AMdup(test_state->doc2),
                      AM_VALUE_DOC,
                      cmocka_cb).doc;
    AMbyteSpan encoded = AMpush(&test_state->stack,
                                AMsyncStateEncode(test_state->sync_state2),
                                AM_VALUE_BYTES,
                                cmocka_cb).bytes;
    AMsyncState* sync_stater = AMpush(&test_state->stack,
                                      AMsyncStateDecode(encoded.src,
                                                        encoded.count),
                                      AM_VALUE_SYNC_STATE,
                                      cmocka_cb).sync_state;
    /* Synchronize another few commits. */
    for (size_t value = 3; value != 6; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
    }
    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);
    /* Everyone should be on the same page here. */
    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMchangeHashes heads2 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc2),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads2), 0);
    assert_true(AMequal(test_state->doc1, test_state->doc2));

    /* Now make a few more changes and then attempt to synchronize the
     * fully-up-to-date n1 with with the confused r. */
    for (size_t value = 6; value != 9; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
    }
    heads1 = AMpush(&test_state->stack,
                    AMgetHeads(test_state->doc1),
                    AM_VALUE_CHANGE_HASHES,
                    cmocka_cb).change_hashes;
    AMchangeHashes headsr = AMpush(&test_state->stack,
                                   AMgetHeads(r),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    assert_int_not_equal(AMchangeHashesCmp(&heads1, &headsr), 0);
    assert_false(AMequal(test_state->doc1, r));
    assert_int_equal(AMpush(&test_state->stack,
                            AMmapGet(test_state->doc1, AM_ROOT, "x", NULL),
                            AM_VALUE_UINT,
                            cmocka_cb).uint, 8);
    assert_int_equal(AMpush(&test_state->stack,
                            AMmapGet(r, AM_ROOT, "x", NULL),
                            AM_VALUE_UINT,
                            cmocka_cb).uint, 2);
    sync(test_state->doc1,
         r,
         test_state->sync_state1,
         sync_stater);
    heads1 = AMpush(&test_state->stack,
                    AMgetHeads(test_state->doc1),
                    AM_VALUE_CHANGE_HASHES,
                    cmocka_cb).change_hashes;
    headsr = AMpush(&test_state->stack,
                    AMgetHeads(r),
                    AM_VALUE_CHANGE_HASHES,
                    cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &headsr), 0);
    assert_true(AMequal(test_state->doc1, r));
}

/**
 * \brief Data sync protocol with diverged documents and it should resync after
 *        one node experiences data loss without disconnecting.
 */
static void test_diverged_resync_after_data_loss_without_disconnection(void **state) {
    TestState* test_state = *state;
    AMfree(AMsetActor(test_state->doc1, AMpush(&test_state->stack,
                                               AMactorIdInitStr("01234567"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    AMfree(AMsetActor(test_state->doc2, AMpush(&test_state->stack,
                                               AMactorIdInitStr("89abcdef"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));

    /* n1 makes three changes which we synchronize to n2. */
    time_t const time = 0;
    for (size_t value = 0; value != 3; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", value));
        AMcommit(test_state->doc1, NULL, &time);
    }
    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);

    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMchangeHashes heads2 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc2),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads2), 0);
    assert_true(AMequal(test_state->doc1, test_state->doc2));

    AMdoc* doc2_after_data_loss = AMpush(&test_state->stack,
                                         AMcreate(),
                                         AM_VALUE_DOC,
                                         cmocka_cb).doc;
    AMfree(AMsetActor(doc2_after_data_loss, AMpush(&test_state->stack,
                                                   AMactorIdInitStr("89abcdef"),
                                                   AM_VALUE_ACTOR_ID,
                                                   cmocka_cb).actor_id));

    /* "n2" now has no data, but n1 still thinks it does. Note we don't do
     * decodeSyncState(encodeSyncState(s1)) in order to simulate data loss
     * without disconnecting. */
    AMsyncState* sync_state2_after_data_loss = AMpush(&test_state->stack,
                                                      AMsyncStateInit(),
                                                      AM_VALUE_SYNC_STATE,
                                                      cmocka_cb).sync_state;
    sync(test_state->doc1,
         doc2_after_data_loss,
         test_state->sync_state1,
         sync_state2_after_data_loss);
    heads1 = AMpush(&test_state->stack,
                    AMgetHeads(test_state->doc1),
                    AM_VALUE_CHANGE_HASHES,
                    cmocka_cb).change_hashes;
    heads2 = AMpush(&test_state->stack,
                    AMgetHeads(doc2_after_data_loss),
                    AM_VALUE_CHANGE_HASHES,
                    cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads2), 0);
    assert_true(AMequal(test_state->doc1, doc2_after_data_loss));
}

/**
 * \brief Data sync protocol with diverged documents and it should handle
 *        changes concurrent to the last sync heads.
 */
static void test_diverged_handles_concurrent_changes(void **state) {
    TestState* test_state = *state;
    AMfree(AMsetActor(test_state->doc1, AMpush(&test_state->stack,
                                               AMactorIdInitStr("01234567"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    AMfree(AMsetActor(test_state->doc2, AMpush(&test_state->stack,
                                               AMactorIdInitStr("89abcdef"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    AMdoc* doc3 = AMpush(&test_state->stack,
                         AMcreate(),
                         AM_VALUE_DOC,
                         cmocka_cb).doc;
    AMfree(AMsetActor(doc3, AMpush(&test_state->stack,
                                   AMactorIdInitStr("fedcba98"),
                                   AM_VALUE_ACTOR_ID,
                                   cmocka_cb).actor_id));
    AMsyncState* sync_state12 = test_state->sync_state1;
    AMsyncState* sync_state21 = test_state->sync_state2;
    AMsyncState* sync_state23 = AMpush(&test_state->stack,
                                       AMsyncStateInit(),
                                       AM_VALUE_SYNC_STATE,
                                       cmocka_cb).sync_state;
    AMsyncState* sync_state32 = AMpush(&test_state->stack,
                                       AMsyncStateInit(),
                                       AM_VALUE_SYNC_STATE,
                                       cmocka_cb).sync_state;

    /* Change 1 is known to all three nodes. */
    time_t const time = 0;
    AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", 1));
    AMcommit(test_state->doc1, NULL, &time);
    sync(test_state->doc1, test_state->doc2, sync_state12, sync_state21);
    sync(test_state->doc2, doc3, sync_state23, sync_state32);

    /* Change 2 is known to n1 and n2. */
    AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", 2));
    AMcommit(test_state->doc1, NULL, &time);
    sync(test_state->doc1, test_state->doc2, sync_state12, sync_state21);

    /* Each of the three nodes makes one change (changes 3, 4, 5). */
    AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", 3));
    AMcommit(test_state->doc1, NULL, &time);
    AMfree(AMmapPutUint(test_state->doc2, AM_ROOT, "x", 4));
    AMcommit(test_state->doc2, NULL, &time);
    AMfree(AMmapPutUint(doc3, AM_ROOT, "x", 5));
    AMcommit(doc3, NULL, &time);

    /* Apply n3's latest change to n2. */
    AMchanges changes = AMpush(&test_state->stack,
                               AMgetLastLocalChange(doc3),
                               AM_VALUE_CHANGES,
                               cmocka_cb).changes;
    AMfree(AMapplyChanges(test_state->doc2, &changes));

    /* Now sync n1 and n2. n3's change is concurrent to n1 and n2's last sync
     * heads. */
    sync(test_state->doc1, test_state->doc2, sync_state12, sync_state21);
    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMchangeHashes heads2 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc2),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads2), 0);
    assert_true(AMequal(test_state->doc1, test_state->doc2));
}

/**
 * \brief Data sync protocol with diverged documents and it should handle
 *        histories with lots of branching and merging.
 */
static void test_diverged_handles_histories_of_branching_and_merging(void **state) {
    TestState* test_state = *state;
    AMfree(AMsetActor(test_state->doc1, AMpush(&test_state->stack,
                                               AMactorIdInitStr("01234567"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    AMfree(AMsetActor(test_state->doc2, AMpush(&test_state->stack,
                                               AMactorIdInitStr("89abcdef"),
                                               AM_VALUE_ACTOR_ID,
                                               cmocka_cb).actor_id));
    AMdoc* doc3 = AMpush(&test_state->stack,
                         AMcreate(),
                         AM_VALUE_DOC,
                         cmocka_cb).doc;
    AMfree(AMsetActor(doc3, AMpush(&test_state->stack,
                                   AMactorIdInitStr("fedcba98"),
                                   AM_VALUE_ACTOR_ID,
                                   cmocka_cb).actor_id));
    time_t const time = 0;
    AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "x", 0));
    AMcommit(test_state->doc1, NULL, &time);
    AMchanges changes = AMpush(&test_state->stack,
                               AMgetLastLocalChange(test_state->doc1),
                               AM_VALUE_CHANGES,
                               cmocka_cb).changes;
    AMfree(AMapplyChanges(test_state->doc2, &changes));
    AMfree(AMapplyChanges(doc3, &changes));
    AMfree(AMmapPutUint(doc3, AM_ROOT, "x", 1));
    AMcommit(doc3, NULL, &time);

    /*        - n1c1 <------ n1c2 <------ n1c3 <-- etc. <-- n1c20 <------ n1c21
     *       /          \/           \/                              \/
     *      /           /\           /\                              /\
     * c0 <---- n2c1 <------ n2c2 <------ n2c3 <-- etc. <-- n2c20 <------ n2c21
     *      \                                                          /
     *       ---------------------------------------------- n3c1 <-----
     */
    for (size_t value = 1; value != 20; ++value) {
        AMfree(AMmapPutUint(test_state->doc1, AM_ROOT, "n1", value));
        AMcommit(test_state->doc1, NULL, &time);
        AMfree(AMmapPutUint(test_state->doc2, AM_ROOT, "n2", value));
        AMcommit(test_state->doc2, NULL, &time);
        AMchanges changes1 = AMpush(&test_state->stack,
                                    AMgetLastLocalChange(test_state->doc1),
                                    AM_VALUE_CHANGES,
                                    cmocka_cb).changes;
        AMchanges changes2 = AMpush(&test_state->stack,
                                    AMgetLastLocalChange(test_state->doc2),
                                    AM_VALUE_CHANGES,
                                    cmocka_cb).changes;
        AMfree(AMapplyChanges(test_state->doc1, &changes2));
        AMfree(AMapplyChanges(test_state->doc2, &changes1));
    }

    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);

    /* Having n3's last change concurrent to the last sync heads forces us into
     * the slower code path. */
    AMchanges changes3 = AMpush(&test_state->stack,
                                AMgetLastLocalChange(doc3),
                                AM_VALUE_CHANGES,
                                cmocka_cb).changes;
    AMfree(AMapplyChanges(test_state->doc2, &changes3));
    AMfree(AMmapPutStr(test_state->doc1, AM_ROOT, "n1", "final"));
    AMcommit(test_state->doc1, NULL, &time);
    AMfree(AMmapPutStr(test_state->doc2, AM_ROOT, "n2", "final"));
    AMcommit(test_state->doc2, NULL, &time);

    sync(test_state->doc1,
         test_state->doc2,
         test_state->sync_state1,
         test_state->sync_state2);
    AMchangeHashes heads1 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc1),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    AMchangeHashes heads2 = AMpush(&test_state->stack,
                                   AMgetHeads(test_state->doc2),
                                   AM_VALUE_CHANGE_HASHES,
                                   cmocka_cb).change_hashes;
    assert_int_equal(AMchangeHashesCmp(&heads1, &heads2), 0);
    assert_true(AMequal(test_state->doc1, test_state->doc2));
}

int run_sync_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_converged_empty_local_doc_reply_no_local_data, setup, teardown),
        cmocka_unit_test_setup_teardown(test_converged_empty_local_doc_no_reply, setup, teardown),
        cmocka_unit_test_setup_teardown(test_converged_equal_heads_no_reply, setup, teardown),
        cmocka_unit_test_setup_teardown(test_converged_offer_all_changes_from_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_converged_sync_peers_with_uneven_commits, setup, teardown),
        cmocka_unit_test_setup_teardown(test_converged_works_with_prior_sync_state, setup, teardown),
        cmocka_unit_test_setup_teardown(test_converged_no_message_once_synced, setup, teardown),
        cmocka_unit_test_setup_teardown(test_converged_allow_simultaneous_messages, setup, teardown),
        cmocka_unit_test_setup_teardown(test_converged_assume_sent_changes_were_received, setup, teardown),
        cmocka_unit_test_setup_teardown(test_converged_works_regardless_of_who_initiates, setup, teardown),
        cmocka_unit_test_setup_teardown(test_diverged_works_without_prior_sync_state, setup, teardown),
        cmocka_unit_test_setup_teardown(test_diverged_works_with_prior_sync_state, setup, teardown),
        cmocka_unit_test_setup_teardown(test_diverged_ensure_not_empty_after_sync, setup, teardown),
        cmocka_unit_test_setup_teardown(test_diverged_resync_after_node_crash_with_data_loss, setup, teardown),
        cmocka_unit_test_setup_teardown(test_diverged_resync_after_data_loss_without_disconnection, setup, teardown),
        cmocka_unit_test_setup_teardown(test_diverged_handles_concurrent_changes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_diverged_handles_histories_of_branching_and_merging, setup, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
