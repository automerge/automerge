#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* third-party */
#include <cmocka.h>

/* local */
#include <automerge-c/automerge.h>
#include <automerge-c/config.h>
#include <automerge-c/utils/stack_callback_data.h>
#include "base_state.h"
#include "cmocka_utils.h"
#include "doc_state.h"
#include "str_utils.h"

typedef struct {
    DocState* doc_state;
    AMbyteSpan actor_id_str;
    uint8_t* actor_id_bytes;
    size_t actor_id_size;
} TestState;

static int setup(void** state) {
    TestState* test_state = test_calloc(1, sizeof(TestState));
    setup_doc((void**)&test_state->doc_state);
    test_state->actor_id_str.src = "000102030405060708090a0b0c0d0e0f";
    test_state->actor_id_str.count = strlen(test_state->actor_id_str.src);
    test_state->actor_id_size = test_state->actor_id_str.count / 2;
    test_state->actor_id_bytes = test_malloc(test_state->actor_id_size);
    hex_to_bytes(test_state->actor_id_str.src, test_state->actor_id_bytes, test_state->actor_id_size);
    *state = test_state;
    return 0;
}

static int teardown(void** state) {
    TestState* test_state = *state;
    teardown_doc((void**)&test_state->doc_state);
    test_free(test_state->actor_id_bytes);
    test_free(test_state);
    return 0;
}

static void test_AMkeys_empty(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    AMitems forward = AMstackItems(stack_ptr, AMkeys(doc, AM_ROOT, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    assert_int_equal(AMitemsSize(&forward), 0);
    AMitems reverse = AMitemsReversed(&forward);
    assert_int_equal(AMitemsSize(&reverse), 0);
    assert_null(AMitemsNext(&forward, 1));
    assert_null(AMitemsPrev(&forward, 1));
    assert_null(AMitemsNext(&reverse, 1));
    assert_null(AMitemsPrev(&reverse, 1));
}

static void test_AMkeys_list(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    AMobjId const* const list =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("list"), AM_OBJ_TYPE_LIST), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    AMstackItem(NULL, AMlistPutInt(doc, list, 0, true, 0), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMlistPutInt(doc, list, 1, true, 0), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMlistPutInt(doc, list, 2, true, 0), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMitems forward = AMstackItems(stack_ptr, AMkeys(doc, list, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_int_equal(AMitemsSize(&forward), 3);
    AMitems reverse = AMitemsReversed(&forward);
    assert_int_equal(AMitemsSize(&reverse), 3);
    /* Forward iterator forward. */
    AMbyteSpan str;
    assert_true(AMitemToStr(AMitemsNext(&forward, 1), &str));
    assert_ptr_equal(strstr(str.src, "2@"), str.src);
    assert_true(AMitemToStr(AMitemsNext(&forward, 1), &str));
    assert_ptr_equal(strstr(str.src, "3@"), str.src);
    assert_true(AMitemToStr(AMitemsNext(&forward, 1), &str));
    assert_ptr_equal(strstr(str.src, "4@"), str.src);
    assert_null(AMitemsNext(&forward, 1));
    // /* Forward iterator reverse. */
    assert_true(AMitemToStr(AMitemsPrev(&forward, 1), &str));
    assert_ptr_equal(strstr(str.src, "4@"), str.src);
    assert_true(AMitemToStr(AMitemsPrev(&forward, 1), &str));
    assert_ptr_equal(strstr(str.src, "3@"), str.src);
    assert_true(AMitemToStr(AMitemsPrev(&forward, 1), &str));
    assert_ptr_equal(strstr(str.src, "2@"), str.src);
    assert_null(AMitemsPrev(&forward, 1));
    /* Reverse iterator forward. */
    assert_true(AMitemToStr(AMitemsNext(&reverse, 1), &str));
    assert_ptr_equal(strstr(str.src, "4@"), str.src);
    assert_true(AMitemToStr(AMitemsNext(&reverse, 1), &str));
    assert_ptr_equal(strstr(str.src, "3@"), str.src);
    assert_true(AMitemToStr(AMitemsNext(&reverse, 1), &str));
    assert_ptr_equal(strstr(str.src, "2@"), str.src);
    assert_null(AMitemsNext(&reverse, 1));
    /* Reverse iterator reverse. */
    assert_true(AMitemToStr(AMitemsPrev(&reverse, 1), &str));
    assert_ptr_equal(strstr(str.src, "2@"), str.src);
    assert_true(AMitemToStr(AMitemsPrev(&reverse, 1), &str));
    assert_ptr_equal(strstr(str.src, "3@"), str.src);
    assert_true(AMitemToStr(AMitemsPrev(&reverse, 1), &str));
    assert_ptr_equal(strstr(str.src, "4@"), str.src);
    assert_null(AMitemsPrev(&reverse, 1));
}

static void test_AMkeys_map(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    AMstackItem(NULL, AMmapPutInt(doc, AM_ROOT, AMstr("one"), 1), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutInt(doc, AM_ROOT, AMstr("two"), 2), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMmapPutInt(doc, AM_ROOT, AMstr("three"), 3), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMitems forward = AMstackItems(stack_ptr, AMkeys(doc, AM_ROOT, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));
    assert_int_equal(AMitemsSize(&forward), 3);
    AMitems reverse = AMitemsReversed(&forward);
    assert_int_equal(AMitemsSize(&reverse), 3);
    /* Forward iterator forward. */
    AMbyteSpan str;
    assert_true(AMitemToStr(AMitemsNext(&forward, 1), &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "one", str.count);
    assert_true(AMitemToStr(AMitemsNext(&forward, 1), &str));
    assert_int_equal(str.count, 5);
    assert_memory_equal(str.src, "three", str.count);
    assert_true(AMitemToStr(AMitemsNext(&forward, 1), &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "two", str.count);
    assert_null(AMitemsNext(&forward, 1));
    /* Forward iterator reverse. */
    assert_true(AMitemToStr(AMitemsPrev(&forward, 1), &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "two", str.count);
    assert_true(AMitemToStr(AMitemsPrev(&forward, 1), &str));
    assert_int_equal(str.count, 5);
    assert_memory_equal(str.src, "three", str.count);
    assert_true(AMitemToStr(AMitemsPrev(&forward, 1), &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "one", str.count);
    assert_null(AMitemsPrev(&forward, 1));
    /* Reverse iterator forward. */
    assert_true(AMitemToStr(AMitemsNext(&reverse, 1), &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "two", str.count);
    assert_true(AMitemToStr(AMitemsNext(&reverse, 1), &str));
    assert_int_equal(str.count, 5);
    assert_memory_equal(str.src, "three", str.count);
    assert_true(AMitemToStr(AMitemsNext(&reverse, 1), &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "one", str.count);
    assert_null(AMitemsNext(&reverse, 1));
    /* Reverse iterator reverse. */
    assert_true(AMitemToStr(AMitemsPrev(&reverse, 1), &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "one", str.count);
    assert_true(AMitemToStr(AMitemsPrev(&reverse, 1), &str));
    assert_int_equal(str.count, 5);
    assert_memory_equal(str.src, "three", str.count);
    assert_true(AMitemToStr(AMitemsPrev(&reverse, 1), &str));
    assert_int_equal(str.count, 3);
    assert_memory_equal(str.src, "two", str.count);
    assert_null(AMitemsPrev(&reverse, 1));
}

static void test_AMload(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    FILE* fp = fopen("files/brave-ape-49.automerge", "rb");
    assert_non_null(fp);
    fseek(fp, 0L, SEEK_END);
    size_t const count = ftell(fp);
    assert_int_not_equal(count, 0);
    rewind(fp);
    uint8_t* src = test_calloc(count, sizeof(uint8_t));
    for (size_t i = 0; i != count; ++i) {
        src[i] = fgetc(fp);
    }
    assert_int_equal(ferror(fp), 0);
    assert_int_equal(fclose(fp), 0);
    AMitem* doc_item = AMstackItem(stack_ptr, AMload(src, count), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC));
    test_free(src);
    assert_non_null(doc_item);
    AMdoc* doc = NULL;
    assert_true(AMitemToDoc(doc_item, &doc));
    assert_non_null(doc);
    AMitems keys = AMstackItems(stack_ptr, AMkeys(doc, AM_ROOT, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR));

    enum Key {ASSETS, DATA, DOM, META, SIZE_};

    assert_int_equal(AMitemsSize(&keys), SIZE_);
    size_t match_counts[SIZE_] = {0};
    AMitem* key_item;
    while ((key_item = AMitemsNext(&keys, 1)) != NULL) {
        AMbyteSpan str;
        assert_true(AMitemToStr(key_item, &str));
        if (!strncmp(str.src, "assets", str.count)) {
            match_counts[ASSETS] += 1;
        } else if (!strncmp(str.src, "data", str.count)) {
            match_counts[DATA] += 1;
        } else if (!strncmp(str.src, "dom", str.count)) {
            match_counts[DOM] += 1;
        } else if (!strncmp(str.src, "meta", str.count)) {
            match_counts[META] += 1;
        }
    }
    assert_int_equal(match_counts[ASSETS], 1);
    assert_int_equal(match_counts[DATA], 1);
    assert_int_equal(match_counts[DOM], 1);
    assert_int_equal(match_counts[META], 1);
}

static void test_AMputActor_bytes(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromBytes(test_state->actor_id_bytes, test_state->actor_id_size), cmocka_cb,
                    AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMstackItem(NULL, AMsetActorId(test_state->doc_state->doc, actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMgetActorId(test_state->doc_state->doc), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMbyteSpan const bytes = AMactorIdBytes(actor_id);
    assert_int_equal(bytes.count, test_state->actor_id_size);
    assert_memory_equal(bytes.src, test_state->actor_id_bytes, bytes.count);
}

static void test_AMputActor_str(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    AMactorId const* actor_id;
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMactorIdFromStr(test_state->actor_id_str), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMstackItem(NULL, AMsetActorId(test_state->doc_state->doc, actor_id), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    assert_true(AMitemToActorId(
        AMstackItem(stack_ptr, AMgetActorId(test_state->doc_state->doc), cmocka_cb, AMexpect(AM_VAL_TYPE_ACTOR_ID)),
        &actor_id));
    AMbyteSpan const str = AMactorIdStr(actor_id);
    assert_int_equal(str.count, test_state->actor_id_str.count);
    assert_memory_equal(str.src, test_state->actor_id_str.src, str.count);
}

#define assert_str_equal(actual, expected) \
    assert_int_equal(actual.count, strlen(expected)); \
    assert_memory_equal(actual.src, expected, actual.count);

static void test_AMspliceText(void** state) {
    TestState* test_state = *state;
    AMstack** stack_ptr = &test_state->doc_state->base_state->stack;
    AMdoc* doc;
    assert_true(AMitemToDoc(AMstackItem(stack_ptr, AMcreate(NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_DOC)), &doc));
    AMobjId const* const text =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("text"), AM_OBJ_TYPE_TEXT), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    AMstackItem(NULL, AMspliceText(doc, text, 0, 0, AMstr("one + ")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMspliceText(doc, text, 4, 2, AMstr("two = ")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMstackItem(NULL, AMspliceText(doc, text, 8, 2, AMstr("three")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    AMbyteSpan str;
    assert_true(
        AMitemToStr(AMstackItem(stack_ptr, AMtext(doc, text, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_int_equal(str.count, strlen("one two three"));
    assert_memory_equal(str.src, "one two three", str.count);

    AMobjId const* const unicode =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("unicode"), AM_OBJ_TYPE_TEXT), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    AMstackItem(NULL, AMspliceText(doc, unicode, 0, 0, AMstr("🇬🇧🇩🇪")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

#if defined(AUTOMERGE_C_UTF8)

    AMstackItem(NULL, AMspliceText(doc, unicode, 8, 4, AMstr("🇦")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    assert_int_equal(AMobjSize(doc, unicode, NULL), 16);

    assert_true(
        AMitemToStr(AMstackItem(stack_ptr, AMtext(doc, unicode, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_str_equal(str, "🇬🇧🇦🇪");

    AMobjId const* const edge =
        AMitemObjId(AMstackItem(stack_ptr, AMmapPutObject(doc, AM_ROOT, AMstr("edge"), AM_OBJ_TYPE_TEXT), cmocka_cb,
                                AMexpect(AM_VAL_TYPE_OBJ_TYPE)));
    AMstackItem(NULL, AMspliceText(doc, edge, 0, 0, AMstr("🇬🇧")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));
    // it should delete the whole character instead of partial characters
    AMstackItem(NULL, AMspliceText(doc, edge, 4, 1, AMstr("")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    assert_true(
        AMitemToStr(AMstackItem(stack_ptr, AMtext(doc, edge, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_str_equal(str, "🇬");

    // it should insert at the character boundary
    AMstackItem(NULL, AMspliceText(doc, edge, 2, 0, AMstr("🇵")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    assert_true(
        AMitemToStr(AMstackItem(stack_ptr, AMtext(doc, edge, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_str_equal(str, "🇬🇵");

#elif defined(AUTOMERGE_C_UTF32)

    AMstackItem(NULL, AMspliceText(doc, unicode, 2, 1, AMstr("🇦")), cmocka_cb, AMexpect(AM_VAL_TYPE_VOID));

    assert_int_equal(AMobjSize(doc, unicode, NULL), 4);

    assert_true(
        AMitemToStr(AMstackItem(stack_ptr, AMtext(doc, unicode, NULL), cmocka_cb, AMexpect(AM_VAL_TYPE_STR)), &str));
    assert_str_equal(str, "🇬🇧🇦🇪");

#else

    print_error("%s", "Neither `AUTOMERGE_C_UTF8` nor `AUTOMERGE_C_UTF32` are defined.");

#endif
}

int run_doc_tests(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_AMkeys_empty, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMkeys_list, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMkeys_map, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMload, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMputActor_bytes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMputActor_str, setup, teardown),
        cmocka_unit_test_setup_teardown(test_AMspliceText, setup, teardown),
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
