
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "automerge.h"

#define BUFSIZE 4096
#define CMP_PATCH(x, y) \
    do { \
        char _buff[BUFSIZE]; \
        char _buff2[BUFSIZE]; \
        Buffer *_rbuff  = automerge_create_buff(); \
        int ret = automerge_get_patch(x, _rbuff); \
        int len1 = _rbuff->len; \
        ASSERT_RET(x, 0); \
        ret = automerge_get_patch(y, _rbuff); \
        int len2 = _rbuff->len; \
        ASSERT_RET(y, 0); \
        printf("*** get_patch of " #x " & " #y " -- (likely) equal? *** --> %s\n\n", len1 == len2 ? "true": "false"); \
        assert(len1 == len2); \
        automerge_free_buff(_rbuff); \
    } while (0)

// Probably shouldn't use implicit declaration of `ret`...
#define ASSERT_RET(db, expected) \
    do { \
        if (ret != expected) { \
            printf("LINE: %d, expected ret to be: %d, but it was: %d. Error: %s\n", __LINE__, expected, ret, automerge_error(db)); \
            assert(ret == expected); \
        } \
    } while(0)

#define SEND_MSG(x, y) \
    do { \
        ret = automerge_generate_sync_message(db ## x, rbuff, ss ## x); \
        ASSERT_RET(db ## x, 0); \
        ret = automerge_receive_sync_message(db ## y, rbuff, ss ## y, rbuff->data, rbuff->len); \
        ASSERT_RET(db ## y, 0); \
    } while (0)

void test_sync_basic() {
  printf("begin sync test - basic\n");
  int ret;

  Buffer  * rbuff  = automerge_create_buff();
  Backend * dbA = automerge_init();
  Backend * dbB = automerge_init();

  SyncState * ssA = automerge_sync_state_init();
  SyncState * ssB = automerge_sync_state_init();

  ret = automerge_generate_sync_message(dbA, rbuff, ssA);
  ASSERT_RET(dbA, 0);
  ret = automerge_receive_sync_message(dbB, rbuff, ssB, rbuff->data, rbuff->len);
  ASSERT_RET(dbB, 0);

  ret = automerge_generate_sync_message(dbB, rbuff, ssB);
  ASSERT_RET(dbB, 0);
  assert(rbuff->len == 0);

  automerge_sync_state_free(ssA);
  automerge_sync_state_free(ssB);
  automerge_free_buff(rbuff);
}

void test_sync_encode_decode() {
  printf("begin sync test - encode/decode\n");
  int ret;

  char buff[BUFSIZE];
  char sync_state_buff[BUFSIZE];

  Buffer *rbuff  = automerge_create_buff();
  Backend * dbA = automerge_init();
  Backend * dbB = automerge_init();
  SyncState * ssA = automerge_sync_state_init();
  SyncState * ssB = automerge_sync_state_init();

  const char * requestA1 = "{\"actor\":\"111111\",\"seq\":1,\"time\":0,\"deps\":[],\"startOp\":1,\"ops\":[{\"action\":\"set\",\"obj\":\"_root\",\"key\":\"bird\",\"value\":\"magpie\",\"pred\":[]}]}";
  const char * requestB1 = "{\"actor\":\"222222\",\"seq\":1,\"time\":0,\"deps\":[],\"startOp\":1,\"ops\":[{\"action\":\"set\",\"obj\":\"_root\",\"key\":\"bird\",\"value\":\"crow\",\"pred\":[]}]}";

  unsigned char * A1msgpack = NULL;
  unsigned char * B1msgpack = NULL;
  uintptr_t A1msgpack_len = 0;
  uintptr_t B1msgpack_len = 0;

  debug_json_change_to_msgpack(requestA1, &A1msgpack, &A1msgpack_len);
  debug_json_change_to_msgpack(requestB1, &B1msgpack, &B1msgpack_len);

  ret = automerge_apply_local_change(dbA, rbuff, A1msgpack, A1msgpack_len);
  ASSERT_RET(dbA, 0);
  ret = automerge_apply_local_change(dbB, rbuff, B1msgpack, B1msgpack_len);
  ASSERT_RET(dbB, 0);

  // A -> B
  SEND_MSG(A, B);

  // B -> A
  SEND_MSG(B, A);

  // A -> B
  SEND_MSG(A, B);

  // B -> A
  SEND_MSG(B, A);

  ret = automerge_generate_sync_message(dbA, rbuff, ssA);
  ASSERT_RET(dbA, 0);

  // Save the sync state
  ret = automerge_encode_sync_state(dbB, rbuff, ssB);
  ASSERT_RET(dbB, 0);
  // Read it back
  ret = automerge_decode_sync_state(dbB, rbuff->data, rbuff->len, &ssB);
  ASSERT_RET(dbB, 0);

  // Redo B -> A
  SEND_MSG(B, A);

  ret = automerge_generate_sync_message(dbA, rbuff, ssA);
  ASSERT_RET(dbA, 0);
  assert(rbuff->len == 0);
}

int main() {
  int ret;

  // In a real application you would need to check to make sure your buffer is large enough for any given read
  char buff[BUFSIZE];
  char buff2[BUFSIZE];
  char buff3[BUFSIZE];

  printf("begin\n");

  Buffer  * rbuff  = automerge_create_buff();
  Backend * dbA = automerge_init();
  Backend * dbB = automerge_init();

  const char * requestA1 = "{\"actor\":\"111111\",\"seq\":1,\"time\":0,\"deps\":[],\"startOp\":1,\"ops\":[{\"action\":\"set\",\"obj\":\"_root\",\"key\":\"bird\",\"value\":\"magpie\",\"pred\":[]}]}";
  const char * requestA2 = "{\"actor\":\"111111\",\"seq\":2,\"time\":0,\"deps\":[],\"startOp\":2,\"ops\":[{\"action\":\"set\",\"obj\":\"_root\",\"key\":\"dog\",\"value\":\"mastiff\",\"pred\":[]}]}";
  const char * requestB1 = "{\"actor\":\"222222\",\"seq\":1,\"time\":0,\"deps\":[],\"startOp\":1,\"ops\":[{\"action\":\"set\",\"obj\":\"_root\",\"key\":\"bird\",\"value\":\"crow\",\"pred\":[]}]}";
  const char * requestB2 = "{\"actor\":\"222222\",\"seq\":2,\"time\":0,\"deps\":[],\"startOp\":2,\"ops\":[{\"action\":\"set\",\"obj\":\"_root\",\"key\":\"cat\",\"value\":\"tabby\",\"pred\":[]}]}";

  unsigned char * A1msgpack = NULL;
  unsigned char * A2msgpack = NULL;
  unsigned char * B1msgpack = NULL;
  unsigned char * B2msgpack = NULL;
  uintptr_t A1msgpack_len = 0;
  uintptr_t A2msgpack_len = 0;
  uintptr_t B1msgpack_len = 0;
  uintptr_t B2msgpack_len = 0;

  debug_json_change_to_msgpack(requestA1, &A1msgpack, &A1msgpack_len);
  debug_json_change_to_msgpack(requestA2, &A2msgpack, &A2msgpack_len);
  debug_json_change_to_msgpack(requestB1, &B1msgpack, &B1msgpack_len);
  debug_json_change_to_msgpack(requestB2, &B2msgpack, &B2msgpack_len);

  ret = automerge_apply_local_change(dbA, rbuff, A1msgpack, A1msgpack_len);
  ASSERT_RET(dbA, 0);
  printf("cap: %ld, len: %ld, ptr: %p\n",rbuff->cap, rbuff->len, rbuff->data);
  debug_print_msgpack_patch("*** patchA1 ***", rbuff->data, rbuff->len);

  // TODO: Port this test to msgpack
  // ret = automerge_apply_local_change(dbA, rbuff, "{}");
  // ASSERT_RET(dbA, 0);
  // printf("*** patchA2 expected error string ** (%s)\n\n",automerge_error(dbA));

  ret = automerge_apply_local_change(dbA, rbuff, A2msgpack, A2msgpack_len);
  ASSERT_RET(dbA, 0);
  debug_print_msgpack_patch("*** patchA2 ***", rbuff->data, rbuff->len);

  ret = automerge_apply_local_change(dbB, rbuff, B1msgpack, B1msgpack_len);
  ASSERT_RET(dbB, 0);
  debug_print_msgpack_patch("*** patchB1 ***", rbuff->data, rbuff->len);

  ret = automerge_apply_local_change(dbB, rbuff, B2msgpack, B2msgpack_len);
  ASSERT_RET(dbB, 0);
  debug_print_msgpack_patch("*** patchB2 ***", rbuff->data, rbuff->len);

  printf("*** clone dbA -> dbC ***\n\n");
  Backend * dbC = NULL;
  ret = automerge_clone(dbA, &dbC);
  ASSERT_RET(dbA, 0);

  CMP_PATCH(dbA, dbC);

  ret = automerge_save(dbA, rbuff);
  ASSERT_RET(dbA, 0);
  printf("*** save dbA - %ld bytes ***\n\n", rbuff->len);

  printf("*** load the save into dbD ***\n\n");
  Backend * dbD = automerge_load(rbuff->data, rbuff->len);

  CMP_PATCH(dbA, dbD);

  ret = automerge_get_changes_for_actor(dbA, rbuff, "111111");
  ASSERT_RET(dbA, 0);

  // We are reading one return value (rbuff) while needing to return
  // something else, so we need another `Buffers` struct
  // Buffers rbuff2  = automerge_create_buffs();
  // int start = 0;
  // for(int i = 0; i < rbuff->lens_len; ++i) {
  //     int len = rbuff->lens[i];
  //     char * data_start = rbuff->data + start;
  //     automerge_decode_change(dbA, rbuff2, data_start, len);
  //     util_read_buffs(rbuff2, 0, buff2);
  //     printf("Change decoded to msgpack\n");
  //     start += len;
  //     automerge_encode_change(dbB, &rbuff2, buff2, rbuff2.lens[0]);
  //     assert(memcmp(data_start, rbuff2.data, len) == 0);
  // }
  // CBuffers cbuffs = { data: rbuff->data, data_len: rbuff->data_len, lens: rbuff->lens, lens_len: rbuff->lens_len };
  // ret = automerge_apply_changes(dbB, &rbuff, cbuffs);
  // ASSERT_RET(dbB, 0);
  // automerge_free_buffs(&rbuff2);

  ret = automerge_apply_changes(dbB, rbuff, rbuff->data, rbuff->len);
  ASSERT_RET(dbB, 0);

  printf("*** get head from dbB ***\n\n");
  ret = automerge_get_heads(dbB, rbuff);
  ASSERT_RET(dbB,0);

  //int num_heads = 0;
  //for (int i = 0; i < rbuff->lens_len; ++i) {
  //    assert(rbuff->lens[i] == 32);
  //    util_read_buffs(&rbuff, i, buff3 + (num_heads * 32));
  //    num_heads++;
  //}
  //assert(num_heads == 2);
  ret = automerge_get_changes(dbB, rbuff, rbuff->data, rbuff->len);
  ASSERT_RET(dbB, 0);

  printf("*** copy changes from dbB to A ***\n\n");
  ret = automerge_get_changes_for_actor(dbB, rbuff, "222222");
  ASSERT_RET(dbB, 0);

  ret = automerge_apply_changes(dbA, rbuff, rbuff->data, rbuff->len);
  ASSERT_RET(dbA, 0);

  CMP_PATCH(dbA, dbB);

  printf("*** copy changes from dbA to E using load ***\n\n");
  Backend * dbE = automerge_init();
  ret = automerge_get_changes(dbA, rbuff, NULL, 0);
  ASSERT_RET(dbA, 0);
  ret = automerge_load_changes(dbE, rbuff->data, rbuff->len);
  ASSERT_RET(dbE, 0);

  CMP_PATCH(dbA, dbE);
  CMP_PATCH(dbA, dbB);

  //ret = automerge_get_missing_deps(dbE, &rbuff, buff3, num_heads);
  //ASSERT_RET(dbE, 0);
  //util_read_buffs(&rbuff, 0, buff);
  //assert(strlen(buff) == 2); // [] - nothing missing

  test_sync_basic();
  test_sync_encode_decode();

  printf("free resources\n");
  automerge_free(dbA);
  automerge_free(dbB);
  automerge_free(dbC);
  automerge_free(dbD);
  automerge_free(dbE);
  automerge_free_buff(rbuff);

  printf("end\n");
}
