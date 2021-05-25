
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "automerge.h"

#define BUFSIZE 4096

void test_sync_basic() {
  printf("begin sync test - basic\n");
  int ret;

  Buffers rbuffs  = automerge_create_buffs();
  Backend * dbA = automerge_init();
  Backend * dbB = automerge_init();

  SyncState * ssA = automerge_sync_state_init();
  SyncState * ssB = automerge_sync_state_init();

  ret = automerge_generate_sync_message(dbA, &rbuffs, ssA);
  assert(ret == 0);
  ret = automerge_receive_sync_message(dbB, &rbuffs, ssB, rbuffs.data, rbuffs.lens[0]);
  assert(ret == 0);
  ret = automerge_generate_sync_message(dbB, &rbuffs, ssB);
  assert(ret == 0);
  assert(rbuffs.lens_len == 0);

  automerge_sync_state_free(ssA);
  automerge_sync_state_free(ssB);
  automerge_free_buffs(&rbuffs);
}

void test_sync_encode_decode() {
  printf("begin sync test - encode/decode\n");
  int ret;

  char buff[BUFSIZE];
  char sync_state_buff[BUFSIZE];

  Buffers rbuffs  = automerge_create_buffs();
  Backend * dbA = automerge_init();
  Backend * dbB = automerge_init();
  SyncState * ssA = automerge_sync_state_init();
  SyncState * ssB = automerge_sync_state_init();

  const char * requestA1 = "{\"actor\":\"111111\",\"seq\":1,\"time\":0,\"deps\":[],\"startOp\":1,\"ops\":[{\"action\":\"set\",\"obj\":\"_root\",\"key\":\"bird\",\"value\":\"magpie\",\"pred\":[]}]}";
  const char * requestB1 = "{\"actor\":\"222222\",\"seq\":1,\"time\":0,\"deps\":[],\"startOp\":1,\"ops\":[{\"action\":\"set\",\"obj\":\"_root\",\"key\":\"bird\",\"value\":\"crow\",\"pred\":[]}]}";
  ret = automerge_apply_local_change(dbA, &rbuffs, requestA1);
  assert(ret == 0);
  ret = automerge_apply_local_change(dbB, &rbuffs, requestB1);
  assert(ret == 0);

  // A -> B
  ret = automerge_generate_sync_message(dbA, &rbuffs, ssA);
  assert(ret == 0);
  ret = automerge_receive_sync_message(dbB, &rbuffs, ssB, rbuffs.data, rbuffs.lens[0]);
  assert(ret == 0);

  // B -> A
  ret = automerge_generate_sync_message(dbB, &rbuffs, ssB);
  assert(ret == 0);
  ret = automerge_receive_sync_message(dbA, &rbuffs, ssA, rbuffs.data, rbuffs.lens[0]);
  assert(ret == 0);

  // A -> B
  ret = automerge_generate_sync_message(dbA, &rbuffs, ssA);
  assert(ret == 0);
  ret = automerge_receive_sync_message(dbB, &rbuffs, ssB, rbuffs.data, rbuffs.lens[0]);
  assert(ret == 0);

  // B -> A
  ret = automerge_generate_sync_message(dbB, &rbuffs, ssB);
  assert(ret == 0);
  ret = automerge_receive_sync_message(dbA, &rbuffs, ssA, rbuffs.data, rbuffs.lens[0]);
  assert(ret == 0);

  ret = automerge_generate_sync_message(dbA, &rbuffs, ssA);
  assert(ret == 0);

  // Save the sync state
  ret = automerge_encode_sync_state(dbB, &rbuffs, ssB);
  assert(ret == 0);
  // Read it back
  ret = automerge_decode_sync_state(dbB, rbuffs.data, rbuffs.lens[0], &ssB);
  assert(ret == 0);

  // Redo B -> A
  ret = automerge_generate_sync_message(dbB, &rbuffs, ssB);
  assert(ret == 0);
  ret = automerge_receive_sync_message(dbA, &rbuffs, ssA, rbuffs.data, rbuffs.lens[0]);
  assert(ret == 0);

  ret = automerge_generate_sync_message(dbA, &rbuffs, ssA);
  assert(ret == 0);
  assert(rbuffs.lens_len == 0);
}



int main() {
  int len;
  int ret;

  // In a real application you would need to check to make sure your buffer is large enough for any given read
  char buff[BUFSIZE];
  char buff2[BUFSIZE];
  char buff3[BUFSIZE];

  printf("begin\n");

  Buffers rbuffs  = automerge_create_buffs();
  Backend * dbA = automerge_init();
  Backend * dbB = automerge_init();

  const char * requestA1 = "{\"actor\":\"111111\",\"seq\":1,\"time\":0,\"deps\":[],\"startOp\":1,\"ops\":[{\"action\":\"set\",\"obj\":\"_root\",\"key\":\"bird\",\"value\":\"magpie\",\"pred\":[]}]}";
  const char * requestA2 = "{\"actor\":\"111111\",\"seq\":2,\"time\":0,\"deps\":[],\"startOp\":2,\"ops\":[{\"action\":\"set\",\"obj\":\"_root\",\"key\":\"dog\",\"value\":\"mastiff\",\"pred\":[]}]}";
  const char * requestB1 = "{\"actor\":\"222222\",\"seq\":1,\"time\":0,\"deps\":[],\"startOp\":1,\"ops\":[{\"action\":\"set\",\"obj\":\"_root\",\"key\":\"bird\",\"value\":\"crow\",\"pred\":[]}]}";
  const char * requestB2 = "{\"actor\":\"222222\",\"seq\":2,\"time\":0,\"deps\":[],\"startOp\":2,\"ops\":[{\"action\":\"set\",\"obj\":\"_root\",\"key\":\"cat\",\"value\":\"tabby\",\"pred\":[]}]}";

  printf("*** requestA1 ***\n\n%s\n\n",requestA1);

  ret = automerge_apply_local_change(dbA, &rbuffs, requestA1);
  assert(ret == 0);
  // 0th buff = the binary change, 1st buff = patch as JSON
  util_read_buffs_str(&rbuffs, 1, buff);
  printf("*** patchA1 ***\n\n%s\n\n",buff);

  ret = automerge_apply_local_change(dbA, &rbuffs, "{}");
  assert(ret == -6);
  printf("*** patchA2 expected error string ** (%s)\n\n",automerge_error(dbA));

  ret = automerge_apply_local_change(dbA, &rbuffs, requestA2);
  assert(ret == 0);
  util_read_buffs_str(&rbuffs, 1, buff);
  printf("*** patchA2 ***\n\n%s\n\n",buff);

  ret = automerge_apply_local_change(dbB, &rbuffs, requestB1);
  assert(ret == 0);
  util_read_buffs_str(&rbuffs, 1, buff);
  printf("*** patchB1 ***\n\n%s\n\n",buff);

  ret = automerge_apply_local_change(dbB, &rbuffs, requestB2);
  assert(ret == 0);
  util_read_buffs_str(&rbuffs, 1, buff);
  printf("*** patchB2 ***\n\n%s\n\n",buff);

  printf("*** clone dbA -> dbC ***\n\n");
  Backend * dbC = NULL;
  ret = automerge_clone(dbA, &dbC);
  assert(ret == 0);

  ret = automerge_get_patch(dbA, &rbuffs);
  assert(ret == 0);
  util_read_buffs_str(&rbuffs, 0, buff);
  ret = automerge_get_patch(dbC, &rbuffs);
  assert(ret == 0);
  util_read_buffs_str(&rbuffs, 0, buff2);
  // the json can serialize in different orders so I can't do a straight strcmp()
  printf("*** get_patch of dbA & dbC -- equal? *** --> %s\n\n",strlen(buff) == strlen(buff2) ? "true" : "false");
  assert(strlen(buff) == strlen(buff2));

  ret = automerge_save(dbA, &rbuffs);
  assert(ret == 0);
  util_read_buffs(&rbuffs, 0, buff2);
  printf("*** save dbA - %ld bytes ***\n\n", rbuffs.lens[0]);

  printf("*** load the save into dbD ***\n\n");
  Backend * dbD = automerge_load(buff2, rbuffs.lens[0]);
  ret = automerge_get_patch(dbD, &rbuffs);
  assert(ret == 0);
  util_read_buffs_str(&rbuffs, 0, buff2);
  printf("*** get_patch of dbA & dbD -- equal? *** --> %s\n\n",strlen(buff) == strlen(buff2) ? "true" : "false");
  assert(strlen(buff) == strlen(buff2));

  ret = automerge_get_changes_for_actor(dbA, &rbuffs, "111111");
  assert(ret == 0);

  // We are reading one return value (rbuffs) while needing to return
  // something else, so we need another `Buffers` struct
  Buffers rbuffs2  = automerge_create_buffs();
  int start = 0;
  for(int i = 0; i < rbuffs.lens_len; ++i) {
      int len = rbuffs.lens[i];
      char * data_start = rbuffs.data + start;
      automerge_decode_change(dbA, &rbuffs2, data_start, len);
      util_read_buffs_str(&rbuffs2, 0, buff2);
      printf("Change decoded to json -- %s\n",buff2);
      start += len;
      automerge_encode_change(dbB, &rbuffs2, buff2);
      assert(memcmp(data_start, rbuffs2.data, len) == 0);
  }
  CBuffers cbuffs = { data: rbuffs.data, data_len: rbuffs.data_len, lens: rbuffs.lens, lens_len: rbuffs.lens_len };
  ret = automerge_apply_changes(dbB, &rbuffs, &cbuffs);
  assert(ret == 0);
  automerge_free_buffs(&rbuffs2);

  printf("*** get head from dbB ***\n\n");
  ret = automerge_get_heads(dbB, &rbuffs);
  assert(ret == 0);

  int num_heads = 0;
  for (int i = 0; i < rbuffs.lens_len; ++i) {
      assert(rbuffs.lens[i] == 32);
      util_read_buffs(&rbuffs, i, buff3 + (num_heads * 32));
      num_heads++;
  }
  assert(num_heads == 2);
  ret = automerge_get_changes(dbB, &rbuffs, buff3, num_heads);
  assert(ret == 0);

  printf("*** copy changes from dbB to A ***\n\n");
  ret = automerge_get_changes_for_actor(dbB, &rbuffs, "222222");
  assert(ret == 0);

  CBuffers cbuffs2 = { data: rbuffs.data, data_len: rbuffs.data_len, lens: rbuffs.lens, lens_len: rbuffs.lens_len };
  ret = automerge_apply_changes(dbA, &rbuffs, &cbuffs2);
  assert(ret == 0);

  ret = automerge_get_patch(dbA, &rbuffs);
  assert(ret == 0);
  util_read_buffs_str(&rbuffs, 0, buff);
  ret = automerge_get_patch(dbB, &rbuffs);
  assert(ret == 0);
  util_read_buffs_str(&rbuffs, 0, buff2);
  // the json can serialize in different orders so I can't do a straight strcmp()
  printf("*** get_patch of dbA & dbB -- equal? *** --> %s\n\n",strlen(buff) == strlen(buff2) ? "true" : "false");
  assert(strlen(buff) == strlen(buff2));

  printf("*** copy changes from dbA to E using load ***\n\n");
  Backend * dbE = automerge_init();
  ret = automerge_get_changes(dbA, &rbuffs, NULL, 0);
  assert(ret == 0);
  CBuffers cbuffs3 = { data: rbuffs.data, data_len: rbuffs.data_len, lens: rbuffs.lens, lens_len: rbuffs.lens_len };
  ret = automerge_load_changes(dbE, &cbuffs3);
  assert(ret == 0);

  ret = automerge_get_patch(dbA, &rbuffs);
  assert(ret == 0);
  util_read_buffs_str(&rbuffs, 0, buff);
  ret = automerge_get_patch(dbE, &rbuffs);
  assert(ret == 0);
  util_read_buffs_str(&rbuffs, 0, buff2);
  // the json can serialize in different orders so I can't do a straight strcmp()
  printf("*** get_patch of dbA & dbE -- equal? *** --> %s\n\n",strlen(buff) == strlen(buff2) ? "true" : "false");
  assert(strlen(buff) == strlen(buff2));

  ret = automerge_get_patch(dbA, &rbuffs);
  assert(ret == 0);
  util_read_buffs_str(&rbuffs, 0, buff);
  ret = automerge_get_patch(dbB, &rbuffs);
  assert(ret == 0);
  util_read_buffs_str(&rbuffs, 0, buff2);
  // the json can serialize in different orders so I can't do a straight strcmp()
  printf("*** get_patch of dbA & dbB -- equal? *** --> %s\n\n",strlen(buff) == strlen(buff2) ? "true" : "false");
  assert(strlen(buff) == strlen(buff2));

  ret = automerge_get_missing_deps(dbE, &rbuffs, buff3, num_heads);
  assert(ret == 0);
  util_read_buffs_str(&rbuffs, 0, buff);
  assert(strlen(buff) == 2); // [] - nothing missing

  test_sync_basic();
  test_sync_encode_decode();

  printf("free resources\n");
  automerge_free(dbA);
  automerge_free(dbB);
  automerge_free(dbC);
  automerge_free(dbD);
  automerge_free(dbE);
  automerge_free_buffs(&rbuffs);

  printf("end\n");
}
