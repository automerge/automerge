
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "automerge.h"

/*
 * need to test: 
 *
  automerge_get_missing_deps
  automerge_error
*/

#define BUFSIZE 4096

int main() {
  int len;

  // In a real application you would need to check to make sure your buffer is large enough for any given read
  char buff[BUFSIZE];
  char buff2[BUFSIZE];
  char buff3[BUFSIZE];

  printf("begin\n");

  Backend * dbA = automerge_init();
  Backend * dbB = automerge_init();

  const char * requestA1 = R"({"actor":"111111","seq":1,"time":0,"version":0,"ops":[{"action":"set","obj":"00000000-0000-0000-0000-000000000000","key":"bird","value":"magpie"}]})";
  const char * requestA2 = R"({"actor":"111111","seq":2,"time":0,"version":1,"ops":[{"action":"set","obj":"00000000-0000-0000-0000-000000000000","key":"dog","value":"mastiff"}]})";
  const char * requestB1 = R"({"actor":"222222","seq":1,"time":0,"version":0,"ops":[{"action":"set","obj":"00000000-0000-0000-0000-000000000000","key":"bird","value":"crow"}]})";
  const char * requestB2 = R"({"actor":"222222","seq":2,"time":0,"version":1,"ops":[{"action":"set","obj":"00000000-0000-0000-0000-000000000000","key":"cat","value":"tabby"}]})";

  printf("*** requestA1 ***\n\n%s\n\n",requestA1);

  len = automerge_apply_local_change(dbA, requestA1);
  assert(len <= BUFSIZE);
  automerge_read_json(dbA, buff);
  printf("*** patchA1 ***\n\n%s\n\n",buff);

  len = automerge_apply_local_change(dbA, requestA2);
  assert(len <= BUFSIZE);
  automerge_read_json(dbA, buff);
  printf("*** patchA2 ***\n\n%s\n\n",buff);

  len = automerge_apply_local_change(dbB, requestB1);
  assert(len <= BUFSIZE);
  automerge_read_json(dbB, buff);
  printf("*** patchB1 ***\n\n%s\n\n",buff);

  len = automerge_apply_local_change(dbB, requestB2);
  assert(len <= BUFSIZE);
  automerge_read_json(dbB, buff);
  printf("*** patchB2 ***\n\n%s\n\n",buff);

  printf("*** clone dbA -> dbC ***\n\n");
  Backend * dbC = automerge_clone(dbA);

  len = automerge_get_patch(dbA);
  assert(len <= BUFSIZE);
  automerge_read_json(dbA, buff);
  len = automerge_get_patch(dbC);
  assert(len <= BUFSIZE);
  automerge_read_json(dbC, buff2);
  // the json can serialize in different orders so I can do a stright strcmp()
  printf("*** get_patch of dbA & dbC -- equal? *** --> %s\n\n",strlen(buff) == strlen(buff2) ? "true" : "false");
  assert(strlen(buff) == strlen(buff2));

  len = automerge_save(dbA);
  assert(len <= BUFSIZE);
  automerge_read_binary(dbA, buff2);
  printf("*** save dbA - %d bytes ***\n\n",len);

  printf("*** load the save into dbD ***\n\n");
  Backend * dbD = automerge_load(len, buff2);
  len = automerge_get_patch(dbD);
  assert(len <= BUFSIZE);
  automerge_read_json(dbD, buff2);
  printf("*** get_patch of dbA & dbD -- equal? *** --> %s\n\n",strlen(buff) == strlen(buff2) ? "true" : "false");
  assert(strlen(buff) == strlen(buff2));

  printf("*** copy changes from dbA to B ***\n\n");
  len = automerge_get_changes_for_actor(dbA,"111111");
  while (len > 0) {
    assert(len <= BUFSIZE);
    int nextlen = automerge_read_binary(dbA,buff);
    automerge_write_change(dbB,len,buff);

    // decode the change for debug
    // encode and decode could happen with either dbA or dbB,
    // however encode needs to be done against dbB instead of dbA
    // only because dbA is in the middle of iterating over some binary results
    // and needs to finish before queuing another
    automerge_decode_change(dbA,len,buff);
    automerge_read_json(dbA, buff2);
    printf("Change decoded to json -- %s\n",buff2);
    automerge_encode_change(dbB,buff2);
    automerge_read_binary(dbB,buff3);
    assert(memcmp(buff,buff3,len) == 0);

    len = nextlen;
  }
  automerge_apply_changes(dbB);


  printf("*** copy changes from dbB to A ***\n\n");
  len = automerge_get_changes_for_actor(dbB,"222222");
  while (len > 0) {
    assert(len <= BUFSIZE);
    int nextlen = automerge_read_binary(dbB,buff);
    automerge_write_change(dbA,len,buff);
    len = nextlen;
  }
  automerge_apply_changes(dbA);

  len = automerge_get_patch(dbA);
  assert(len <= BUFSIZE);
  automerge_read_json(dbA, buff);
  len = automerge_get_patch(dbB);
  assert(len <= BUFSIZE);
  automerge_read_json(dbB, buff2);
  printf("*** get_patch of dbA & dbB -- equal? *** --> %s\n\n",strlen(buff) == strlen(buff2) ? "true" : "false");
  assert(strlen(buff) == strlen(buff2));

  printf("*** copy changes from dbB to E using load ***\n\n");
  Backend * dbE = automerge_init();
  len = automerge_get_changes(dbA,0,NULL);
  while (len > 0) {
    assert(len <= BUFSIZE);
    int nextlen = automerge_read_binary(dbA,buff);
    automerge_write_change(dbE,len,buff);
    len = nextlen;
  }
  automerge_load_changes(dbE);

  len = automerge_get_patch(dbA);
  assert(len <= BUFSIZE);
  automerge_read_json(dbA, buff);
  len = automerge_get_patch(dbE);
  assert(len <= BUFSIZE);
  automerge_read_json(dbE, buff2);
  printf("*** get_patch of dbA & dbE -- equal? *** --> %s\n\n",strlen(buff) == strlen(buff2) ? "true" : "false");
  assert(strlen(buff) == strlen(buff2));

  len = automerge_get_missing_deps(dbE);
  automerge_read_json(dbE, buff); // [] - nothing missing
  assert(strlen(buff) == 2);

  printf("free resources\n");
  automerge_free(dbA);
  automerge_free(dbB);
  automerge_free(dbC);
  automerge_free(dbD);
  automerge_free(dbE);

  printf("end\n");
}
