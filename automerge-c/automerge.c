#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "automerge.h"

int main() {

  //char buff2[BUFSIZE];
  //char buff3[BUFSIZE];

  printf("begin\n");

  Automerge * doc  = automerge_create();

  automerge_set_map(doc, &ROOT, "string", AM_TYPE_STR, "hello world");

  automerge_free(doc);

  printf("end\n");
}
