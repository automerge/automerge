#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "automerge.h"

#define MAX_BUFF_SIZE 4096

int main() {
  int n = 0;
  int data_type = 0;
  char buff[MAX_BUFF_SIZE];
  char obj[MAX_BUFF_SIZE];
  AMresult* res = NULL;

  printf("begin\n");

  AMdoc* doc  = AMcreate();

  printf("AMconfig()...");
  AMconfig(doc, "actor", "aabbcc");
  printf("pass!\n");

  printf("AMmapSetStr()...\n");
  res = AMmapSetStr(doc, NULL, "string", "hello world");
  if (AMresultStatus(res) != AM_STATUS_COMMAND_OK)
  {
        printf("AMmapSet() failed: %s\n", AMerrorMessage(res));
        return 1;
  }
  AMclear(res);
  printf("pass!\n");

  AMdestroy(doc);
  printf("end\n");
}
