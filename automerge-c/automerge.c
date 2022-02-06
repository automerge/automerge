#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "automerge.h"

#define MAX_BUFF_SIZE 4096

int main() {
  int n = 0;
  int datatype = 0;
  char buff[MAX_BUFF_SIZE];
  char obj[MAX_BUFF_SIZE];

  printf("begin\n");

  Automerge * doc  = am_create();

  printf("am_set_actor_hex()...");
  am_set_actor_hex(doc, "aabbcc");
  printf("pass!\n");

  printf("testing str...");

  printf("am_map_set()...");
  n = am_map_set(doc, AM_ROOT, "string", Datatype_Str, "hello world");
  assert(n == 0);
  printf("pass!\n");

  printf("am_map_values()...");
  n = am_map_values(doc, AM_ROOT, "string");
  assert(n == 1);
  printf("pass!\n");

  printf("am_pop_value()...");
  n = am_pop_value(doc, &datatype, buff, MAX_BUFF_SIZE);
  assert(n == strlen("hello world") + 1);
  assert(datatype == Datatype_Str);
  assert(strcmp(buff, "hello world") == 0);
  printf("pass!\n");

  printf("testing list...");

  printf("am_map_set()...");
  n = am_map_set(doc, AM_ROOT, "list", Datatype_List, NULL);
  assert(n == 1);
  printf("pass!\n");

  /*
  printf("am_pop()...");
  n = am_pop(doc, buff, MAX_BUFF_SIZE);
  assert(n == 3);
  printf("pass!\n");
  */

  printf("am_map_values()...");
  n = am_map_values(doc, AM_ROOT, "list");
  assert(n == 1);
  printf("pass!\n");

  printf("am_pop_value()...");
  n = am_pop_value(doc, &datatype, buff, MAX_BUFF_SIZE);
  assert(n == strlen("list") + 1);
  assert(datatype == Datatype_List);
  assert(strcmp(buff, "list") == 0);
  printf("pass!\n");

  printf("am_free()...");
  am_free(doc);
  printf("pass!\n");

  printf("end\n");
}
