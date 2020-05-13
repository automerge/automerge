

#include <stdio.h>
#include "automerge.h"

int main() {
  printf("begin\n");
  Backend * b = automerge_init();
  const char * request = R"({"requestType":"change","actor":"111111","seq":1,"time":0,"version":0,"ops":[{"action":"set","obj":"00000000-0000-0000-0000-000000000000","key":"bird","value":"magpie"}]})";
  printf("request: %s\n",request);
  const char * patch = automerge_apply_local_change(b, request);
  printf("patch: %s\n",patch);
  printf("free resources\n");
  automerge_free_string(patch);
  automerge_free(b);
  printf("end\n");
}
