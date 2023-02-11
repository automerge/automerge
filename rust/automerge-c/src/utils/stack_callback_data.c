#include <stdlib.h>

#include <automerge-c/utils/stack_callback_data.h>

AMstackCallbackData* AMstackCallbackDataInit(AMvalType const bitmask, char const* const file, int const line) {
    AMstackCallbackData* data = malloc(sizeof(AMstackCallbackData));
    *data = (AMstackCallbackData){.bitmask = bitmask, .file = file, .line = line};
    return data;
}
