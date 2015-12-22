#ifndef __BUFLIST_H
#define __BUFLIST_H

#include <sys/types.h>
#include <stdarg.h>
#include <stdint.h>

typedef struct buflist_t {
    uint32_t size;
    uint32_t len;
    uint32_t alloc;
    char buf[];
} buflist_t;

typedef struct buflist_iter_t {
    const buflist_t* l;
    char* offset;
} buflist_iter_t;

// create new buflist
buflist_t* buflist_new();
// create new buflist from exist buf
buflist_t* buflist_new_from_buf(const char* buf, uint32_t sz);

void buflist_push(buflist_t** l, char* buf, uint32_t size);
uint32_t buflist_len(buflist_t* l);
void buflist_get(
    buflist_t* l, int i, char** ret_ptr, uint32_t* size, char** err);
void buflist_getbuf(buflist_t* l, char** ret_ptr, uint32_t* sz);
void buflist_free(buflist_t* l);

// iterator funcs
buflist_iter_t* buflist_iter_new(const buflist_t* l);
void buflist_iter_next(buflist_iter_t* it);
int buflist_iter_valid(buflist_iter_t* it);
void buflist_iter_cur(buflist_iter_t* it, char** ret_ptr, uint32_t* size);
void buflist_iter_free(buflist_iter_t* it);
void _debug_output(buflist_t* l);
#endif
