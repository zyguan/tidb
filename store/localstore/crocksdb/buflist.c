#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include "buflist.h"

#define DEFAULT_BUFSIZE 1024

void _debug_output(buflist_t* l)
{
    int i = 0;
    for (i = 0; i < l->size; i++) {
        printf("%02X ", (char)(l->buf[i]));
    }
    printf("%d %d\n", l->size, l->len);
}

buflist_t* buflist_new()
{
    buflist_t* ret = malloc(sizeof(buflist_t) + DEFAULT_BUFSIZE);
    ret->size = 0;
    ret->len = 0;
    ret->alloc = DEFAULT_BUFSIZE;
    return ret;
}

buflist_t* buflist_new_from_buf(const unsigned char* buf, uint32_t sz)
{
    buflist_t* ret = malloc(sizeof(buflist_t) + sz);
    ret->size = sz;
    ret->alloc = sz;
    ret->len = 0;
    memcpy(ret->buf, buf, sz);
    unsigned char* h = ret->buf;
    while (h < ret->buf + sz) {
        h += (uint32_t)(*h) + sizeof(uint32_t);
        ret->len++;
    }
    return ret;
}

uint32_t buflist_len(buflist_t* l) { return l->len; }

void buflist_push(buflist_t** l, unsigned char* buf, uint32_t size)
{
    int i;
    int new_size = (*l)->size + size + sizeof(uint32_t);
    if (new_size > (*l)->alloc) {
        // realloc
        *l = realloc(*l, new_size * 2);
        (*l)->alloc = new_size * 2;
    }
    // move to buf's tail
    unsigned char* h = (*l)->buf + (*l)->size;
    // write node data
    memcpy(h, &size, sizeof(uint32_t));
    memcpy(h + sizeof(uint32_t), buf, size);
    // update new meta
    (*l)->size = new_size;
    (*l)->len++;
}

void buflist_free(buflist_t* l)
{
    if (l != NULL) {
        free(l);
    }
}

buflist_iter_t* buflist_iter_new(const buflist_t* l)
{
    buflist_iter_t* ret = malloc(sizeof(buflist_iter_t));
    ret->offset = (unsigned char*)l->buf;
    ret->l = l;
    return ret;
}

void buflist_iter_next(buflist_iter_t* it)
{
    it->offset += (uint32_t)(*it->offset) + sizeof(uint32_t);
}

void buflist_iter_cur(
    buflist_iter_t* it, unsigned char** ret_ptr, uint32_t* size)
{
    if (buflist_iter_valid(it)) {
        *size = (uint32_t)(*(it->offset));
        *ret_ptr = it->offset + sizeof(uint32_t);
    }
}

int buflist_iter_valid(buflist_iter_t* it)
{
    return it->offset < it->l->buf + it->l->size ? 1 : 0;
}

void buflist_iter_free(buflist_iter_t* it)
{
    if (it != NULL) {
        free(it);
    }
}
