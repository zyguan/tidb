#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/time.h>
#include <unistd.h>

#include <rocksdb/c.h>
#include "buflist.h"
#include "sds.h"

#include "bridge.h"

static rocksdb_t* db;

// timing helper
static struct timeval tm1;
static inline void start() { gettimeofday(&tm1, NULL); }
static inline void stop()
{
    struct timeval tm2;
    gettimeofday(&tm2, NULL);
    unsigned long long t = 1000 * (tm2.tv_sec - tm1.tv_sec)
        + (tm2.tv_usec - tm1.tv_usec) / 1000;
    printf("%llu ms\n", t);
}

int init(const unsigned char* path, uint32_t path_sz, char** err)
{
    rocksdb_options_t* options = rocksdb_options_create();
    long cpus = sysconf(_SC_NPROCESSORS_ONLN); // get # of online cores
    rocksdb_options_increase_parallelism(options, (int)(cpus));
    rocksdb_options_optimize_level_style_compaction(options, 0);
    rocksdb_options_set_create_if_missing(options, 1);
    // open DB
    char* p = malloc(path_sz + 1);
    memset(p, 0, path_sz + 1);
    memcpy(p, path, path_sz);
    db = rocksdb_open(options, p, err);
    if (err != NULL) {
        free(p);
        rocksdb_options_destroy(options);
        return -1;
    }
    // cleanup
    free(p);
    rocksdb_options_destroy(options);
    return 0;
}

void get(const unsigned char* key, uint32_t key_sz, unsigned char** ret_val,
    uint32_t* ret_sz)
{
    rocksdb_readoptions_t* readoptions = rocksdb_readoptions_create();
    size_t len;
    char* err = NULL;
    char* returned_value
        = rocksdb_get(db, readoptions, key, key_sz, &len, &err);
    if (err != NULL) {
        return;
    }
    *ret_val = returned_value;
    *ret_sz = len;
}

void seek(const unsigned char* start, uint32_t start_sz,
    unsigned char** ret_key, uint32_t* ret_key_sz, unsigned char** ret_val,
    uint32_t* ret_val_sz)
{
    rocksdb_readoptions_t* readoptions = rocksdb_readoptions_create();
    rocksdb_iterator_t* it = rocksdb_create_iterator(db, readoptions);
    // get start keys
    rocksdb_iter_seek(it, start, (size_t)start_sz);
    if (rocksdb_iter_valid(it)) {
        size_t key_sz = 0;
        size_t val_sz = 0;
        *ret_key = (char*)rocksdb_iter_key(it, &key_sz);
        *ret_val = (char*)rocksdb_iter_value(it, &val_sz);
        *ret_key_sz = (uint32_t)key_sz;
        *ret_val_sz = (uint32_t)val_sz;
    }
    rocksdb_iter_destroy(it);
}

void multi_put(const unsigned char* key_list, uint32_t key_list_sz,
    const unsigned char* val_list, uint32_t val_list_sz)
{
    buflist_t* keys = buflist_new_from_buf(key_list, key_list_sz);
    buflist_t* vals = buflist_new_from_buf(val_list, val_list_sz);
    _debug_output(vals);

    rocksdb_writebatch_t* batch = rocksdb_writebatch_create();
    buflist_iter_t* key_it = buflist_iter_new(keys);
    buflist_iter_t* val_it = buflist_iter_new(vals);

    for (; buflist_iter_valid(key_it); buflist_iter_next(key_it)) {
        char *key, *val;
        uint32_t key_sz = 0;
        uint32_t val_sz = 0;
        // get key
        buflist_iter_cur(key_it, &key, &key_sz);
        // get val
        buflist_iter_cur(val_it, &val, &val_sz);
        printf("%u %u\n", key_sz, val_sz);
        if (val_sz == 1 && val[0] == '\0') {
            // delete mark
            rocksdb_writebatch_delete(batch, key, key_sz);
        }
        else if (key_sz > 0 && val_sz > 0) {
            rocksdb_writebatch_put(batch, key, key_sz, val, val_sz);
        }
        // go next val
        buflist_iter_next(val_it);
    }

    char* err = NULL;
    rocksdb_writeoptions_t* writeoptions = rocksdb_writeoptions_create();
    rocksdb_write(db, writeoptions, batch, &err);

    buflist_free(keys);
    buflist_free(vals);

    if (err != NULL) {
        printf("err: %s\n", err);
        free(err);
    }
}

void multi_seek(const unsigned char* start_key_list,
    uint32_t start_key_list_sz, const unsigned char** key_list,
    uint32_t* key_list_sz, const unsigned char** val_list,
    uint32_t* val_list_sz)
{
    buflist_t* start_keys
        = buflist_new_from_buf(start_key_list, start_key_list_sz);

    buflist_t* ret_keys = buflist_new();
    buflist_t* ret_vals = buflist_new();

    rocksdb_readoptions_t* readoptions = rocksdb_readoptions_create();
    rocksdb_iterator_t* it = rocksdb_create_iterator(db, readoptions);
    buflist_iter_t* sit = buflist_iter_new(start_keys);
    rocksdb_iter_seek_to_first(it);

    for (; buflist_iter_valid(sit); buflist_iter_next(sit)) {
        char* start_key;
        uint32_t sz = 0;
        buflist_iter_cur(sit, &start_key, &sz);
        rocksdb_iter_seek(it, start_key, (size_t)sz);
        if (rocksdb_iter_valid(it)) {
            size_t key_sz = 0;
            size_t val_sz = 0;
            char* key = (char*)rocksdb_iter_key(it, &key_sz);
            char* val = (char*)rocksdb_iter_value(it, &val_sz);
            if (key != NULL && val != NULL) {
                buflist_push(&ret_keys, key, key_sz);
                buflist_push(&ret_vals, val, val_sz);
            }
        }
        else {
            char s[] = "\0";
            buflist_push(&ret_keys, s, 1);
            buflist_push(&ret_vals, s, 1);
        }
    }
    *key_list = ret_keys->buf;
    *key_list_sz = ret_keys->size;
    *val_list = ret_vals->buf;
    *val_list_sz = ret_vals->size;
}
