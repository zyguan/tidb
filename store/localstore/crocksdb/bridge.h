#ifndef __BRIDGE_H
#define __BRIDGE_H

#include "buflist.h"

int init(const char* db_path, uint32_t path_sz, char** err);

void get(const char* key, uint32_t key_sz, char** ret_val, uint32_t* ret_sz);

void seek(const char* start, uint32_t start_sz, char** ret_key,
    uint32_t* ret_key_sz, char** ret_val, uint32_t* ret_val_sz);

void multi_put(const char* key_list, uint32_t key_list_sz,
    const char* val_list, uint32_t val_list_sz);

void multi_seek(const char* start_key_list, uint32_t start_key_list_sz,
    const char** key_list, uint32_t* key_list_sz, const char** val_list,
    uint32_t* val_list_sz);

void free(void* ptr);

#endif
