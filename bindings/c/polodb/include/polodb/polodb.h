#pragma once

#include <stdio.h>
#include <stdint.h>
#include "doc.h"

struct Database;
extern struct Database* Database_open_path(const char *path);
extern void Database_destroy(struct Database* database);

struct Collection;
extern struct Collection* Database_collection(struct Database* database, const char *name);
extern uint32_t Collection_insert_many(struct Collection* collection, const char *json);
extern void Collection_destroy(struct Collection* collection);

struct Find;
extern struct Find* Collection_find(struct Collection* collection, const char *json);

extern const char** Find_run(struct Find* find);

extern void Vector_destroy();
extern void String_destroy(const char* string);