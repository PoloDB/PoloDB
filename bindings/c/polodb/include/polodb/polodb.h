#pragma once

#include <stdio.h>
#include "doc.h"

struct Database {
    void* handle;
};

struct Database* Database_open_path(const char *path) {
    return 0;
}
void Database_destroy(struct Database* database) {}

struct Collection {
    void* handle;
};

struct Collection* Database_collection(struct Database* database, const char *name) {
    return 0;
}

void Collection_destroy(struct Collection* collection) {}

int Collection_insert_many(struct Collection* collection, const char *json) {
    printf("Inserting: %s\n", json);
    return 1;
}

struct Find {
    void* handle;
};

struct Find* Collection_find(struct Collection* collection, const char *json) {
    printf("Finding: %s\n", json);
    return 0;
}

void Find_destroy(struct Find* find) {}

const char** Find_run(struct Find* find) {
    static const char* results[] = {
        OBJECT(
            FIELD("title", "The Three-Body Problem"),
            FIELD("author", "Liu Cixin")
        ),
        OBJECT(
            FIELD("title", "The Dark Forest"),
            FIELD("author", "Liu Cixin")
        ),
        0
    };
    return results;
}
