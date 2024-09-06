#include <stdio.h>
#include <assert.h>
#include <polodb/polodb.h>
#include "doc.h"

int main(int argc, char **argv) {
    struct Database* database = Database_open_path("./data.db");
    printf("%p\n", database);
    struct Collection* collection = Database_collection(database, "books");

    const char* books = ARRAY(
        OBJECT(
            FIELD("title", "The Three-Body Problem"),
            FIELD("author", "Liu Cixin")
        ),
        OBJECT(
            FIELD("title", "The Dark Forest"),
            FIELD("author", "Liu Cixin")
        ),
        OBJECT(
            FIELD("title", "The Posthumous Memoirs of Brás Cubas"),
            FIELD("author", "Machado de Assis")
        )
    );
    assert(Collection_insert_many(collection, books));
    struct Find* find = Collection_find(collection, OBJECT(FIELD("author", "Liu Cixin")));
    const char** results = Find_run(find);
    for (int i = 0; results[i]; i++) {
        printf("%s\n", results[i]);
    }

    Find_destroy(find);
    Collection_destroy(collection);
    Database_destroy(database);
    return 0;
}
