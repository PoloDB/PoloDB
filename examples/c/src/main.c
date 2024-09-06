#include <stdio.h>
#include <assert.h>
#include <polodb/polodb.h>
#include "doc.h"

int main(int argc, char **argv) {
    struct Database* database = Database_open_path("./data.db");
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
    printf("Inserted %d\n", Collection_insert_many(collection, books));
    struct Find* find = Collection_find(collection, OBJECT(FIELD("author", "Liu Cixin")));
    const char** results = Find_run(find); // TODO: Change the return time to Cursor. Where Cursor is represented as Cursor<bson::Document> on the Rust side.
    for (int i = 0; results[i]; i++) {
        printf("%s\n", results[i]);
        String_destroy(results[i]); // FIXME: This will become unnecessary once we return a Cursor.
    }

    Vector_destroy(results); // TODO: Replace this with a Cursor destructor. Maybe it will not even be necessary if we consume the whole cursor.
    Collection_destroy(collection);
    Database_destroy(database);
    return 0;
}
