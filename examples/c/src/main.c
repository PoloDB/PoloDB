#include <stdio.h>
#include <assert.h>
#include <polodb/polodb.h>

int main(int argc, char **argv) {
    struct Database* database = Database_open_path("./data.db");
    struct Collection* collection = Database_collection(database, "books");

    const char* books = "[\
        {\
            \"title\" : \"The Three-Body Problem\",\
            \"author\" : \"Liu Cixin\"\
        },\
        {\
            \"title\" : \"The Dark Forest\",\
            \"author\" : \"Liu Cixin\"\
        },\
        {\
            \"title\": \"The Posthumous Memoirs of Brás Cubas\",\
            \"author\": \"Machado de Assis\"\
        }\
    ]";
    assert(Collection_insert_many(collection, books));
    struct Find* find = Collection_find(collection, "{ \"author\" : \"Liu Cixin\" }");
    const char** results = Find_run(find);
    for (int i = 0; results[i]; i++) {
        printf("%s\n", results[i]);
    }

    Find_destroy(find);
    Collection_destroy(collection);
    Database_destroy(database);
    return 0;
}