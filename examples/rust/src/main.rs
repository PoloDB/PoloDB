
use polodb_core::{bson::doc, CollectionT, Database};
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
struct Book {
    title: String,
    author: String,
}

impl Book {
    pub fn new(title: impl ToString, author: impl ToString) -> Self {
        let title = title.to_string();
        let author = author.to_string();
        Self { title, author }
    }
}

fn main() -> polodb_core::Result<()> {
    let database = Database::open_path("./data.db")?;
    let collection = database.collection::<Book>("books");
    let books = vec![
        Book::new("The Three-Body Problem", "Liu Cixin"),
        Book::new("The Dark Forest", "Liu Cixin"),
        Book::new("The Posthumous Memoirs of Brás Cubas", "Machado de Assis")
    ];
    collection.insert_many(books)?;

    for book in collection.find(doc! { "author": "Liu Cixin" }).run()? {
        println!("{:?}", book?);
    }
    Ok(())
}
