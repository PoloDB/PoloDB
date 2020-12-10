
pub struct Line {
    index: usize,
    content: String,
}

impl Line {

    pub fn new(index: usize, content: String) -> Line {
        Line {
            index,
            content,
        }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn content(&self) -> &str {
        self.content.as_str()
    }

}
