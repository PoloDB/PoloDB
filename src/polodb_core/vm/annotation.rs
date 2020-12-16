use std::fmt;
use std::collections::BTreeMap;

pub(super) struct Annotation {
    comment: BTreeMap<u32, String>,
}

impl Annotation {

    pub(super) fn new() -> Annotation {
        Annotation {
            comment: BTreeMap::new(),
        }
    }

    pub(super) fn annotate(&mut self, position: u32, content: String) {
        self.comment.insert(position, content);
    }

    pub(super) fn write_fmt(&self, f: &mut fmt::Formatter, pc: u32) -> fmt::Result {
        if let Some(content) = self.comment.get(&pc) {
            writeln!(f)?;
            writeln!(f, "{}:", content)?;
        }
        Ok(())
    }

}
