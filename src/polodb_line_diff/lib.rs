mod line;

use ansi_term::Colour::{Red, Green};
use line::Line;

#[derive(Eq, PartialEq)]
pub enum DiffOp {
    Preserve,
    Delete,
    Insert,
    Replace,
}

struct Item {
    distance: usize,
    op: DiffOp,
}

impl Item {

    fn new() -> Item {
        Item {
            distance: 0,
            op: DiffOp::Preserve,
        }
    }

}

fn make_row(m: usize) -> Vec<Item> {
    let mut result: Vec<Item> = Vec::new();

    result.resize_with(m, || {
        Item::new()
    });

    result
}

fn make_matrix(n: usize, m: usize) -> Vec<Vec<Item>> {
    let mut matrix: Vec<Vec<Item>> = vec![];

    matrix.resize_with(n + 1, || {
        make_row(m + 1)
    });

    matrix
}

fn minimum(a: usize, b: usize, c: usize) -> (usize, char) {
    if a < b {
        if a < c {
            (a, 'a')
        } else {
            (c, 'c')
        }
    } else {
        if b < c {
            (b, 'b')
        } else {
            (c, 'c')
        }
    }
}

pub fn diff(a: &str, b: &str, splitter: &str) -> Vec<Diff> {
    let a_lines: Vec<&str> = a.split(splitter).collect();
    let b_lines: Vec<&str> = b.split(splitter).collect();

    let mut matrix: Vec<Vec<Item>> = make_matrix(a_lines.len() + 1, b_lines.len() + 1);

    for i in 0..=a_lines.len() {
        matrix[i][0].distance = i;
    }

    for j in 0..=b_lines.len() {
        matrix[0][j].distance = j;
    }

    for i in 1..=a_lines.len() {
        let line1 = a_lines[i - 1];
        for j in 1..=b_lines.len() {
            let line2 = b_lines[j - 1];

            let cost: usize = if line1 == line2 {
                0
            } else {
                1
            };

            let (dis, ch) = minimum(
                matrix[i - 1][j].distance + 1,
                matrix[i][j - 1].distance + 1,
                matrix[i - 1][j - 1].distance + cost
            );

            let op = match (ch, cost) {
                ('a', _) => DiffOp::Delete,
                ('b', _) => DiffOp::Insert,
                ('c', 0) => DiffOp::Preserve,
                ('c', 1) => DiffOp::Replace,
                _ => panic!("unreachable")
            };

            matrix[i][j].distance = dis;
            matrix[i][j].op = op;
        }
    }

    backtracking(&matrix, &a_lines, &b_lines, a_lines.len(), b_lines.len())
}

pub fn format_differences(diff: &Vec<Diff>) -> String {
    let mut tmp = String::new();
    for item in diff {
        let text = format!("{}", item);
        tmp.push_str(&text);
    }
    tmp
}

pub struct Diff {
    op: DiffOp,
    in_lines: Vec<Line>,
    de_lines: Vec<Line>,
}

impl std::fmt::Display for Diff {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.op {
            DiffOp::Preserve => {
                writeln!(f, "Preserve")
            }

            DiffOp::Insert => {
                writeln!(f, "@@ {}", self.in_lines[0].index() + 1)?;
                for line in &self.in_lines {
                    let color_add = format!("+ {}", line.content());
                    writeln!(f, "{}", Green.paint(color_add))?;
                }
                Ok(())
            }

            DiffOp::Delete => {
                writeln!(f, "@@ {}", self.de_lines[0].index() + 1)?;
                for line in &self.de_lines {
                    let color_minus = format!("- {}", line.content());
                    writeln!(f, "{}", Red.paint(color_minus))?;
                }
                Ok(())
            }

            DiffOp::Replace => {
                writeln!(f, "@@ {}", self.in_lines[0].index() + 1)?;
                for line in &self.in_lines {
                    let color_add = format!("+ {}", line.content());
                    writeln!(f, "{}", Green.paint(color_add))?;
                }
                for line in &self.de_lines {
                    let color_minus = format!("- {}", line.content());
                    writeln!(f, "{}", Red.paint(color_minus))?;
                }
                Ok(())
            }
        }
    }

}

fn backtracking(matrix: &Vec<Vec<Item>>, a_lines: &Vec<&str>, b_lines: &Vec<&str>, mut i: usize, mut j: usize) -> Vec<Diff> {
    let mut result: Vec<Diff> = vec![];
    while i > 0 && j > 0 {
        match matrix[i][j].op {
            DiffOp::Preserve => {
                i -= 1;
                j -= 1;
            }

            DiffOp::Replace => {
                let in_line = Line::new(i - 1,a_lines[i - 1].into());
                let de_line = Line::new(j - 1, b_lines[j - 1].into());
                if let Some(last) = result.last_mut() {
                    if last.op == DiffOp::Replace {
                        last.in_lines.push(in_line);
                        last.de_lines.push(de_line);

                        i -= 1;
                        j -= 1;
                        continue
                    }
                }
                result.push(Diff {
                    op: DiffOp::Replace,
                    in_lines: vec![ in_line ],
                    de_lines: vec![ de_line ],
                });

                i -= 1;
                j -= 1;
            }

            DiffOp::Insert => {
                let in_line = Line::new(j - 1, b_lines[j - 1].into());

                if let Some(last) = result.last_mut() {
                    if last.op == DiffOp::Insert {
                        last.in_lines.push(in_line);
                        j -= 1;
                        continue;
                    }
                }

                result.push(Diff {
                    op: DiffOp::Insert,
                    in_lines: vec![ in_line ],
                    de_lines: vec![],
                });

                j -= 1;
            }

            DiffOp::Delete => {
                let de_line = Line::new(i - 1, a_lines[i - 1].into());

                if let Some(last) = result.last_mut() {
                    if last.op == DiffOp::Delete {
                        last.de_lines.push(de_line);
                        i -= 1;
                        continue;
                    }
                }

                result.push(Diff {
                    op: DiffOp::Delete,
                    in_lines: vec![],
                    de_lines: vec![ de_line ],
                });

                i -= 1;
            }

        }
    }
    result.reverse();
    result
}

pub fn line_diff<T1: AsRef<str>, T2: AsRef<str>>(a: T1, b: T2) -> Vec<Diff> {
    diff(a.as_ref(), b.as_ref(), "\n")
}

#[cfg(test)]
mod tests {
    use crate::line_diff;

    #[test]
    fn test_equal() {
        let text = r#"
a
b
c
"#;

        let diff = line_diff(text, text);
        assert_eq!(diff.len(), 0);

        for item in diff {
            println!("{}", item);
        }
    }

    #[test]
    fn test_diff() {
        let text1 = r#"
a
b
c
"#;
        let text2 = r#"
a
b2
c
"#;

        let diff = line_diff(text1, text2);
        assert_eq!(diff.len(), 1);
        for item in diff {
            println!("{}", item);
        }
    }

    #[test]
    fn test_insert() {
        let text1 = r#"
a
c
"#;
        let text2 = r#"
a
b
c
"#;

        let diff = line_diff(text1, text2);
        assert_eq!(diff.len(), 1);
        for item in diff {
            println!("{}", item);
        }
    }

    #[test]
    fn test_delete() {
        let text1 = r#"
a
b
c
"#;
        let text2 = r#"
a
c
"#;

        let diff = line_diff(text1, text2);
        assert_eq!(diff.len(), 1);
        for item in diff {
            println!("{}", item);
        }
    }

}

#[macro_export]
macro_rules! assert_eq {
    ($left:expr, $right:expr) => ({
        let diff = $crate::line_diff($left, $right);
        if !diff.is_empty() {
            let tmp = $crate::format_differences(&diff);
            panic!("{}", tmp);
        }
    })
}
