
#[macro_export]
macro_rules! polo_log (
    ($($arg:tt)+) => {
        if cfg!(log) {
            eprintln!($($arg)*);
        }
    }
);
