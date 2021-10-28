
#[macro_export]
macro_rules! mk_document(
    {} => (
        $crate::Document::new_without_id()
    );
    { $($key:literal : $value:expr),+ $(,)? } => {
        {
            let mut m = $crate::Document::new_without_id();
            $(
                m.insert($key.into(), $crate::Value::from($value));
            )+
            m
        }
     };
);

#[macro_export]
macro_rules! mk_array(
    [] => (
        $crate::Array::new()
    );
    [ $($elem:expr),+ $(,)? ] => {
        {
            let mut arr = $crate::Array::new();
            $(
                arr.push($crate::Value::from($elem));
            )+
            arr
        }
    }
);

#[macro_export]
macro_rules! doc(
    {} => (
        $crate::Document::new_without_id()
    );
    { $($key:literal : $value:expr),+ $(,)? } => {
        {
            let mut m = $crate::Document::new_without_id();
            $(
                m.insert($key.into(), $crate::Value::from($value));
            )+
            m
        }
     };
);

#[macro_export]
macro_rules! arr(
    [] => (
        $crate::Array::new()
    );
    [ $($elem:expr),+ $(,)? ] => {
        {
            let mut arr = $crate::Array::new();
            $(
                arr.push($crate::Value::from($elem));
            )+
            arr
        }
    }
);
