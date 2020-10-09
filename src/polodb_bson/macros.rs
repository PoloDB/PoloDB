/*
 * Copyright (c) 2020 Vincent Chan
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation; either version 3, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#[macro_export]
macro_rules! mk_document(
    {} => (
        $crate::Document::new_without_id()
    );
    { $($key:literal : $value:expr),+ $(,)? } => {
        {
            let mut m = $crate::Document::new_without_id();
            $(
                m.insert(String::from($key), $crate::Value::from($value));
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
