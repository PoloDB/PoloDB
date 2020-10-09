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
use self::heapsize::{HeapSizeOf, heap_size_of};
use std::hash::{Hash, BuildHasher};

use super::{LinkedHashMap, KeyRef, Node};

impl<K> HeapSizeOf for KeyRef<K> {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}

impl<K, V> HeapSizeOf for Node<K, V>
    where K: HeapSizeOf,
          V: HeapSizeOf
{
    fn heap_size_of_children(&self) -> usize {
        self.key.heap_size_of_children() + self.value.heap_size_of_children()
    }
}

impl<K, V, S> HeapSizeOf for LinkedHashMap<K, V, S>
    where K: HeapSizeOf + Hash + Eq,
          V: HeapSizeOf,
          S: BuildHasher
{
    fn heap_size_of_children(&self) -> usize {
        unsafe {
            let mut size = self.map.heap_size_of_children();
            for &value in self.map.values() {
                size += (*value).heap_size_of_children();
                size += heap_size_of(value as *const _ as *const _);
            }

            if !self.head.is_null() {
                size += heap_size_of(self.head as *const _ as *const _);
            }

            let mut free = self.free;
            while !free.is_null() {
                size += heap_size_of(free as *const _ as *const _);
                free = (*free).next
            }

            size
        }
    }
}
