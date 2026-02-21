// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let base = match (self.1.peek(), other.1.peek()) {
            (Some((a_key, _)), Some((b_key, _))) => a_key.cmp(&b_key).then(self.0.cmp(&other.0)),
            (Some(_), None) => cmp::Ordering::Less,
            (None, Some(_)) => cmp::Ordering::Greater,
            (None, None) => cmp::Ordering::Equal,
        };

        base.reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters: BinaryHeap<_> = iters
            .into_iter()
            .enumerate()
            .map(|(i, it)| HeapWrapper(i, it))
            .collect();
        let current = iters.pop();
        MergeIterator { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn peek(&self) -> Option<(KeySlice<'_>, &[u8])> {
        let current = self.current.as_ref()?;
        current.1.peek()
    }

    fn next(&mut self) -> Result<()> {
        let curr = self.current.take();
        match curr {
            None => (),
            Some(mut curr) => {
                match curr.1.peek() {
                    None => (),
                    Some((k, _)) => {
                        while let Some(mut inner) = self.iters.peek_mut()
                            && inner.1.peek().map(|(k, _)| k) == Some(k)
                        {
                            if let Err(e) = inner.1.next() {
                                PeekMut::pop(inner);
                                return Err(e);
                            }
                        }
                    }
                };
                curr.1.next()?;
                self.iters.push(curr);
                self.current = self.iters.pop();
            }
        }
        Ok(())
    }
}
