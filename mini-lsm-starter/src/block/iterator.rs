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

use std::sync::Arc;

use crate::{
    iterators::StorageIterator,
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current entry (key and value range), or None if iterator is invalid/exhausted
    current: Option<(KeyVec, (usize, usize))>,
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            current: None,
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block.clone());
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block.clone());
        iter.seek_to_key(key);
        iter
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.update_current();
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut found = false;
        for i in 0..self.block.offsets.len() {
            let (k_bytes, _) = Self::parse_entry(&self.block.data, self.block.offsets[i] as usize);
            if k_bytes >= key.raw_ref() {
                self.idx = i;
                found = true;
                break;
            }
        }
        if found {
            self.update_current();
        } else {
            self.idx = self.block.offsets.len(); // invalid
            self.current = None;
        }
    }
    fn update_current(&mut self) {
        if self.idx < self.block.offsets.len() {
            let (k_bytes, v_range) =
                Self::parse_entry(&self.block.data, self.block.offsets[self.idx] as usize);
            let key = KeyVec::from_vec(k_bytes.to_vec());
            self.current = Some((key, v_range));
        } else {
            self.current = None;
        }
    }

    fn parse_entry(data: &[u8], offset: usize) -> (&[u8], (usize, usize)) {
        let mut pos = offset;
        let key_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        let key = &data[pos..pos + key_len];
        pos += key_len;
        let value_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        let value_range = (pos, pos + value_len);
        (key, value_range)
    }
}

impl StorageIterator for BlockIterator {
    type KeyType<'a>
        = KeySlice<'a>
    where
        Self: 'a;

    fn peek(&self) -> Option<(Self::KeyType<'_>, &[u8])> {
        if let Some((key, (start, end))) = &self.current {
            let key_slice = key.as_key_slice();
            let value = &self.block.data[*start..*end];
            Some((key_slice, value))
        } else {
            None
        }
    }

    /// Move to the next key in the block.
    fn next(&mut self) -> anyhow::Result<()> {
        if self.idx < self.block.offsets.len() {
            self.idx += 1;
            self.update_current();
        }
        Ok(())
    }
}
