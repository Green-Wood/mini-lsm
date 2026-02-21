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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
use bytes::{BufMut, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);
        buf.extend_from_slice(&self.data);

        for offset in &self.offsets {
            buf.put_u16_le(*offset);
        }

        // Footer: number of offsets (u16, little endian)
        buf.put_u16_le(self.offsets.len() as u16);

        buf.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // Footer: last 4 bytes = number of offsets
        let footer_pos = data.len() - 2;
        let num_offsets = u16::from_le_bytes(data[footer_pos..].try_into().unwrap()) as usize;

        let offsets_pos = footer_pos - num_offsets * 2;
        let data_bytes = &data[..offsets_pos];
        let offsets_bytes = &data[offsets_pos..footer_pos];

        let mut offsets = Vec::with_capacity(num_offsets);
        let mut offset_buf = offsets_bytes;
        for _ in 0..num_offsets {
            offsets.push(u16::from_le_bytes(offset_buf[..2].try_into().unwrap()));
            offset_buf = &offset_buf[2..];
        }

        Block {
            data: data_bytes.to_vec(),
            offsets,
        }
    }
}
