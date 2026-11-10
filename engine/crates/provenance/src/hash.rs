// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Canonical, domain-separated provenance hashing.

use crate::{ArtifactRef, ContentDigest};

pub(crate) struct CanonicalHasher(blake3::Hasher);

impl CanonicalHasher {
    pub(crate) fn new(domain: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        write_bytes(&mut hasher, domain);
        Self(hasher)
    }

    pub(crate) fn write_u8(&mut self, value: u8) {
        self.0.update(&[value]);
    }

    pub(crate) fn write_u64(&mut self, value: u64) {
        self.0.update(&value.to_be_bytes());
    }

    pub(crate) fn write_str(&mut self, value: &str) {
        write_bytes(&mut self.0, value.as_bytes());
    }

    pub(crate) fn write_bytes(&mut self, value: &[u8]) {
        write_bytes(&mut self.0, value);
    }

    pub(crate) fn write_digest(&mut self, value: &ContentDigest) {
        self.0.update(value.as_bytes());
    }

    pub(crate) fn write_ref(&mut self, value: &ArtifactRef) {
        self.write_str(value.id().as_str());
        self.write_digest(value.digest());
    }

    pub(crate) fn write_optional_ref(&mut self, value: Option<&ArtifactRef>) {
        match value {
            Some(value) => {
                self.write_u8(1);
                self.write_ref(value);
            }
            None => self.write_u8(0),
        }
    }

    pub(crate) fn finish(self) -> ContentDigest {
        ContentDigest::from_array(*self.0.finalize().as_bytes())
    }
}

fn write_bytes(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}
