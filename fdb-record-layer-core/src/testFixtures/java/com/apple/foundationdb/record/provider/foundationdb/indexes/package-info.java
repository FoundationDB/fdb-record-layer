/*
 * package-info.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Test fixtures for the index-maintainer classes in the production package. Current contents
 * are limited to {@link com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils},
 * which provides shared constants for the standard test document types, a compressing
 * serializer for round-trip tests, and a helper for shaping a
 * {@link com.apple.foundationdb.record.RecordMetaData} so text indexes resemble relational tables.
 */
package com.apple.foundationdb.record.provider.foundationdb.indexes;
