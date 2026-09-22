/*
 * IndexGenerationOptions.java
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

package com.apple.foundationdb.relational.recordlayer.query.ddl;

import javax.annotation.Nonnull;

/**
 * What the definition asks of the index beyond its key.
 *
 * @param unique whether the index enforces uniqueness
 * @param containsNullableArray whether the table holds a nullable array, which changes how an array field is wrapped
 * @param emptyKeyAllowed whether an index with no key columns gets an empty key and all its columns in the value, which
 * is what a vector index without {@code PARTITION BY} needs
 * @param extremumEverStorage which form an extremum-ever aggregate is stored in
 */
public record IndexGenerationOptions(boolean unique, boolean containsNullableArray, boolean emptyKeyAllowed,
                                     @Nonnull ExtremumEverStorage extremumEverStorage) {
}
