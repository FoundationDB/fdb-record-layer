/*
 * package-info.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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
 * Support classes for text indexing.
 *
 * <h2>Tokenizing Text</h2>
 *
 * <p>
 * A text index has an entry for every token (word) of the indexed string value.
 * The conversion of the string value to tokens is done by a {@link com.apple.foundationdb.record.provider.common.text.TextTokenizer}.
 * The {@link com.apple.foundationdb.record.metadata.Index} options in the meta-data specify the name of the tokenizer, which is looked
 * up in the {@link com.apple.foundationdb.record.provider.common.text.TextTokenizerRegistry} to get a {@link com.apple.foundationdb.record.provider.common.text.TextTokenizerFactory}
 * to create the actual tokenizer object.
 * </p>
 */
package com.apple.foundationdb.record.provider.common.text;
