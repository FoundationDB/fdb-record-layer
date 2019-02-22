/*
 * TextTokenizerFactory.java
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

package com.apple.foundationdb.record.provider.common.text;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexMaintainer;

import javax.annotation.Nonnull;

/**
 * Each implementation of {@link TextTokenizer} should have its own implementation
 * of this factory interface to provide instances of the tokenizer to a
 * {@link TextTokenizerRegistry}. The registry will populate a mapping of names to
 * tokenizers using the methods of this interface.
 *
 * @see TextIndexMaintainer
 */
@API(API.Status.EXPERIMENTAL)
public interface TextTokenizerFactory {
    /**
     * Get the unique name for the text tokenizer. This is the name that should
     * be included in the index meta-data in order to indicate that this tokenizer
     * should be used within a certain index.
     *
     * @return the name of the tokenizer that this factory creates
     */
    @Nonnull
    String getName();

    /**
     * Get an instance of the text tokenizer. For a given factory, each tokenizer
     * should be of the same type, and it should match the result of {@link #getName()}.
     *
     * @return an instance of the tokenizer that this factory creates
     */
    @Nonnull
    TextTokenizer getTokenizer();
}
