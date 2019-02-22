/*
 * TextTokenizerRegistry.java
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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.MetaDataException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * Registry for {@link TextTokenizer}s. This registry allows for full-text indexes to specify
 * their tokenizer type through an index option, using the
 * "{@value IndexOptions#TEXT_TOKENIZER_NAME_OPTION}" option. The registry
 * will then be queried for the tokenizer that has that name at index- and query-time.
 *
 * <p>
 * Note that there are two ways of adding elements to the tokenizer registry. The first
 * is to use the {@link com.google.auto.service.AutoService AutoService} annotation to mark
 * a {@link TextTokenizerFactory} implementation as one that should be loaded into
 * the registry. The other is to call {@link #register(TextTokenizerFactory) register()}
 * on this interface to register that tokenizer manually. This second way is useful
 * for tokenizers that are built on the fly from configuration parameters, for example.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public interface TextTokenizerRegistry {
    /**
     * Gets the tokenizer of the given name. If <code>name</code> is <code>null</code>,
     * it returns an instance of the {@link DefaultTextTokenizer}.
     *
     * @param name the name of the tokenizer to retrieve
     * @return the tokenizer registered with the given name
     * @throws MetaDataException if no such tokenizer exists
     */
    @Nonnull
    TextTokenizer getTokenizer(@Nullable String name);

    /**
     * Registers a new tokenizer in this registry. The tokenizer should have a different
     * name from all tokenizers that are currently registered. This will throw an error
     * if there is already a tokenizer present that is not pointer-equal to the
     * <code>tokenizerFactory</code> parameter given.
     *
     * @param tokenizerFactory new tokenizer to register
     * @throws RecordCoreArgumentException if there is a tokenizer of the same name already registered
     */
    void register(@Nonnull TextTokenizerFactory tokenizerFactory);

    /**
     * Returns all registered tokenizers.
     * @return a map from tokenizer name to {@link TextTokenizerFactory}
     */
    @Nonnull
    Map<String, TextTokenizerFactory> getRegistry();

    /**
     * Clears the registry and reloads tokenizers from the classpath. This is intended
     * mainly for testing purposes (to avoid having one test add a tokenizer to the
     * registry that another test cannot override).
     */
    void reset();
}
