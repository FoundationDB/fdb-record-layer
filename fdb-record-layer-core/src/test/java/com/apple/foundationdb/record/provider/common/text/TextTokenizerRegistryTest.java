/*
 * TextTokenizerRegistryTest.java
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

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link TextTokenizerRegistry}.
 */
public class TextTokenizerRegistryTest {
    private static final TextTokenizerRegistry registry = TextTokenizerRegistryImpl.instance();

    @Test
    public void containsExpected() {
        DefaultTextTokenizer defaultTokenizer = DefaultTextTokenizer.instance();
        TextTokenizer registeredDefaultTokenizer = registry.getTokenizer(DefaultTextTokenizer.NAME);
        assertEquals(defaultTokenizer, registeredDefaultTokenizer);
        TextTokenizer registeredNullTokenzier = registry.getTokenizer(null);
        assertEquals(defaultTokenizer, registeredNullTokenzier);

        PrefixTextTokenizer prefixTokenzier = PrefixTextTokenizer.instance();
        TextTokenizer registeredPrefixTokenizer = registry.getTokenizer(PrefixTextTokenizer.NAME);
        assertEquals(prefixTokenzier, registeredPrefixTokenizer);

        UniqueTokenLimitTextTokenizer uniqueLimitTokenizer = UniqueTokenLimitTextTokenizer.instance();
        TextTokenizer registeredUniqueLimitTokenizer = registry.getTokenizer(UniqueTokenLimitTextTokenizer.NAME);
        assertEquals(uniqueLimitTokenizer, registeredUniqueLimitTokenizer);
    }

    @Test
    public void missingUnexpected() {
        assertThrows(MetaDataException.class, () -> registry.getTokenizer("not_a_real_tokenizer"));
    }

    @Test
    public void register() {
        registry.reset();
        final String name = "filter_the";
        TextTokenizerFactory theRemovingTokenizer = FilteringTextTokenizer.create(
                name,
                new DefaultTextTokenizerFactory(),
                (token, version) -> !token.equals("the")
        );
        assertThrows(MetaDataException.class, () -> registry.getTokenizer(name));
        registry.register(theRemovingTokenizer);
        assertEquals(theRemovingTokenizer.getTokenizer(), registry.getTokenizer(name));

        // Works because the factory is the same
        registry.register(theRemovingTokenizer);

        // Fails because the factory is different
        TextTokenizerFactory theOtherRemovingTokenizer = FilteringTextTokenizer.create(
                name,
                new DefaultTextTokenizerFactory(),
                (token, version) -> !token.toString().equals("the")
        );
        assertThrows(RecordCoreArgumentException.class, () -> registry.register(theOtherRemovingTokenizer));
        assertEquals(theRemovingTokenizer.getTokenizer(), registry.getTokenizer(name));
    }

    @Test
    public void reset() {
        registry.reset();
        final String name = "filter_some";
        final ImmutableSet<String> someWords = ImmutableSet.of("some", "sum", "somme", "sommes", "dim");
        TextTokenizerFactory someFilterTokenizer = FilteringTextTokenizer.create(
                name,
                new DefaultTextTokenizerFactory(),
                (token, version) -> !someWords.contains(token.toString())
        );
        assertThrows(MetaDataException.class, () -> registry.getTokenizer(name));
        registry.register(someFilterTokenizer);
        assertEquals(someFilterTokenizer.getTokenizer(), registry.getTokenizer(name));
        registry.reset();
        assertEquals(DefaultTextTokenizer.instance(), registry.getTokenizer(DefaultTextTokenizer.NAME));
        assertThrows(MetaDataException.class, () -> registry.getTokenizer(name));
    }
}
