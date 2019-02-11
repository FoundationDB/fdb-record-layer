/*
 * TextCollatorRegistryJRE.java
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

import com.apple.foundationdb.API;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.text.Collator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A text collator registry using the Java Platform's own {@link Collator} implementation.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("checkstyle:abbreviationaswordinname") // Allow JRE here.
public class TextCollatorRegistryJRE implements TextCollatorRegistry {
    public static final TextCollatorRegistryJRE INSTANCE = new TextCollatorRegistryJRE();

    private static final String DEFAULT_LOCALE = "";

    private Map<Pair<String, Integer>, TextCollatorJRE> collators = new ConcurrentHashMap<>();

    /**
     * Get the singleton instance of this registry.
     * @return the text collator registry that used JRE classes.
     */
    @SuppressWarnings("squid:S1845")    // The two are the same.
    public static TextCollatorRegistry instance() {
        return INSTANCE;
    }

    private TextCollatorRegistryJRE() {
    }

    @Override
    @Nonnull
    public TextCollator getTextCollator(int strength) {
        return getTextCollator(DEFAULT_LOCALE, strength);
    }

    @Override
    @Nonnull
    public TextCollator getTextCollator(@Nonnull String locale, int strength) {
        return collators.computeIfAbsent(Pair.of(locale, strength), key -> {
            final Collator collator = DEFAULT_LOCALE.equals(locale) ?
                                      Collator.getInstance() :
                                      // Some minimal consistency between BCP 47 and C-like identifiers.
                                      Collator.getInstance(Locale.forLanguageTag(locale.replace("_", "-")));
            collator.setStrength(strength);
            return new TextCollatorJRE(collator);
        });
    }

    protected static class TextCollatorJRE implements TextCollator {
        @Nonnull
        private final Collator collator;
        
        protected TextCollatorJRE(@Nonnull Collator collator) {
            this.collator = collator;
        }

        @Override
        public int compare(@Nonnull String str1, @Nonnull String str2) {
            return collator.compare(str1, str2);
        }

        @Nonnull
        @Override
        public ByteString getKey(@Nonnull String str) {
            return ByteString.copyFrom(collator.getCollationKey(str).toByteArray());
        }
    }
}
