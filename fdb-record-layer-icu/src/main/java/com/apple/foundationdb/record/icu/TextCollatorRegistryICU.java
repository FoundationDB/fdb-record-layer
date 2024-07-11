/*
 * TextCollatorRegistryICU.java
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

package com.apple.foundationdb.record.icu;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.common.text.TextCollator;
import com.apple.foundationdb.record.provider.common.text.TextCollatorRegistry;
import com.apple.foundationdb.record.util.MapUtils;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyByteString;
import com.ibm.icu.text.Collator;
import com.ibm.icu.util.ULocale;

import javax.annotation.Nonnull;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A text collator registry using ICU4J's {@link Collator} implementation.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("checkstyle:abbreviationaswordinname") // Allow ICU here.
public class TextCollatorRegistryICU implements TextCollatorRegistry {
    public static final TextCollatorRegistryICU INSTANCE = new TextCollatorRegistryICU();

    private static final String DEFAULT_LOCALE = "";

    private Map<NonnullPair<String, Integer>, TextCollatorICU> collators = new ConcurrentHashMap<>();

    /**
     * Get the singleton instance of this registry.
     * @return the text collator registry that used ICU classes.
     */
    @SuppressWarnings("squid:S1845")    // The two are the same.
    public static TextCollatorRegistry instance() {
        return INSTANCE;
    }

    private TextCollatorRegistryICU() {
    }

    @Override
    @Nonnull
    public TextCollator getTextCollator(int strength) {
        return getTextCollator(DEFAULT_LOCALE, strength);
    }

    @Override
    @Nonnull
    public TextCollator getTextCollator(@Nonnull String locale, int strength) {
        return MapUtils.computeIfAbsent(collators, NonnullPair.of(locale, strength), key -> {
            final Collator collator = DEFAULT_LOCALE.equals(locale) ?
                                      Collator.getInstance(ULocale.forLocale(Locale.ROOT)) :
                                      Collator.getInstance(new ULocale(locale));
            collator.setStrength(strength);
            return new TextCollatorICU(collator.freeze());
        });
    }

    protected static class TextCollatorICU implements TextCollator {
        @Nonnull
        private final Collator collator;
        
        protected TextCollatorICU(@Nonnull Collator collator) {
            this.collator = collator;
        }

        @Override
        public int compare(@Nonnull String str1, @Nonnull String str2) {
            return collator.compare(str1, str2);
        }

        @Nonnull
        @Override
        public ByteString getKey(@Nonnull String str) {
            return ZeroCopyByteString.wrap(collator.getCollationKey(str).toByteArray());
        }
    }
}
