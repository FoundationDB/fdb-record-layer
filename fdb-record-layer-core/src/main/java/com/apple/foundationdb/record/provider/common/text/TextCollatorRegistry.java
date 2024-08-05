/*
 * TextCollatorRegistry.java
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

import javax.annotation.Nonnull;

/**
 * Registry for {@link TextCollator}s.
 * The registry maps locale name and strength to a thread-safe instance.
 */
@API(API.Status.EXPERIMENTAL)
public interface TextCollatorRegistry {
    /**
     * Get the name of this collator registry.
     * Used for serialization and debugging.
     * @return the name of this collator registry
     */
    @Nonnull
    String getName();

    /**
     * Get a weak text collator for the default locale.
     *
     * In general, this means one that is case- and accent-insensitive.
     * @return a weak text collator for the default locale
     */
    @Nonnull
    default TextCollator getTextCollator() {
        return getTextCollator(0);
    }

    /**
     * Get a text collator of the specified strength for the default locale.
     *
     * @param strength the desired strength
     * @return a weak text collator for the given locale and strength
     */
    @Nonnull
    TextCollator getTextCollator(int strength);

    /**
     * Get a weak text collator for the given locale.
     *
     * In general, this means one that is case- and accent-insensitive.
     * @param locale the name of the target locale
     * @return a weak text collator for the given locale
     */
    @Nonnull
    default TextCollator getTextCollator(@Nonnull String locale) {
        return getTextCollator(locale, 0);
    }

    /**
     * Get a text collator of the specified strength for the given locale.
     *
     * @param locale the name of the target locale
     * @param strength the desired strength
     * @return a weak text collator for the given locale and strength
     */
    @Nonnull
    TextCollator getTextCollator(@Nonnull String locale, int strength);
}
