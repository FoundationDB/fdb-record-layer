/*
 * TextCollatorTest.java
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

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * Tests for {@link TextCollator}.
 */
public abstract class TextCollatorTest {

    protected final TextCollatorRegistry registry;

    protected TextCollatorTest(TextCollatorRegistry registry) {
        this.registry = registry;
    }

    /**
     * Test with JRE collators.
     */
    @SuppressWarnings("checkstyle:abbreviationaswordinname") // Allow JRE here.
    public static class TextCollatorJRETest extends TextCollatorTest {
        public TextCollatorJRETest() {
            super(TextCollatorRegistryJRE.instance());
        }
    }

    @Test
    public void primary() {
        final TextCollator collator = registry.getTextCollator();
        assertThat(collator.compare("abc", "abc"), equalTo(0));
        assertThat(collator.compare("abc", "xyz"), lessThan(0));
        assertThat(collator.compare("abc", "ABC"), equalTo(0));
        assertThat(collator.compare("abc", "XYZ"), lessThan(0));
        assertThat(collator.compare("naïve", "naive"), equalTo(0));
    }

    @Test
    public void secondary() {
        final TextCollator collator = registry.getTextCollator(TextCollator.Strength.SECONDARY);
        assertThat(collator.compare("abc", "abc"), equalTo(0));
        assertThat(collator.compare("abc", "xyz"), lessThan(0));
        assertThat(collator.compare("abc", "ABC"), equalTo(0));
        assertThat(collator.compare("abc", "XYZ"), lessThan(0));
        assertThat(collator.compare("Laocoön", "Laocoon"), not(equalTo(0)));
    }

    @Test
    public void tertiary() {
        final TextCollator collator = registry.getTextCollator(TextCollator.Strength.TERTIARY);
        assertThat(collator.compare("abc", "abc"), equalTo(0));
        assertThat(collator.compare("abc", "xyz"), lessThan(0));
        assertThat(collator.compare("abc", "ABC"), lessThan(0));
        assertThat(collator.compare("cooperate", "coöperate"), not(equalTo(0)));
    }

    @Test
    public void locale() {
        final TextCollator collator1 = registry.getTextCollator("en_US");
        final TextCollator collator2 = registry.getTextCollator("sv_SE");
        assertThat(collator1.compare("A", "Ä"), equalTo(0));
        assertThat(collator2.compare("A", "Ä"), lessThan(0));
    }

}
