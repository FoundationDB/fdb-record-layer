/*
 * TextCollatorICUTest.java
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

import com.apple.foundationdb.record.provider.common.text.TextCollatorTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Tests for {@link TextCollatorRegistryICU}.
 */
@SuppressWarnings("checkstyle:abbreviationaswordinname") // Allow ICU here.
public class TextCollatorICUTest extends TextCollatorTest {

    public TextCollatorICUTest() {
        super(TextCollatorRegistryICU.instance());
    }

    @Test
    public void localeType() {
        final List<String> names = Arrays.asList("Müller, A", "Mueller, B", "Müller, C");
        final List<String> sortNormal = new ArrayList<>(names);
        sortNormal.sort(registry.getTextCollator("de_DE"));
        final List<String> sortPhonebook = new ArrayList<>(names);
        // I believe in BCP 47 this would be de-DE-co-phonebk, but that doesn't seem to be present in JRE extensions.
        sortPhonebook.sort(registry.getTextCollator("de_DE@collation=PHONEBOOK"));
        assertThat(sortNormal, not(equalTo(names)));
        assertThat(sortPhonebook, equalTo(names));
    }

}
