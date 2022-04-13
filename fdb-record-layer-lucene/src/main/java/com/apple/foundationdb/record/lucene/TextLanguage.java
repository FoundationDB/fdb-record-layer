/*
 * TextLanguage.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Languages supported for Lucene search.
 */
public enum TextLanguage {
    ENGLISH("e"),
    SIMPLIFIED_CHINESE("c"),
    JAPANESE("j");

    @Nonnull
    private String uniqueIdentifier;

    TextLanguage(@Nonnull String uniqueIdentifier) {
        this.uniqueIdentifier = uniqueIdentifier;
    }

    /**
     * Different languages must have different identifiers.
     */
    @Nonnull
    public String uniqueIdentifier() {
        return uniqueIdentifier;
    }

    @Nonnull
    public static TextLanguage defaultLanguage() {
        return ENGLISH;
    }

    @Nonnull
    public static TextLanguage getLanguageForText(@Nonnull String text) {
        return getLanguageForTexts(Collections.singletonList(text));
    }

    @Nonnull
    public static TextLanguage getLanguageForTexts(@Nonnull List<String> texts) {
        TextLanguage bestLanguage = defaultLanguage();
        if (texts.isEmpty()) {
            return bestLanguage;
        }

        int koreanCount = 0;
        int chineseCount = 0;
        int japaneseCount = 0;
        int otherIncludingEnglish = 0;
        for (String text : texts) {
            for (char codePoint : text.toCharArray()) {
                Character.UnicodeScript script = Character.UnicodeScript.of(codePoint);
                if (script == Character.UnicodeScript.HANGUL) {
                    koreanCount++;
                } else if (script == Character.UnicodeScript.HIRAGANA
                           || script == Character.UnicodeScript.KATAKANA) {
                    japaneseCount++;
                } else if (script == Character.UnicodeScript.HAN) {
                    chineseCount++;
                } else {
                    otherIncludingEnglish++;
                }
            }
        }

        TreeMap<TextLanguage, Integer> counts = new TreeMap<>();

        counts.put(JAPANESE, japaneseCount);
        counts.put(SIMPLIFIED_CHINESE, chineseCount + koreanCount); // TODO get a separate field for Korean
        counts.put(ENGLISH, otherIncludingEnglish);

        int bestCount = 0;

        for (Map.Entry<TextLanguage, Integer> entry : counts.entrySet()) {
            if (entry.getValue() > bestCount) {
                bestCount = entry.getValue();
                bestLanguage = entry.getKey();
            }
        }

        return bestLanguage;
    }
}
