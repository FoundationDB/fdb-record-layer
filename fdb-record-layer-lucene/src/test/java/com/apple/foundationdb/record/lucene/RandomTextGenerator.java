/*
 * RandomTextGenerator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Utility class for generating a bunch of random text.
 * In the future, it would be reasonable for this to use actual words and not randomly generated ones.
 */
class RandomTextGenerator {

    private final Random random;
    private final List<String> words;

    private RandomTextGenerator(final Random random, final List<String> words) {
        this.random = random;
        this.words = words;
    }

    public RandomTextGenerator(Random random) {
        this.random = random;
        this.words = generateRandomWords(random, 10_000);
    }

    /**
     * Create a new version with the same words, but a different seed to pick the words.
     * @param random A new random used for picking words
     * @return a new {@code RandomTextGenerator}
     */
    public RandomTextGenerator withNewRandom(Random random) {
        return new RandomTextGenerator(random, words);
    }

    public String generateRandomText(final String universalWord) {
        int length;
        do {
            length = (int)((random.nextGaussian() * 500) + 1000);
        } while (length <= 0 || length > 10_000);
        StringBuilder builder = new StringBuilder(words.get(random.nextInt(words.size())));
        final int universalWordIndex = random.nextInt(length);
        for (int i = 0; i < length; i++) {
            final String baseWord = i == universalWordIndex ? universalWord : words.get(random.nextInt(words.size()));
            if (random.nextInt(100) == 0) {
                builder.append(". ")
                        .append(Character.toUpperCase(baseWord.charAt(0)))
                        .append(baseWord, 1, baseWord.length());
            } else if (random.nextInt(30) == 0) {
                builder.append(", ")
                        .append(baseWord);
            } else {
                builder.append(" ")
                        .append(baseWord);
            }
        }
        return builder.toString();
    }

    private static List<String> generateRandomWords(Random random, int numberOfWords) {
        assert numberOfWords > 0 : "Number of words have to be greater than 0";
        List<String> words = new ArrayList<>(numberOfWords);
        for (int i = 0; i < numberOfWords; i++) {
            int wordLength;
            do {
                wordLength = (int) (random.nextGaussian() * 2 + 5);
            } while (wordLength <= 0);
            char[] word = new char[wordLength];
            for (int j = 0; j < word.length; j++) {
                word[j] = (char)('a' + random.nextInt(26));
            }
            words.add(new String(word));
        }
        return words;
    }
}
