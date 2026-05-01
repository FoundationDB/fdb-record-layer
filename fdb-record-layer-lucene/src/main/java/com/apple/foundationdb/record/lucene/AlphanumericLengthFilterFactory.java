/*
 * AlphanumericLengthFilterFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.lucene.filter.AlphanumericLengthFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import java.util.Map;

/**
 * A TokenFilterFactory that creates Alphanumeric Length filters.
 */
public final class AlphanumericLengthFilterFactory extends TokenFilterFactory {

    final int min;
    final int max;
    public static final String MIN_KEY = "min";
    public static final String MAX_KEY = "max";

    public AlphanumericLengthFilterFactory(Map<String, String> args) {
        super(args);
        min = requireInt(args, MIN_KEY);
        max = requireInt(args, MAX_KEY);
        if (!args.isEmpty()) {
            throw new IllegalArgumentException("Unknown parameters: " + args);
        }
    }

    @Override
    public TokenStream create(final TokenStream input) {
        return new AlphanumericLengthFilter(input, min, max);
    }
}
