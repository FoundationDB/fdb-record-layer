/*
 * LuceneScanSpellcheck.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Scan a {@code LUCENE} index for auto-complete suggestions.
 */
@API(API.Status.UNSTABLE)
public class LuceneScanSpellCheck extends LuceneScanBounds {
    @Nonnull
    final List<String> fields;
    @Nonnull
    final String word;

    public LuceneScanSpellCheck(@Nonnull IndexScanType scanType, @Nonnull Tuple groupKey, @Nonnull List<String> fields, @Nonnull String word) {
        super(scanType, groupKey);
        this.fields = fields;
        this.word = word;
    }

    @Nonnull
    public List<String> getFields() {
        return fields;
    }

    @Nonnull
    public String getWord() {
        return word;
    }

    @Override
    public String toString() {
        return super.toString() + " " + word + " in " + fields;
    }
}
