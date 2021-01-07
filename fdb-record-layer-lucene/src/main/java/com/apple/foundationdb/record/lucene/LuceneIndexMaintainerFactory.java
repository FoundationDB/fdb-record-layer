/*
 * LuceneIndexMaintainerFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

/**
 * Index Maintainer Factory for Lucene Indexes.  This adds the FULL_TEXT index to supported indexes.
 *
 */
@AutoService(IndexMaintainerFactory.class)
@API(API.Status.UNSTABLE)
public class LuceneIndexMaintainerFactory implements IndexMaintainerFactory {

    @Nonnull
    private static final List<String> TYPES = Collections.singletonList(IndexTypes.FULL_TEXT);

    @Override
    public Iterable<String> getIndexTypes() {
        return TYPES;
    }

    @Nonnull
    @Override
    public IndexValidator getIndexValidator(Index index) {
        return new IndexValidator(index) {
            @Override
            public void validate(@Nonnull MetaDataValidator metaDataValidator) {
                // nothing to validate
            }
        };
    }

    @Override
    public IndexMaintainer getIndexMaintainer(final IndexMaintainerState state) {
        return new LuceneIndexMaintainer(state);
    }
}
