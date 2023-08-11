/*
 * ValueBuggyIndexMaintainerFactory.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Collections;

/**
 * Factory for {@link ValueBuggyIndexMaintainer}.
 */
@AutoService(IndexMaintainerFactory.class)
public class ValueBuggyIndexMaintainerFactory implements IndexMaintainerFactory {
    @Nonnull
    @Override
    public Iterable<String> getIndexTypes() {
        return Collections.singletonList("value_buggy");
    }

    @Nonnull
    @Override
    public IndexValidator getIndexValidator(Index index) {
        return new IndexValidator(index);
    }

    @Nonnull
    @Override
    public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
        return new ValueBuggyIndexMaintainer(state);
    }
}
