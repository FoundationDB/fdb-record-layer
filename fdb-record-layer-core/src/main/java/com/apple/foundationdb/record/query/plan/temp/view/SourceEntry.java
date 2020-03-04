/*
 * SourceEntry.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.view;

import com.apple.foundationdb.annotation.API;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;

/**
 * A single entity produced by a source from a record, mapping each (inclusive) dependant source to a concrete value.
 *
 * <p>
 * Given a record, the {@link Source#evalSourceEntriesFor} method produces a stream of {@code SourceEntry}s. Each entry
 * is much like a named tuple in the relational model; each source is mapped to an object containing a concrete value.
 * In the case of multi-valued sources, one source entry is generated for each of those repeated values. A source entry
 * from a particular source includes mappings from all dependent sources from which that source is derived.
 * A {@code SourceEntry} object is immutable but supports the builder method {@link #addSourceValue(Source, Object)} to
 * generate a new source with one additional value.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class SourceEntry {
    @Nonnull
    public static final SourceEntry EMPTY = new SourceEntry(Collections.emptyMap());

    @Nonnull
    private final Map<Source, Object> sourceValues;

    private SourceEntry(@Nonnull Map<Source, Object> sourceValues) {
        this.sourceValues = sourceValues;
    }

    @Nonnull
    public Object getValueFor(@Nonnull Source source) {
        if (!(sourceValues.containsKey(source))) {
            throw new UnsupportedOperationException("tried to get source that is not present");
        }
        return sourceValues.get(source);
    }

    @Nonnull
    public SourceEntry addSourceValue(@Nonnull Source source, @Nonnull Object object) {
        return new SourceEntry(
                ImmutableMap.<Source, Object>builderWithExpectedSize(sourceValues.size() + 1)
                        .putAll(sourceValues)
                        .put(source, object)
                        .build());
    }

    public static SourceEntry from(@Nonnull Source source, @Nonnull Object object) {
        return new SourceEntry(Collections.singletonMap(source, object));
    }
}
