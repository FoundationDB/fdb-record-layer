/*
 * RecordTypeSource.java
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
import com.google.protobuf.MessageOrBuilder;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A {@link Source} representing the stream of all records of a particular type or types.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordTypeSource extends Source {
    @Nonnull
    private final Set<String> recordTypeNames;

    public RecordTypeSource(@Nonnull Set<String> recordTypeNames) {
        super();
        this.recordTypeNames = recordTypeNames;
    }

    @Nonnull
    public Set<String> getRecordTypeNames() {
        return recordTypeNames;
    }

    @Override
    public Set<Source> getSources() {
        return Collections.singleton(this);
    }

    @Override
    public boolean supportsSourceIn(@Nonnull ViewExpressionComparisons comparisons, @Nonnull Source other) {
        return !comparisons.hasComparison(this) &&
               other instanceof RecordTypeSource &&
               recordTypeNames.equals(((RecordTypeSource)other).getRecordTypeNames());
    }

    @Nonnull
    @Override
    public RecordTypeSource withSourceMappedInto(@Nonnull Source originalSource, @Nonnull Source duplicateSource) {
        if (equals(duplicateSource)) {
            return (RecordTypeSource) originalSource;
        }
        return this;
    }

    @Nonnull
    @Override
    protected Stream<SourceEntry> evalSourceEntriesFor(@Nonnull SourceEntry entry) {
        Object value = entry.getValueFor(this);
        if (!(value instanceof MessageOrBuilder)) {
            return Stream.empty();
        }
        MessageOrBuilder message = (MessageOrBuilder) value;
        if (recordTypeNames.contains(message.getDescriptorForType().getName())) {
            return Stream.of(SourceEntry.from(this, message));
        } else {
            return Stream.empty();
        }
    }

    @Override
    public String toString() {
        final String prefix;
        if (recordTypeNames.size() == 1) {
            prefix = recordTypeNames.iterator().next();
        } else {
            prefix = "[" + String.join(", ", recordTypeNames) + "]";
        }
        return prefix + "@" + Integer.toHexString(hashCode());
    }
}
