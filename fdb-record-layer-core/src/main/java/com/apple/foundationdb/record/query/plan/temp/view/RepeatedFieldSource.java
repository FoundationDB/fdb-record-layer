/*
 * RepeatedFieldSource.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageOrBuilder;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A {@link Source} representing the stream of the zero or more values of a repeated field.
 *
 * <p>
 * Encapsulates the logic for fanning-out the values of the repeated field and ensuring that the planner uses repeated
 * fields in the correct way. In particular, the {@link #supportsSourceIn(ViewExpressionComparisons, Source)} method
 * ensures that multiple comparisons are associated with this source only if they can refer to the same iteration over
 * the elements of the repeated field.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class RepeatedFieldSource extends Source {
    @Nonnull
    private final Source source;
    @Nonnull
    private final List<String> fieldNames;

    public RepeatedFieldSource(@Nonnull Source source, @Nonnull List<String> fieldNames) {
        this.source = source;
        this.fieldNames = fieldNames;

        this.source.addDependentSource(this);
    }

    @Nonnull
    public List<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public Set<Source> getSources() {
        final Set<Source> parentSources = source.getSources();
        return ImmutableSet.<Source>builderWithExpectedSize(parentSources.size() + 1)
                .addAll(parentSources)
                .add(source)
                .build();
    }

    @Override
    public boolean supportsSourceIn(@Nonnull ViewExpressionComparisons comparisons, @Nonnull Source other) {
        return !comparisons.hasComparison(this) &&
               other instanceof RepeatedFieldSource &&
               fieldNames.equals(((RepeatedFieldSource)other).fieldNames) &&
               source.supportsSourceIn(comparisons, ((RepeatedFieldSource)other).source);
    }

    @Nonnull
    @Override
    public Source withSourceMappedInto(@Nonnull Source originalSource, @Nonnull Source duplicateSource) {
        if (equals(duplicateSource)) {
            return originalSource;
        }
        final Source mappedChild = source.withSourceMappedInto(originalSource, duplicateSource);
        if (!source.equals(mappedChild)) {
            return new RepeatedFieldSource(mappedChild, fieldNames);
        }
        return this;
    }

    @Nonnull
    @Override
    public Stream<SourceEntry> evalSourceEntriesFor(@Nonnull SourceEntry parentEntry) {
        Object parentValue = parentEntry.getValueFor(source);
        if (!(parentValue instanceof MessageOrBuilder)) {
            throw new RecordCoreException("cannot evaluate repeated field source against non-message type");
        }
        MessageOrBuilder parentMessage = (MessageOrBuilder) parentValue;
        Descriptors.FieldDescriptor field = null;
        String fieldName = "";
        final Iterator<String> iterator = fieldNames.iterator();
        while (iterator.hasNext()) {
            fieldName = iterator.next();
            field = MessageValue.findFieldDescriptorOnMessage(parentMessage, fieldName);

            if (field.isRepeated()) {
                if (iterator.hasNext()) {
                    // Found a repeated field that isn't the last one.
                    throw new RecordCoreException("cannot evaluate a nested repeated field with an internal repeated field.")
                            .addLogInfo("fieldNames", fieldNames)
                            .addLogInfo("illegalRepatedName", fieldName);
                }
            } else {
                parentValue = parentMessage.getField(field);
                if (!(parentValue instanceof MessageOrBuilder)) {
                    throw new RecordCoreException("cannot evaluate repeated field source against non-message type");
                }
                parentMessage = (MessageOrBuilder) parentValue;
            }
        }

        if (!field.isRepeated()) {
            throw new RecordCoreException("cannot evaluate repeated field source against non-repeated field " + fieldName);
        }
        List<SourceEntry> entries = new ArrayList<>();
        for (int i = 0; i < parentMessage.getRepeatedFieldCount(field); i++) {
            entries.add(parentEntry.addSourceValue(this, parentMessage.getRepeatedField(field, i)));
        }
        return entries.stream();
    }

    @Override
    public String toString() {
        return source.toString() + "." + String.join(".", fieldNames) + "@" + Integer.toHexString(hashCode());
    }
}
