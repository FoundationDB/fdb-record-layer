/*
 * FieldElement.java
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.predicates.ElementPredicate;
import com.google.protobuf.MessageOrBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * An element that extracts a single, non-repeated field (at an arbitrary nesting depth) from a source.
 *
 * <p>
 * Note that while nested fields can be referenced from a single {@code FieldElement}, repeated fields cannot because
 * they define their own stream of data values, which must be represented by a {@link RepeatedFieldSource}.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.OverrideBothEqualsAndHashcode")
public class FieldElement extends ElementWithSingleSource {
    @Nonnull
    private final List<String> fieldNames;

    public FieldElement(@Nonnull Source source, @Nonnull String fieldName) {
        this(source, Collections.singletonList(fieldName));
    }

    public FieldElement(@Nonnull Source source, @Nonnull List<String> fieldNames) {
        super(source);
        this.fieldNames = fieldNames;
    }

    @Nonnull
    public List<String> getFieldNames() {
        return fieldNames;
    }

    @Nonnull
    @Override
    public FieldElement withSourceMappedInto(@Nonnull Source originalSource, @Nonnull Source duplicateSource) {
        if (source.equals(duplicateSource)) {
            return new FieldElement(originalSource, fieldNames);
        }
        return this;
    }

    @Nonnull
    @Override
    public Optional<ComparisonRange> matchWith(@Nonnull ComparisonRange existingComparisons,
                                               @Nonnull ElementPredicate predicate) {
        if (predicate.getElement().equals(this)) {
            return existingComparisons.tryToAdd(predicate.getComparison());
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public Optional<ViewExpressionComparisons> matchSourcesWith(@Nonnull ViewExpressionComparisons viewExpressionComparisons,
                                                                @Nonnull Element element) {
        if (element instanceof FieldElement) {
            final FieldElement fieldElement = (FieldElement) element;
            if (fieldElement.getFieldNames().equals(fieldNames) &&
                    source.supportsSourceIn(viewExpressionComparisons, fieldElement.getSource())) {
                return Optional.of(viewExpressionComparisons.withSourcesMappedInto(fieldElement.getSource(), source));
            }
        }
        return Optional.empty();
    }

    @Nullable
    @Override
    public Object eval(@Nonnull SourceEntry sourceEntry) {
        Object value = sourceEntry.getValueFor(source);

        if (!(value instanceof MessageOrBuilder)) {
            return null;
        }
        return MessageValue.getFieldValue((MessageOrBuilder) value, fieldNames);
    }

    @Override
    public String toString() {
        return source.toString() + "." + String.join(".", fieldNames);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldElement that = (FieldElement)o;
        return Objects.equals(source, that.source) &&
               Objects.equals(fieldNames, that.fieldNames);
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(source, fieldNames);
    }

    @Override
    public int planHash(PlanHashKind hashKind) {
        if (fieldNames.size() == 1) {
            return fieldNames.get(0).hashCode();
        } else {
            return PlanHashable.iterablePlanHash(hashKind, fieldNames);
        }
    }

    @Nonnull
    @Override
    public FieldElement rebase(@Nonnull final AliasMap translationMap) {
        return new FieldElement(getSource(), getFieldNames()); // TODO needs to be rebased
    }
}
