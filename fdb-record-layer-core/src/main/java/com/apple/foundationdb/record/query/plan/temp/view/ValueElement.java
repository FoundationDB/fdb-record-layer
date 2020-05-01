/*
 * ValueElement.java
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
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.predicates.ElementPredicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

/**
 * An {@link Element} representing an entire value, such as a scalar or message, obtained from a source.
 *
 * <p>
 * For example, the integer values from a {@link RepeatedFieldSource} on a record's repeated integer field
 * would be represented by a {@code ValueElement}.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class ValueElement extends ElementWithSingleSource {
    public ValueElement(@Nonnull Source source) {
        super(source);
    }

    @Nonnull
    @Override
    public Optional<ComparisonRange> matchWith(@Nonnull ComparisonRange existingComparisons, @Nonnull ElementPredicate predicate) {
        if (predicate.getElement().equals(this)) {
            return existingComparisons.tryToAdd(predicate.getComparison());
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public Optional<ViewExpressionComparisons> matchSourcesWith(@Nonnull ViewExpressionComparisons viewExpressionComparisons,
                                                                @Nonnull Element element) {
        if (element instanceof ValueElement) {
            final ValueElement valueElement = (ValueElement) element;
            if (source.supportsSourceIn(viewExpressionComparisons, valueElement.getSource())) {
                return Optional.of(viewExpressionComparisons.withSourcesMappedInto(valueElement.getSource(), source));
            }
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public Element withSourceMappedInto(@Nonnull Source originalSource, @Nonnull Source duplicateSource) {
        if (source.equals(duplicateSource)) {
            return new ValueElement(originalSource);
        }
        return this;
    }

    @Nullable
    @Override
    public Object eval(@Nonnull SourceEntry sourceEntry) {
        return sourceEntry.getValueFor(source);
    }

    @Override
    public String toString() {
        return source.toString() + " has value";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ValueElement that = (ValueElement)o;
        return Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source);
    }

    @Override
    public int planHash() {
        return 0;
    }
}
