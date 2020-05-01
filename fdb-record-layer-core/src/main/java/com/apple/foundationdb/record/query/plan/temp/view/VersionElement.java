/*
 * VersionElement.java
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

import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.predicates.ElementPredicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * An {@link Element} representing a serialized versionstamp in a query or index entry. Since versions are associated
 * with individual index entries rather than records <em>per se</em>, a version element does not have a source.
 */
public class VersionElement implements Element {
    @Nonnull
    @Override
    public Optional<ComparisonRange> matchWith(@Nonnull ComparisonRange existingComparisons, @Nonnull ElementPredicate predicate) {
        if (equals(predicate.getElement())) {
            return existingComparisons.tryToAdd(predicate.getComparison());
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public Optional<ViewExpressionComparisons> matchSourcesWith(@Nonnull ViewExpressionComparisons viewExpressionComparisons, @Nonnull Element element) {
        return Optional.empty();
    }

    @Nonnull
    @Override
    public Set<Source> getAncestralSources() {
        return Collections.emptySet();
    }

    @Nonnull
    @Override
    public Element withSourceMappedInto(@Nonnull Source originalSource, @Nonnull Source duplicateSource) {
        return this;
    }

    @Nullable
    @Override
    public Object eval(@Nonnull SourceEntry sourceEntry) {
        throw new UnsupportedOperationException("Cannot evaluate version elements yet");
    }

    @Override
    public int planHash() {
        return 1;
    }
}
