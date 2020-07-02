/*
 * ElementWithSingleSource.java
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

import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

/**
 * A base class for all elements that draw from exactly one source.
 */
abstract class ElementWithSingleSource implements Element {
    @Nonnull
    protected final Source source;

    public ElementWithSingleSource(@Nonnull Source source) {
        this.source = source;
    }

    @Nonnull
    public Source getSource() {
        return source;
    }

    @Override
    public Set<Source> getAncestralSources() {
        return source.getSources();
    }


    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        // TODO create the correlation identifier
        return ImmutableSet.of();
    }

    @Override
    public boolean resultEquals(@Nullable final Object other, @Nonnull final AliasMap equivalenceMap) {
        return equals(other); // TODO this should be adapted
    }
}
