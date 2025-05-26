/*
 * InferredTranslationMap.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values.translation;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.values.LeafValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Map used to specify translations. TODO
 */
public class InferredTranslationMap implements TranslationMap {
    @Nonnull
    private final Map<CorrelationIdentifier, CorrelationIdentifier> sourceToTargetMap;

    public InferredTranslationMap() {
        this.sourceToTargetMap = new LinkedHashMap<>();
    }

    @Nonnull
    public Optional<AliasMap> getAliasMapMaybe() {
        return Optional.empty();
    }

    @Nonnull
    public AliasMap getSnapshotAliasMap() {
        return AliasMap.copyOf(sourceToTargetMap);
    }

    @Nullable
    @Override
    public CorrelationIdentifier getTarget(@Nonnull final CorrelationIdentifier sourceAlias) {
        return computeTargetIfAbsent(sourceAlias);
    }

    public boolean definesOnlyIdentities() {
        return false;
    }

    public boolean containsSourceAlias(@Nullable CorrelationIdentifier sourceAlias) {
        // we can always translate, therefore all possible aliases are in our map
        return true;
    }

    @Nonnull
    public Value applyTranslationFunction(@Nonnull final CorrelationIdentifier sourceAlias,
                                          @Nonnull final LeafValue leafValue) {
        return leafValue.rebaseLeaf(computeTargetIfAbsent(sourceAlias));
    }

    @Nonnull
    private CorrelationIdentifier computeTargetIfAbsent(@Nonnull final CorrelationIdentifier sourceAlias) {
        return sourceToTargetMap.computeIfAbsent(sourceAlias,
                ignored0 -> Quantifier.uniqueID());
    }
}
