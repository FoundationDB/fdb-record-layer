/*
 * ToUniqueAliasesTranslationMap.java
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
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Translation map that allows for rebasing of graphs in a way that the resulting rebased graph is only using unique
 * aliases. This property is needed when a sub-graph that is pre-compiled is later unified with the current query's
 * graph, and we have to ensure that aliases between the sub-graph and the encompassing graph do not clash.
 * The logic adds a unique source to target mapping when a source is seen for the first time. When the same source is
 * then mapped again, the previously fixed target is returned.
 */
public class ToUniqueAliasesTranslationMap implements TranslationMap {
    @Nonnull
    private final Map<CorrelationIdentifier, CorrelationIdentifier> sourceToTargetMap;

    private ToUniqueAliasesTranslationMap(@Nonnull final AliasMap identities) {
        Verify.verify(identities.definesOnlyIdentities());
        this.sourceToTargetMap = new LinkedHashMap<>();
        identities.forEachMapping(sourceToTargetMap::put);
    }

    @Nonnull
    @Override
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

    @Override
    public boolean definesOnlyIdentities() {
        return false;
    }

    @Override
    public boolean containsSourceAlias(@Nullable CorrelationIdentifier sourceAlias) {
        // we can always translate, therefore all possible aliases are in our map
        return true;
    }

    @Nonnull
    @Override
    public Value applyTranslationFunction(@Nonnull final CorrelationIdentifier sourceAlias,
                                          @Nonnull final LeafValue leafValue) {
        return leafValue.rebaseLeaf(computeTargetIfAbsent(sourceAlias));
    }

    @Nonnull
    private CorrelationIdentifier computeTargetIfAbsent(@Nonnull final CorrelationIdentifier sourceAlias) {
        return sourceToTargetMap.computeIfAbsent(sourceAlias,
                ignored0 -> Quantifier.uniqueId());
    }

    @Nonnull
    public static ToUniqueAliasesTranslationMap newInstance() {
        return new ToUniqueAliasesTranslationMap(AliasMap.emptyMap());
    }

    @Nonnull
    public static ToUniqueAliasesTranslationMap newInstance(@Nonnull final Set<CorrelationIdentifier> constantAliases) {
        return new ToUniqueAliasesTranslationMap(AliasMap.identitiesFor(constantAliases));
    }
}
