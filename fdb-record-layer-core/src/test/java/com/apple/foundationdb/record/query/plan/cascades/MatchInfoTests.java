/*
 * MatchInfoTests.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

public class MatchInfoTests {
    @Test
    void testCollectPredicates() {
        final var memoizer = Memoizer.noMemoization(PlannerStage.PLANNED);

        final var fuseQuery = fuse();
        final var fuseCandidate = fuse();
        final var aCandidate = CorrelationIdentifier.of("a'");
        final var quantifierOverFuseCandidate =
                Quantifier.forEach(memoizer.memoizeExploratoryExpression(fuseCandidate), aCandidate);

        final var selectOverFuseCandidate =
                GraphExpansion.builder()
                        .addQuantifier(quantifierOverFuseCandidate)
                        .build().buildSimpleSelectOverQuantifier(quantifierOverFuseCandidate);

        final var matchInfos =
                fuseQuery.exactlySubsumedBy(fuseCandidate, AliasMap.emptyMap(), IdentityBiMap.create(), TranslationMap.empty());
        Assertions.assertThat(matchInfos).hasSizeGreaterThan(0);
        final var matchInfo = Iterables.getFirst(matchInfos, null);
        final var maxMatchMap = matchInfo.getMaxMatchMap();
        final var adjustedMaxMatchMapOptional =
                maxMatchMap.adjustMaybe(aCandidate, selectOverFuseCandidate.getResultValue(), ImmutableSet.of(aCandidate));
        Assertions.assertThat(adjustedMaxMatchMapOptional.isPresent()).isTrue();

        final var adjustedMatch =
                matchInfo.adjustedBuilder()
                        .setMaxMatchMap(adjustedMaxMatchMapOptional.get())
                        .build();

        Assertions.assertThat(adjustedMatch.collectPulledUpPredicateMappings(selectOverFuseCandidate, ImmutableSet.of()))
                .isEmpty();
    }

    @Nonnull
    private static FullUnorderedScanExpression fuse() {
        return new FullUnorderedScanExpression(ImmutableSet.of("someType"),
                someRecordType(), new AccessHints());
    }

    @Nonnull
    private static Type.Record someRecordType() {
        return RuleTestHelper.TYPE_S;
    }

}
