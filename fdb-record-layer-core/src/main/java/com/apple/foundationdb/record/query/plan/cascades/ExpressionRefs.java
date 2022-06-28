/*
 * ExpressionRefs.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.properties.ReferencesAndDependenciesProperty;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Utility methods for {@link ExpressionRef}s.
 */
public class ExpressionRefs {
    private ExpressionRefs() {
        // do not instantiate
    }

    public static List<? extends ExpressionRef<? extends RelationalExpression>> translateCorrelations(@Nonnull final List<? extends ExpressionRef<? extends RelationalExpression>> refs,
                                                                                                      @Nonnull final TranslationMap translationMap) {
        if (refs.isEmpty()) {
            return ImmutableList.of();
        }

        final var partialOrder = ReferencesAndDependenciesProperty.evaluate(refs);

        final var expressionRefs =
                TopologicalSort.anyTopologicalOrderPermutation(partialOrder)
                        .orElseThrow(() -> new RecordCoreException("graph has cycles"));

        final var cachedTranslationsMap =
                Maps.<ExpressionRef<? extends RelationalExpression>, ExpressionRef<? extends RelationalExpression>>newIdentityHashMap();

        for (final var expressionRef : expressionRefs) {
            if (expressionRef.getCorrelatedTo().stream().anyMatch(translationMap::containsSourceAlias)) {
                var allMembersSame = true;
                final var translatedMembersBuilder = ImmutableList.<RelationalExpression>builder();
                for (final var member : expressionRef.getMembers()) {
                    var allChildTranslationsSame = true;
                    final var translatedQuantifiersBuilder = ImmutableList.<Quantifier>builder();
                    for (final var quantifier : member.getQuantifiers()) {
                        final var childReference = quantifier.getRangesOver();

                        // these must exist
                        Verify.verify(cachedTranslationsMap.containsKey(childReference));
                        final ExpressionRef<? extends RelationalExpression> translatedChildReference = cachedTranslationsMap.get(childReference);
                        if (translatedChildReference != childReference) {
                            translatedQuantifiersBuilder.add(quantifier.overNewReference(translatedChildReference));
                            allChildTranslationsSame = false;
                        } else {
                            translatedQuantifiersBuilder.add(quantifier);
                        }
                    }

                    final var translatedQuantifiers = translatedQuantifiersBuilder.build();
                    final RelationalExpression translatedMember;

                    // we may not have to translate the current member
                    if (allChildTranslationsSame) {
                        final Set<CorrelationIdentifier> memberCorrelatedTo;
                        if (member instanceof RelationalExpressionWithChildren) {
                            memberCorrelatedTo = ((RelationalExpressionWithChildren)member).getCorrelatedToWithoutChildren();
                        } else {
                            memberCorrelatedTo = member.getCorrelatedTo();
                        }

                        if (memberCorrelatedTo.stream().noneMatch(translationMap::containsSourceAlias)) {
                            translatedMember = member;
                        } else {
                            translatedMember = member.translateCorrelations(translationMap, translatedQuantifiers);
                            allMembersSame = false;
                        }
                    } else {
                        translatedMember = member.translateCorrelations(translationMap, translatedQuantifiers);
                        allMembersSame = false;
                    }
                    translatedMembersBuilder.add(translatedMember);
                }
                if (allMembersSame) {
                    cachedTranslationsMap.put(expressionRef, expressionRef);
                } else {
                    cachedTranslationsMap.put(expressionRef, GroupExpressionRef.from(translatedMembersBuilder.build()));
                }
            } else {
                cachedTranslationsMap.put(expressionRef, expressionRef);
            }
        }

        return refs.stream()
                .map(ref -> Objects.requireNonNull(cachedTranslationsMap.get(ref)))
                .collect(ImmutableList.toImmutableList());
    }
}
