/*
 * TableFunctionExpressionAutoQATest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ExplodeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional tests for {@link TableFunctionExpression}, focusing on matching explodes and on the consistency of
 * equality, hashing and matching.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class TableFunctionExpressionAutoQATest {
    @Nonnull
    private static final Type LONG = Type.primitiveType(Type.TypeCode.LONG, false);
    @Nonnull
    private static final Type INT = Type.primitiveType(Type.TypeCode.INT, false);
    @Nonnull
    private static final Type.Record RECORD_TYPE = Type.Record.fromFields(List.of(
            Type.Record.Field.of(new Type.Array(LONG), Optional.of("values"))));
    /**
     * Same field name and ordinal as {@link #RECORD_TYPE}, but with elements of another type.
     */
    @Nonnull
    private static final Type.Record OTHER_RECORD_TYPE = Type.Record.fromFields(List.of(
            Type.Record.Field.of(new Type.Array(INT), Optional.of("values"))));
    @Nonnull
    private static final CorrelationIdentifier QUERY_ALIAS = CorrelationIdentifier.of("q");
    @Nonnull
    private static final CorrelationIdentifier CANDIDATE_ALIAS = CorrelationIdentifier.of("c");
    @Nonnull
    private static final AliasMap BINDING_ALIAS_MAP = AliasMap.ofAliases(QUERY_ALIAS, CANDIDATE_ALIAS);

    @Test
    void semanticEqualsOrderPreservingAndNonPreservingIsFalse() {
        // Matching is one-directional and must not be confused with equality: the explodes are not equal either way.
        final var query = explode(QUERY_ALIAS, false);
        final var candidate = explode(CANDIDATE_ALIAS, true);

        assertThat(subsumedBy(query, candidate, BINDING_ALIAS_MAP)).hasSize(1);
        assertThat(query.semanticEquals(candidate, BINDING_ALIAS_MAP)).isFalse();
        assertThat(candidate.semanticEquals(query, BINDING_ALIAS_MAP.inverse())).isFalse();
        assertThat(query.equalsWithoutChildren(candidate, BINDING_ALIAS_MAP)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void semanticEqualsUnderAliasMapHasEqualHashCodes(final boolean preserveOrder) {
        // Explodes that are equal under the binding alias map must have equal semantic hash codes.
        final var query = explode(QUERY_ALIAS, preserveOrder);
        final var candidate = explode(CANDIDATE_ALIAS, preserveOrder);

        assertThat(query.semanticEquals(candidate, BINDING_ALIAS_MAP)).isTrue();
        assertThat(query.semanticHashCode()).isEqualTo(candidate.semanticHashCode());
        assertThat(query.hashCodeWithoutChildren()).isEqualTo(candidate.hashCodeWithoutChildren());
        // the same explode over the same alias is equal by equals() as well
        final var same = explode(QUERY_ALIAS, preserveOrder);
        assertThat(query).isEqualTo(same);
        assertThat(query.hashCode()).isEqualTo(same.hashCode());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void translateCorrelationsKeepsOrderFlag(final boolean preserveOrder) {
        // Translating the correlation of an explode keeps whether it preserves its order, so the translated query
        // explode is equal to the candidate explode without an alias map.
        final var query = explode(QUERY_ALIAS, preserveOrder);
        final var candidate = explode(CANDIDATE_ALIAS, preserveOrder);

        final var translated = query.translateCorrelations(TranslationMap.ofAliases(QUERY_ALIAS, CANDIDATE_ALIAS),
                true, ImmutableList.of());

        assertThat(translated.getCorrelatedTo()).containsExactly(CANDIDATE_ALIAS);
        assertThat(((ExplodeValue)translated.getValue()).preservesOrder()).isEqualTo(preserveOrder);
        assertThat(translated.semanticEquals(candidate, AliasMap.emptyMap())).isTrue();
        assertThat(translated.semanticEquals(explode(CANDIDATE_ALIAS, !preserveOrder), AliasMap.emptyMap()))
                .isFalse();
    }

    @Test
    void subsumedByReverseSortedQueryAgainstOrderPreservingCandidateIsEmpty() {
        // A descending sorted explode is not subsumed by an explode that preserves its order, nor by an ascending one.
        final var query = new TableFunctionExpression(
                new ExplodeValue(collection(QUERY_ALIAS, RECORD_TYPE), false, true, true));

        assertThat(subsumedBy(query, explode(CANDIDATE_ALIAS, true), BINDING_ALIAS_MAP)).isEmpty();
        assertThat(subsumedBy(query, new TableFunctionExpression(
                new ExplodeValue(collection(CANDIDATE_ALIAS, RECORD_TYPE), false, true, false)), BINDING_ALIAS_MAP))
                .isEmpty();
        // the same sorted explode is subsumed
        assertThat(subsumedBy(query, new TableFunctionExpression(
                new ExplodeValue(collection(CANDIDATE_ALIAS, RECORD_TYPE), false, true, true)), BINDING_ALIAS_MAP))
                .hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void translateCorrelationsKeepsDistinctFlag(final boolean shouldSimplifyValues) {
        // Translating the correlation of a distinct candidate explode keeps it distinct, so it is still not equal to the
        // explode of a query, and a query explode matched against it still requires an existential.
        final var distinct = distinctExplode(QUERY_ALIAS);

        final var translated = distinct.translateCorrelations(
                TranslationMap.ofAliases(QUERY_ALIAS, CANDIDATE_ALIAS), shouldSimplifyValues, ImmutableList.of());

        assertThat(translated.getCorrelatedTo()).containsExactly(CANDIDATE_ALIAS);
        assertThat(((ExplodeValue)translated.getValue()).isDistinct()).isTrue();
        assertThat(translated).isEqualTo(distinctExplode(CANDIDATE_ALIAS));
        assertThat(translated).isNotEqualTo(explode(CANDIDATE_ALIAS, false));
        final var matchInfos = subsumedBy(explode(QUERY_ALIAS, false), translated, BINDING_ALIAS_MAP);
        assertThat(matchInfos).hasSize(1);
        assertThat(matchInfos.get(0).requiresExistential()).isTrue();
    }

    @Test
    void distinctAndNonDistinctTableFunctionsAreNotEqual() {
        // The table function of a distinct candidate explode is never equal to that of a query explode, either way,
        // with or without the binding alias map, and equal table functions have equal hash codes.
        final var query = explode(QUERY_ALIAS, false);
        final var distinct = distinctExplode(QUERY_ALIAS);
        final var distinctCandidate = distinctExplode(CANDIDATE_ALIAS);

        assertThat(query).isNotEqualTo(distinct);
        assertThat(distinct).isNotEqualTo(query);
        assertThat(query.semanticEquals(distinct)).isFalse();
        assertThat(distinct.semanticEquals(query)).isFalse();
        assertThat(query.equalsWithoutChildren(distinctCandidate, BINDING_ALIAS_MAP)).isFalse();
        assertThat(distinctCandidate.equalsWithoutChildren(query, BINDING_ALIAS_MAP.inverse())).isFalse();
        assertThat(distinct.semanticEquals(distinctCandidate, BINDING_ALIAS_MAP)).isTrue();
        assertThat(distinct.semanticHashCode()).isEqualTo(distinctCandidate.semanticHashCode());
        assertThat(distinct).isEqualTo(distinctExplode(QUERY_ALIAS));
        assertThat(distinct.hashCode()).isEqualTo(distinctExplode(QUERY_ALIAS).hashCode());
    }

    @ParameterizedTest
    @ValueSource(strings = {"equal", "preserving", "distinct"})
    void onlyADistinctCandidateRequiresAnExistential(@Nonnull final String candidateKind) {
        // A query explode that neither preserves its order nor is sorted is subsumed by an equal explode, by one that
        // preserves its order, and by a distinct one; only the distinct one loses repeated elements, so only its match
        // requires an existential.
        final var query = explode(QUERY_ALIAS, false);
        final TableFunctionExpression candidate;
        switch (candidateKind) {
            case "equal":
                candidate = explode(CANDIDATE_ALIAS, false);
                break;
            case "preserving":
                candidate = explode(CANDIDATE_ALIAS, true);
                break;
            default:
                candidate = distinctExplode(CANDIDATE_ALIAS);
                break;
        }

        final var matchInfos = subsumedBy(query, candidate, BINDING_ALIAS_MAP);
        assertThat(matchInfos).hasSize(1);
        assertThat(matchInfos.get(0).requiresExistential()).isEqualTo("distinct".equals(candidateKind));
        // the match is a leaf match without child matches
        assertThat(matchInfos.get(0).getRegularMatchInfo().getPartialMatchMap().isEmpty()).isTrue();
    }

    @Nonnull
    private static TableFunctionExpression distinctExplode(@Nonnull final CorrelationIdentifier alias) {
        return new TableFunctionExpression(
                new ExplodeValue(collection(alias, RECORD_TYPE), false, false, false, true));
    }

    @Nonnull
    private static TableFunctionExpression explode(@Nonnull final CorrelationIdentifier alias,
                                                   final boolean preserveOrder) {
        return new TableFunctionExpression(new ExplodeValue(collection(alias, RECORD_TYPE), preserveOrder));
    }

    @Nonnull
    private static Value collection(@Nonnull final CorrelationIdentifier alias, @Nonnull final Type.Record recordType) {
        return FieldValue.ofFieldName(QuantifiedObjectValue.of(alias, recordType), "values");
    }

    @Nonnull
    private static List<MatchInfo> subsumedBy(@Nonnull final TableFunctionExpression query,
                                              @Nonnull final TableFunctionExpression candidate,
                                              @Nonnull final AliasMap bindingAliasMap) {
        return ImmutableList.copyOf(query.subsumedBy(candidate, bindingAliasMap, IdentityBiMap.create(),
                EvaluationContext.empty()));
    }
}
