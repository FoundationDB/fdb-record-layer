/*
 * TableFunctionExpressionTest.java
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link TableFunctionExpression}, in particular how an explode in a query matches an explode in a match
 * candidate. The query and the candidate explode the same field of a record, but are correlated to different aliases,
 * which the binding alias map identifies, as they are when the planner matches them.
 */
class TableFunctionExpressionTest {
    @Nonnull
    private static final Type LONG = Type.primitiveType(Type.TypeCode.LONG, false);
    @Nonnull
    private static final Type INT = Type.primitiveType(Type.TypeCode.INT, false);
    @Nonnull
    private static final Type.Record RECORD_TYPE = Type.Record.fromFields(List.of(
            Type.Record.Field.of(new Type.Array(LONG), Optional.of("values")),
            Type.Record.Field.of(new Type.Array(LONG), Optional.of("others")),
            Type.Record.Field.of(new Type.Array(INT), Optional.of("ints"))));
    /**
     * A record whose first field is named like that of {@link #RECORD_TYPE} but holds elements of another type. A field
     * value compares its field by ordinal and a quantified value disregards its type, so a collection of this record
     * compares equal to the same collection of {@link #RECORD_TYPE}.
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
    void explodeNotPreservingItsOrderIsSubsumedByOneThatDoes() {
        final var query = explode(QUERY_ALIAS, "values", false);
        final var candidate = explode(CANDIDATE_ALIAS, "values", true);

        // The query does not observe its ordinals, so the candidate produces everything the query may produce.
        assertMatchesWholeStruct(query, candidate, LONG);
    }

    @Test
    void explodeOfElementsOfTheOrdinalTypeIsSubsumedByOneThatPreservesItsOrder() {
        final var query = explode(QUERY_ALIAS, "ints", false);
        final var candidate = explode(CANDIDATE_ALIAS, "ints", true);

        // The element is of the same type as the ordinal, the struct is still matched as a whole.
        assertMatchesWholeStruct(query, candidate, INT);
    }

    @Test
    void explodeIsNotSubsumedByAnExplodeOfElementsOfAnotherType() {
        final var queryCollection = collection(QUERY_ALIAS, "values");
        final var candidateCollection = collection(CANDIDATE_ALIAS, OTHER_RECORD_TYPE, "values");
        final var query = new TableFunctionExpression(new ExplodeValue(queryCollection, false));
        final var candidate = new TableFunctionExpression(new ExplodeValue(candidateCollection, true));

        // The collections compare equal, but the explodes flow structs of different types.
        assertThat(queryCollection.semanticEquals(candidateCollection, BINDING_ALIAS_MAP)).isTrue();
        assertThat(subsumedBy(query, candidate, BINDING_ALIAS_MAP)).isEmpty();
    }

    @Test
    void explodeIsSubsumedByAnEqualExplode() {
        assertMatchesWholeStruct(explode(QUERY_ALIAS, "values", false), explode(CANDIDATE_ALIAS, "values", false),
                LONG);
        assertMatchesWholeStruct(explode(QUERY_ALIAS, "values", true), explode(CANDIDATE_ALIAS, "values", true),
                LONG);
    }

    @Test
    void explodePreservingItsOrderIsNotSubsumedByOneThatDoesNot() {
        // The query observes its ordinals, which the candidate does not number in the order of the array.
        assertThat(subsumedBy(explode(QUERY_ALIAS, "values", true), explode(CANDIDATE_ALIAS, "values", false),
                BINDING_ALIAS_MAP)).isEmpty();
    }

    @Test
    void sortedExplodeIsNotSubsumedByOneThatPreservesItsOrder() {
        final var query = new TableFunctionExpression(
                new ExplodeValue(collection(QUERY_ALIAS, "values"), false, true, false));
        final var candidate = explode(CANDIDATE_ALIAS, "values", true);

        // The candidate does not produce the elements in the order the query asks for.
        assertThat(subsumedBy(query, candidate, BINDING_ALIAS_MAP)).isEmpty();
    }

    @Test
    void explodeIsNotSubsumedByAnExplodeOfAnotherCollection() {
        assertThat(subsumedBy(explode(QUERY_ALIAS, "values", false), explode(CANDIDATE_ALIAS, "others", true),
                BINDING_ALIAS_MAP)).isEmpty();
    }

    @Test
    void explodeIsNotSubsumedByAnExplodeOfAnUnboundCorrelation() {
        // Without the binding, the collections are over different records.
        assertThat(subsumedBy(explode(QUERY_ALIAS, "values", false), explode(CANDIDATE_ALIAS, "values", true),
                AliasMap.emptyMap())).isEmpty();
    }

    /**
     * Asserts that the query explode is subsumed by the candidate explode with a single match whose max match map
     * maps the struct the query flows to the struct the candidate flows.
     */
    private static void assertMatchesWholeStruct(@Nonnull final TableFunctionExpression query,
                                                 @Nonnull final TableFunctionExpression candidate,
                                                 @Nonnull final Type elementType) {
        final var matchInfos = ImmutableList.copyOf(subsumedBy(query, candidate, BINDING_ALIAS_MAP));
        assertThat(matchInfos).hasSize(1);
        final var maxMatchMap = Iterables.getOnlyElement(matchInfos).getMaxMatchMap();
        assertThat(maxMatchMap.getQueryValue()).isEqualTo(query.getResultValue());
        assertThat(maxMatchMap.getCandidateValue()).isEqualTo(candidate.getResultValue());
        assertThat(maxMatchMap.getMap())
                .containsExactly(Map.entry(query.getResultValue(), candidate.getResultValue()));
        // the mapped value is the (element, ordinal) struct both explodes flow
        assertThat(((Type.Record)maxMatchMap.getQueryValue().getResultType()).getFields())
                .extracting(Type.Record.Field::getFieldType)
                .containsExactly(elementType, INT);
    }

    @Nonnull
    private static TableFunctionExpression explode(@Nonnull final CorrelationIdentifier alias,
                                                   @Nonnull final String fieldName,
                                                   final boolean preserveOrder) {
        return new TableFunctionExpression(new ExplodeValue(collection(alias, fieldName), preserveOrder));
    }

    @Nonnull
    private static Value collection(@Nonnull final CorrelationIdentifier alias, @Nonnull final String fieldName) {
        return collection(alias, RECORD_TYPE, fieldName);
    }

    @Nonnull
    private static Value collection(@Nonnull final CorrelationIdentifier alias, @Nonnull final Type.Record recordType,
                                    @Nonnull final String fieldName) {
        return FieldValue.ofFieldName(QuantifiedObjectValue.of(alias, recordType), fieldName);
    }

    @Nonnull
    private static Iterable<MatchInfo> subsumedBy(@Nonnull final TableFunctionExpression query,
                                                  @Nonnull final TableFunctionExpression candidate,
                                                  @Nonnull final AliasMap bindingAliasMap) {
        return query.subsumedBy(candidate, bindingAliasMap, IdentityBiMap.create(), EvaluationContext.empty());
    }
}
