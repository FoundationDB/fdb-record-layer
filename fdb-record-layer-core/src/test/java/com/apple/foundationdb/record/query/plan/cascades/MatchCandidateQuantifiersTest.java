/*
 * MatchCandidateQuantifiersTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the graphs of match candidates only contain for-each quantifiers. The planner adjusts a match only along
 * the quantifier of a candidate expression (see {@link com.apple.foundationdb.record.query.plan.cascades.rules.AdjustMatchRule}),
 * and an adjusted match carries over whether the match it adjusts requires an existential
 * (see {@link MatchInfo#requiresExistential()}). That is only sound as long as no adjustment is along an existential
 * quantifier, where such a requirement is absorbed instead, which these tests ensure by ensuring that candidates do not
 * contain existential quantifiers in the first place.
 */
class MatchCandidateQuantifiersTest {
    @Nonnull
    private static final RecordMetaData SIMPLE_META_DATA = simpleMetaData();
    @Nonnull
    private static final RecordMetaData RESTAURANT_META_DATA = restaurantMetaData();

    @ParameterizedTest(name = "{1}")
    @MethodSource("indexes")
    void indexMatchCandidatesOnlyContainForEachQuantifiers(@Nonnull final RecordMetaData metaData,
                                                           @Nonnull final String indexName) {
        final var matchCandidates = ImmutableList.copyOf(IndexMaintainerFactoryRegistryImpl.instance()
                .createMatchCandidates(metaData, metaData.getIndex(indexName), false));

        assertThat(matchCandidates).isNotEmpty();
        matchCandidates.forEach(matchCandidate ->
                assertThat(quantifiersOf(matchCandidate)).isNotEmpty().allMatch(Quantifier.ForEach.class::isInstance));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("recordTypes")
    void primaryScanMatchCandidatesOnlyContainForEachQuantifiers(@Nonnull final RecordMetaData metaData,
                                                                 @Nonnull final String recordTypeName) {
        final var matchCandidate = MatchCandidateExpansion.fromPrimaryDefinition(metaData, Set.of(recordTypeName),
                metaData.getRecordType(recordTypeName).getPrimaryKey(), false);

        assertThat(matchCandidate).isPresent();
        assertThat(quantifiersOf(matchCandidate.get())).isNotEmpty().allMatch(Quantifier.ForEach.class::isInstance);
    }

    @Nonnull
    static Stream<Arguments> indexes() {
        return Stream.of(SIMPLE_META_DATA, RESTAURANT_META_DATA)
                .flatMap(metaData -> metaData.getAllIndexes().stream()
                        .map(index -> Arguments.of(metaData, index.getName())));
    }

    @Nonnull
    static Stream<Arguments> recordTypes() {
        return Stream.of(SIMPLE_META_DATA, RESTAURANT_META_DATA)
                .flatMap(metaData -> metaData.getRecordTypes().keySet().stream()
                        .map(recordTypeName -> Arguments.of(metaData, recordTypeName)));
    }

    @Nonnull
    private static List<Quantifier> quantifiersOf(@Nonnull final MatchCandidate matchCandidate) {
        return matchCandidate.getTraversal()
                .getRefs()
                .stream()
                .flatMap(reference -> reference.getAllMemberExpressions().stream())
                .flatMap(expression -> expression.getQuantifiers().stream())
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    private static RecordMetaData simpleMetaData() {
        final var metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", new Index("fan_out_value",
                field("repeater", FanType.FanOut)));
        metaDataBuilder.addIndex("MySimpleRecord", new Index("concat_value",
                concat(field("num_value_2"), field("str_value_indexed"))));
        metaDataBuilder.addIndex("MySimpleRecord", new Index("sum_by_fan_out",
                field("num_value_2").groupBy(field("repeater", FanType.FanOut)), IndexTypes.SUM));
        metaDataBuilder.addIndex("MySimpleRecord", new Index("count",
                new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        metaDataBuilder.addIndex("MySimpleRecord", new Index("max_ever_by_str",
                field("num_value_3_indexed").groupBy(field("str_value_indexed")), IndexTypes.MAX_EVER_TUPLE));
        metaDataBuilder.addIndex("MySimpleRecord", new Index("rank",
                field("num_value_2").ungrouped(), IndexTypes.RANK));
        metaDataBuilder.addIndex("MySimpleRecord", new Index("permuted_max_by_str",
                field("num_value_3_indexed").groupBy(field("str_value_indexed")), IndexTypes.PERMUTED_MAX,
                Map.of(IndexOptions.PERMUTED_SIZE_OPTION, "1")));
        return metaDataBuilder.build();
    }

    @Nonnull
    private static RecordMetaData restaurantMetaData() {
        final var metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords4Proto.getDescriptor());
        metaDataBuilder.addIndex("RestaurantRecord", new Index("review_rating",
                field("reviews", FanType.FanOut).nest("rating")));
        metaDataBuilder.addIndex("RestaurantRecord", new Index("name_review",
                concat(field("name"), field("reviews", FanType.FanOut).nest(concat(field("reviewer"), field("rating"))))));
        metaDataBuilder.addIndex("RestaurantRecord", new Index("sum_rating_by_name",
                field("reviews", FanType.FanOut).nest("rating").groupBy(field("name")), IndexTypes.SUM));
        return metaDataBuilder.build();
    }
}
