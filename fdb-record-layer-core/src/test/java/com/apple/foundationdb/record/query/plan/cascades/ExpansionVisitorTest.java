/*
 * ExpansionVisitorTest.java
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
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsRankProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.expressions.WindowExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RankWindowValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ExpansionVisitor} default methods, the
 * {@link LegacyWindowedIndexExpansionVisitor} unsupported expand overload, and the candidate graph
 * {@link WindowIndexExpansionVisitor} builds for a {@code RANK} index.
 */
class ExpansionVisitorTest {

    @Test
    void testDefaultExpandWithBaseQuantifierSupplierThrowsUnsupportedOperation() {
        final Index index = new Index("test_value_index", field("value"), IndexTypes.VALUE);
        final ValueIndexExpansionVisitor visitor = new ValueIndexExpansionVisitor(index, Collections.emptyList());

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> visitor.expand(() -> null, null, false),
                "Default expand(Supplier, ...) should throw UnsupportedOperationException");
    }

    @Test
    void testLegacyWindowedIndexExpandWithRecordTypeNamesThrowsUnsupportedOperation() {
        final Index index = new Index("test_rank_index", field("score").ungrouped(), IndexTypes.RANK);
        final LegacyWindowedIndexExpansionVisitor visitor = new LegacyWindowedIndexExpansionVisitor(index, Collections.emptyList());

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> visitor.expand(ImmutableSet.of("TestRecord"),
                        ImmutableSet.of("TestRecord"),
                        Type.Record.fromFields(ImmutableList.of()),
                        new IndexAccessHint("test_rank_index"),
                        null,
                        false),
                "LegacyWindowedIndexExpansionVisitor.expand(Set, Set, ...) should throw UnsupportedOperationException");
    }

    @Nonnull
    private static Optional<MatchCandidate> expandRankIndex(@Nonnull final Index index) {
        return expandRankIndex("BasicRankedRecord", index);
    }

    @Nonnull
    private static Optional<MatchCandidate> expandRankIndex(@Nonnull final String recordTypeName,
                                                           @Nonnull final Index index) {
        Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
        Debugger.setup();
        final RecordMetaDataBuilder metaDataBuilder =
                RecordMetaData.newBuilder().setRecords(TestRecordsRankProto.getDescriptor());
        metaDataBuilder.addIndex(recordTypeName, index);
        metaDataBuilder.getRecordType("HeaderRankedRecord")
                .setPrimaryKey(field("header").nest(Key.Expressions.concatenateFields("group", "id")));
        final var metaData = metaDataBuilder.build();
        final var expansionInfo = IndexExpansionInfo.createInfo(metaData, index, false);
        final var visitor = new WindowIndexExpansionVisitor(expansionInfo.getIndex(),
                expansionInfo.getIndexedRecordTypes());

        return MatchCandidateExpansion.expandIndexMatchCandidate(expansionInfo,
                expansionInfo.getCommonPrimaryKeyForTypes(), visitor);
    }

    /**
     * Finds the single {@link WindowExpression} in the candidate graph the expansion produced.
     */
    @Nonnull
    private static WindowExpression windowExpressionOf(@Nonnull final MatchCandidate matchCandidate) {
        final var windowExpressions = matchCandidate.getTraversal().getRefs()
                .stream()
                .flatMap(reference -> reference.getAllMemberExpressions().stream())
                .filter(WindowExpression.class::isInstance)
                .map(WindowExpression.class::cast)
                .collect(ImmutableList.toImmutableList());
        assertThat(windowExpressions).hasSize(1);
        return windowExpressions.get(0);
    }

    /**
     * The select the window expression ranges over, that is the bottom select of the candidate graph.
     */
    @Nonnull
    private static SelectExpression bottomSelectOf(@Nonnull final WindowExpression windowExpression) {
        return windowExpression.getInnerQuantifier().getRangesOver().getAllMemberExpressions()
                .stream()
                .filter(SelectExpression.class::isInstance)
                .map(SelectExpression.class::cast)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no select underneath the window expression"));
    }

    /**
     * The select sitting on top of the window expression.
     */
    @Nonnull
    private static SelectExpression topSelectOf(@Nonnull final MatchCandidate matchCandidate,
                                               @Nonnull final WindowExpression windowExpression) {
        final var traversal = matchCandidate.getTraversal();
        return traversal.getRefsContaining(windowExpression)
                .stream()
                .flatMap(reference -> traversal.getParentExpressions(reference).stream())
                .filter(SelectExpression.class::isInstance)
                .map(SelectExpression.class::cast)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no select on top of the window expression"));
    }

    /**
     * The columns a window expression flows: every column of the base record, then the window value.
     */
    @Nonnull
    private static List<? extends Value> flowedColumnsOf(@Nonnull final WindowExpression windowExpression) {
        return Values.deconstructRecord(windowExpression.getResultValue());
    }

    /**
     * How many of the given values read the named field of the base record.
     */
    private static long countReadsOfField(@Nonnull final List<? extends Value> values,
                                         @Nonnull final String fieldName) {
        return values.stream()
                .filter(value -> readsField(ImmutableList.of(value), fieldName))
                .count();
    }

    /**
     * Whether any of the given values reads the named field of the base record.
     */
    private static boolean readsField(@Nonnull final List<? extends Value> values, @Nonnull final String fieldName) {
        return values.stream()
                .flatMap(Value::preOrderStream)
                .filter(FieldValue.class::isInstance)
                .map(FieldValue.class::cast)
                .anyMatch(fieldValue -> fieldValue.getFieldPathNames().contains(fieldName));
    }

    @Nonnull
    private static ImmutableSet<CorrelationIdentifier> correlationsOf(@Nonnull final List<? extends Value> values) {
        return values.stream()
                .flatMap(value -> value.getCorrelatedTo().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    @Test
    void windowIndexExpansionBuildsACandidateGraphWithAWindowExpression() {
        final var index = new Index("rank_by_gender", field("score").groupBy(field("gender")), IndexTypes.RANK);

        final var matchCandidate = expandRankIndex(index);

        assertThat(matchCandidate).isPresent();
        assertThat(matchCandidate.get().getName()).isEqualTo("rank_by_gender");
        assertThat(matchCandidate.get().getQueriedRecordTypes())
                .extracting(recordType -> recordType.getName())
                .containsExactly("BasicRankedRecord");

        final var windowExpression = windowExpressionOf(matchCandidate.get());
        assertThat(windowExpression.getWindowValue()).isInstanceOf(RankWindowValue.class);
        // the index ranks "score" within each "gender". rank() takes no argument: the ranked column is the window's
        // ORDER BY, and the expression asks its input for the partitioning column followed by it
        assertThat(windowExpression.getWindowValue().getArgumentValues()).isEmpty();
        assertThat(windowExpression.getPartitioningValues()).hasSize(1);
        assertThat(windowExpression.getRequestedOrdering().getOrderingValues()).hasSize(2);
    }

    /**
     * What the candidate advertises is the partitioning columns and the window value, and nothing else -- here
     * {@code gender} and the rank. The window value is among them on purpose: a query constrains the rank, which is what
     * makes a {@code BY_RANK} scan expressible, and the post-window select is the first level at which it exists.
     *
     * <p>The two columns that are not advertised are not advertised for different reasons. {@code score} is what the rank is
     * computed over: binding it would filter rows before they are ranked, which is not what the index materializes.
     * {@code name} is the primary key, physically present in the entry but behind the rank in the entry key, so a
     * constraint on it could not narrow a scan that is not already down to a single rank.
     */
    @Test
    void windowIndexExpansionMakesTheIndexKeyColumnsSargable() {
        final var index = new Index("rank_by_gender", field("score").groupBy(field("gender")), IndexTypes.RANK);

        final var matchCandidate = expandRankIndex(index).orElseThrow();

        assertThat(matchCandidate.getSargableAliases()).hasSize(2);
        assertThat(matchCandidate.getOrderingAliases()).isEqualTo(matchCandidate.getSargableAliases());
    }

    /**
     * Everything a window expression is built from has to be expressed over the quantifier it ranges over. The columns
     * are expanded against the base quantifier and then pulled up through the select-where quantifier, and it is the
     * pulled-up values that the expression has to carry, or it would reference an alias that is not in scope for it.
     */
    @Test
    void windowExpressionValuesAreCorrelatedToItsOwnInput() {
        final var index = new Index("rank_by_gender", field("score").groupBy(field("gender")), IndexTypes.RANK);

        final var windowExpression = windowExpressionOf(expandRankIndex(index).orElseThrow());
        final var innerAlias = windowExpression.getInnerQuantifier().getAlias();

        // rank() takes no argument, so the value references nothing of its own
        assertThat(windowExpression.getWindowValue().getCorrelatedTo()).isEmpty();
        assertThat(correlationsOf(windowExpression.getPartitioningValues())).containsExactly(innerAlias);
        assertThat(correlationsOf(windowExpression.getAdditionalResultValues())).containsExactly(innerAlias);
        // nothing the expression carries may point outside of it
        assertThat(windowExpression.getCorrelatedToWithoutChildren()).containsExactly(innerAlias);
    }

    /**
     * Every {@code GROUP BY} column of the index partitions the window, not just the first. The grouping and grouped
     * counts are complements of each other, so an index with a single grouping column cannot tell the two apart -- which
     * is why this uses two.
     */
    @Test
    void windowIndexExpansionPartitionsByEveryGroupingColumn() {
        final var index = new Index("rank_by_gender_and_name",
                field("score").groupBy(field("gender"), field("name")), IndexTypes.RANK);

        final var windowExpression = windowExpressionOf(expandRankIndex(index).orElseThrow());

        assertThat(windowExpression.getPartitioningValues()).hasSize(2);
        // the remaining column of the index key is what the rank is computed over, which is the window's ORDER BY:
        // the requested ordering is both partitioning columns followed by it
        assertThat(windowExpression.getWindowValue().getArgumentValues()).isEmpty();
        assertThat(windowExpression.getRequestedOrdering().getOrderingValues()).hasSize(3);
    }

    @Test
    void windowIndexExpansionMakesEveryIndexKeyColumnSargable() {
        final var index = new Index("rank_by_gender_and_name",
                field("score").groupBy(field("gender"), field("name")), IndexTypes.RANK);

        final var matchCandidate = expandRankIndex(index).orElseThrow();

        // both partitioning columns plus the rank. "name" is a primary key column, but it is also a partitioning column
        // here, and a column that is already advertised is not advertised a second time
        assertThat(matchCandidate.getSargableAliases()).hasSize(3);
    }

    /**
     * A base record usually has columns the index key does not mention. Those still have to reach the top of the
     * candidate graph -- carried by the bottom select, flowed by the window expression, and flowed again by the
     * select on top -- because a query matching this candidate may well select them. What they must not become is
     * placeholders: only the rank and the partitioning columns are sargable.
     *
     * <p>The bottom select does not name those columns one by one. It flows the record whole, as a single column, and it is
     * the window expression that takes it apart into one column per field: the partitioning columns first, then the rest
     * in the order the index entry carries them, then the window value.
     */
    @Test
    void columnsOutsideTheIndexKeyFlowThroughWithoutBecomingPlaceholders() {
        final var index = new Index("rank_by_gender", field("score").groupBy(field("gender")), IndexTypes.RANK);

        final var matchCandidate = expandRankIndex(index).orElseThrow();
        final var windowExpression = windowExpressionOf(matchCandidate);

        // 1. the bottom select carries it, as part of the one record-shaped column it flows
        assertThat(bottomSelectOf(windowExpression).getResultValues())
                .as("the bottom select should flow the record whole rather than column by column")
                .hasSize(1);

        // 2. the window expression has to flow it, exactly once. A candidate flows every base column, the partitioning
        // ones included: those are sargable, so the select on top has to be able to place a placeholder on them
        assertThat(countReadsOfField(flowedColumnsOf(windowExpression), "description"))
                .as("the window expression should flow the description column exactly once")
                .isEqualTo(1L);
        // name, score, gender, description, and the window value after them
        assertThat(flowedColumnsOf(windowExpression))
                .as("every base column should be flowed, each once, and the window value after them")
                .hasSize(5);

        // 3. the select on top has to flow everything the window expression flows, the window value included
        assertThat(topSelectOf(matchCandidate, windowExpression).getResultValues())
                .as("the top select should flow every column the window expression flows")
                .hasSize(flowedColumnsOf(windowExpression).size());

        // ... but none of the extra columns may be sargable: only the partitioning column "gender" and the rank are --
        // neither "description" nor the primary key "name" is one of them
        assertThat(matchCandidate.getSargableAliases()).hasSize(2);
    }

    /**
     * A partitioning column is sargable at both levels of the candidate graph, under one and the same parameter. The
     * pre-window select has to advertise it so that a {@code WHERE} clause on it binds below the window, and it has to do
     * so under the parameter the post-window select uses, or the two levels would disagree about which parameter the
     * column is sargable under and a query could bind it twice. This is how a grouping column is handled in
     * {@link AggregateIndexExpansionVisitor}.
     *
     * <p>Only the partitioning columns are advertised down there. The column the rank is computed over is not: binding it
     * below the window would filter rows before they are ranked, which is not what the index materializes.
     */
    @Test
    void partitioningColumnKeepsItsParameterAcrossBothSelects() {
        final var index = new Index("rank_by_gender", field("score").groupBy(field("gender")), IndexTypes.RANK);

        final var matchCandidate = expandRankIndex(index).orElseThrow();
        final var windowExpression = windowExpressionOf(matchCandidate);

        final var preWindowPlaceholders = placeholdersOf(bottomSelectOf(windowExpression));
        assertThat(preWindowPlaceholders)
                .as("the pre-window select should advertise the partitioning column and nothing else")
                .hasSize(1);
        assertThat(readsField(ImmutableList.of(preWindowPlaceholders.get(0).getValue()), "gender"))
                .as("the pre-window placeholder should sit on the partitioning column")
                .isTrue();

        final var topPlaceholderAliases = placeholdersOf(topSelectOf(matchCandidate, windowExpression))
                .stream()
                .map(Placeholder::getParameterAlias)
                .collect(ImmutableList.toImmutableList());
        assertThat(topPlaceholderAliases)
                .as("every parameter the candidate advertises should be placed by the post-window select")
                .containsExactlyElementsOf(matchCandidate.getSargableAliases());

        //
        // The partitioning columns lead the post-window select's parameters, so the first of them is gender's -- the very
        // parameter the pre-window select advertises it under.
        //
        assertThat(topPlaceholderAliases.get(0))
                .as("the partitioning column should be sargable under the same parameter at both levels")
                .isEqualTo(preWindowPlaceholders.get(0).getParameterAlias());
    }

    /**
     * The placeholders of the given select, in the order it carries them.
     */
    @Nonnull
    private static List<Placeholder> placeholdersOf(@Nonnull final SelectExpression select) {
        return select.getPredicates()
                .stream()
                .filter(Placeholder.class::isInstance)
                .map(Placeholder.class::cast)
                .collect(ImmutableList.toImmutableList());
    }

    @Test
    void noColumnIsFlowedTwiceAtAnyLevelOfTheCandidateGraph() {
        final var index = new Index("rank_by_gender", field("score").groupBy(field("gender")), IndexTypes.RANK);

        final var matchCandidate = expandRankIndex(index).orElseThrow();
        final var windowExpression = windowExpressionOf(matchCandidate);

        assertNoDuplicates(bottomSelectOf(windowExpression).getResultValues(), "bottom select");
        assertNoDuplicates(windowExpression.getAdditionalResultValues(), "window expression");
        assertNoDuplicates(topSelectOf(matchCandidate, windowExpression).getResultValues(), "top select");
    }

    /**
     * The trailing columns of an index entry are the parts of the primary key the index key does not already cover. The
     * entry carries them, so the candidate has to flow them -- a query matching it may select them, and it does so
     * without a fetch. What it must not do is advertise them: they sit behind the rank in the entry key, so a
     * constraint on one of them could not narrow a scan anyway, and advertising it would let a query bind a parameter
     * the scan cannot honour.
     */
    @Test
    void thePrimaryKeyIsFlowedButNotSargable() {
        final var index = new Index("rank_by_gender", field("score").groupBy(field("gender")), IndexTypes.RANK);

        final var matchCandidate = expandRankIndex(index).orElseThrow();
        final var windowExpression = windowExpressionOf(matchCandidate);

        // "name" is BasicRankedRecord's primary key and is not part of this index's key
        assertThat(countReadsOfField(flowedColumnsOf(windowExpression), "name"))
                .as("the primary key column should be flowed exactly once")
                .isEqualTo(1L);

        //
        // The placeholders sit on references to the window expression's output rather than on the base columns, and
        // there are exactly two of them: the partitioning column and the window value.
        //
        final var placeholders = placeholdersOf(topSelectOf(matchCandidate, windowExpression));
        assertThat(placeholders.stream().map(Placeholder::getParameterAlias).collect(ImmutableList.toImmutableList()))
                .as("only gender and the rank should be advertised")
                .hasSize(2)
                .containsExactlyElementsOf(matchCandidate.getSargableAliases());
        assertThat(readsField(placeholders.stream().map(Placeholder::getValue).collect(ImmutableList.toImmutableList()),
                "name"))
                .as("no placeholder should sit on the primary key column")
                .isFalse();
    }

    /**
     * A sparse index carries a predicate of its own. Where that predicate is on a partitioning column, its ranges are
     * folded into the placeholder the pre-window select already advertises that column under, rather than becoming a
     * plain constraint alongside it: a query predicate on the column then binds the same parameter and simply has to fall
     * inside the index's own range. What must still hold is that the column is sargable under exactly one parameter -- a
     * second expansion of the index key would mint a second alias for it, with only one of them known to the candidate.
     */
    @Test
    void predicateOnAPartitioningColumnDoesNotAddASecondParameter() {
        final var index = predicateRankIndex();

        final var matchCandidate = expandRankIndex(index).orElseThrow();
        final var windowExpression = windowExpressionOf(matchCandidate);

        final var preWindowPlaceholders = placeholdersOf(bottomSelectOf(windowExpression));
        assertThat(preWindowPlaceholders)
                .as("the index predicate should merge into the partitioning column's placeholder, not add one")
                .hasSize(1);
        assertThat(preWindowPlaceholders.get(0).getRanges())
                .as("the index predicate's range should be carried by that placeholder")
                .isNotEmpty();

        // gender and the rank -- one parameter each, no duplicate for the predicated column
        assertThat(matchCandidate.getSargableAliases())
                .as("a predicated partitioning column should still be sargable under exactly one parameter")
                .hasSize(2)
                .doesNotHaveDuplicates();
        assertThat(matchCandidate.getSargableAliases().get(0))
                .as("and under the same parameter the pre-window select uses")
                .isEqualTo(preWindowPlaceholders.get(0).getParameterAlias());
    }

    /**
     * A rank index over {@code score} grouped by {@code gender}, restricted to a single gender. The predicate has to go
     * on through the proto, which is the only way an {@link Index} carries one.
     */
    @Nonnull
    private static Index predicateRankIndex() {
        final var recordType = Type.Record.fromDescriptor(TestRecordsRankProto.BasicRankedRecord.getDescriptor());
        final var genderPredicate = new ValuePredicate(
                FieldValue.ofFieldName(QuantifiedObjectValue.of(Quantifier.current(), recordType), "gender"),
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "F"));
        final var normalized = BooleanPredicateNormalizer.getDefaultInstanceForDnf()
                .normalizeAndSimplify(genderPredicate, true)
                .orElse(genderPredicate);
        return new Index(RecordMetaDataProto.Index.newBuilder()
                .setName("rank_by_gender_sparse")
                .addRecordType("BasicRankedRecord")
                .setType(IndexTypes.RANK)
                .setRootExpression(field("score").groupBy(field("gender")).toKeyExpression())
                .setPredicate(IndexPredicate.fromQueryPredicate(normalized).toProto())
                .build());
    }

    private static void assertNoDuplicates(@Nonnull final List<? extends Value> values, @Nonnull final String where) {
        for (int i = 0; i < values.size(); i++) {
            for (int j = i + 1; j < values.size(); j++) {
                assertThat(values.get(i).semanticEquals(values.get(j), AliasMap.emptyMap()))
                        .as("%s flows the same column at %d and %d", where, i, j)
                        .isFalse();
            }
        }
    }

    /**
     * When the index key already contains the primary key column, that column must still be flowed exactly once: it is
     * both a partitioning column and a primary key component, and each of those is a reason to flow it.
     */
    @Test
    void primaryKeyColumnInsideTheIndexKeyIsFlowedExactlyOnce() {
        // "name" is the primary key of BasicRankedRecord and is also the grouping column here
        final var index = new Index("rank_by_name", field("score").groupBy(field("name")), IndexTypes.RANK);

        final var matchCandidate = expandRankIndex(index).orElseThrow();
        final var windowExpression = windowExpressionOf(matchCandidate);
        final var flowed = flowedColumnsOf(windowExpression);

        assertThat(countReadsOfField(flowed, "name"))
                .as("the primary key column should be flowed once, not once per expansion that mentions it")
                .isEqualTo(1L);
        // one column per record field -- name, score, gender, description -- and the window value after them
        assertThat(flowed).hasSize(5);
        assertNoDuplicates(flowed, "window expression");
    }

    /**
     * The same when the index key contains only part of a compound primary key: the part it contains must not be flowed
     * twice, whether directly or by way of the record it is nested in.
     */
    @Test
    void primaryKeyColumnPartlyInsideTheIndexKeyIsFlowedExactlyOnce() {
        // HeaderRankedRecord's primary key is (header.group, header.id); the index key covers header.group only
        final var index = new Index("rank_by_header_group",
                field("score").groupBy(field("header").nest("group")), IndexTypes.RANK);

        final var matchCandidate = expandRankIndex("HeaderRankedRecord", index).orElseThrow();
        final var windowExpression = windowExpressionOf(matchCandidate);
        final var flowed = flowedColumnsOf(windowExpression);

        assertThat(countReadsOfField(flowed, "group"))
                .as("header.group is in the index key, so it should be flowed once")
                .isEqualTo(1L);
        // the nested record is taken apart into its leaves -- header.group, header.id, score -- then the window value
        assertThat(flowed).hasSize(4);
        assertNoDuplicates(flowed, "window expression");
    }
}
