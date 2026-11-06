/*
 * PrimaryKeyPlaceholderExperimentTest.java
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
import com.apple.foundationdb.record.TestRecordsAbcdExperimentProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Scratch experiment, deliberately standalone, asking where a table's columns turn into placeholders and what ends up
 * in the sort argument, for a value index versus an aggregate index.
 *
 * <p>The table is {@code (A, B, C, D, E)} throughout. Only {@code A}, {@code B} and {@code D} are primary key
 * components, in that order, so {@code C} and {@code E} are not part of the primary key at all.
 * <ul>
 *     <li>{@link #dumpValueIndexOnCAndB()} -- a {@code VALUE} index keyed on {@code (C, B)}. {@code B} is the
 *     <em>middle</em> primary key component sitting in the <em>trailing</em> key position, so it is trimmed out of
 *     the appended primary key while {@code A} and {@code D} still have to be appended.</li>
 *     <li>{@link #dumpAggregateIndexMaxAGroupByDcb()} -- a {@code MAX_EVER_LONG} index for
 *     {@code MAX(A) GROUP BY D, C, B}. An aggregate index takes no primary key at all.</li>
 * </ul>
 */
class PrimaryKeyPlaceholderExperimentTest {

    @Nonnull
    private static final String RECORD_TYPE = "AbcdRecord";

    /**
     * {@code (A, B, D)} -- note {@code C} and {@code E} are not part of it.
     */
    @Nonnull
    private static KeyExpression primaryKey() {
        return Key.Expressions.concatenateFields("a", "b", "d");
    }

    /**
     * The expanded candidate together with the index it came from, since the candidate does not expose the index.
     */
    private record Expansion(@Nonnull MatchCandidate matchCandidate, @Nonnull Index index) {
    }

    /**
     * Builds metadata carrying the given index over {@code (A, B, C, D, E)} with primary key {@code (A, B, D)}, then
     * expands it into a match candidate with the given visitor.
     *
     * @param index the index to expand
     * @param visitorFactory builds the expansion visitor for the index
     * @param commonPrimaryKey the primary key to hand the visitor -- {@code null} for an aggregate index, which
     *        appends no primary key and verifies that it was given none
     */
    @Nonnull
    private static Expansion expand(@Nonnull final Index index,
                                    @Nonnull final VisitorFactory visitorFactory,
                                    @Nullable final KeyExpression commonPrimaryKey) {
        Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
        Debugger.setup();

        final var metaDataBuilder =
                RecordMetaData.newBuilder().setRecords(TestRecordsAbcdExperimentProto.getDescriptor());
        // primary key first, so the index's primaryKeyComponentPositions are computed against (A, B, D)
        metaDataBuilder.getRecordType(RECORD_TYPE).setPrimaryKey(primaryKey());
        metaDataBuilder.addIndex(RECORD_TYPE, index);
        final var metaData = metaDataBuilder.build();

        final var expansionInfo = IndexExpansionInfo.createInfo(metaData, index, false);
        final var visitor = visitorFactory.build(expansionInfo);

        final var matchCandidate = MatchCandidateExpansion.expandIndexMatchCandidate(expansionInfo,
                commonPrimaryKey, visitor).orElseThrow();
        return new Expansion(matchCandidate, expansionInfo.getIndex());
    }

    @FunctionalInterface
    private interface VisitorFactory {
        @Nonnull
        ExpansionVisitor<?> build(@Nonnull IndexExpansionInfo expansionInfo);
    }

    /**
     * Every relational expression reachable from the candidate's root, top level first. Walking down from the root
     * keeps the order deterministic, unlike {@link Traversal#getRefs()}, which is a set.
     */
    @Nonnull
    private static List<RelationalExpression> levelsTopDown(@Nonnull final MatchCandidate matchCandidate) {
        final var levels = new ArrayList<RelationalExpression>();
        collectLevels(matchCandidate.getTraversal().getRootReference(), levels);
        return levels;
    }

    private static void collectLevels(@Nonnull final Reference reference,
                                      @Nonnull final List<RelationalExpression> levels) {
        for (final var expression : reference.getAllMemberExpressions()) {
            levels.add(expression);
            for (final var quantifier : expression.getQuantifiers()) {
                collectLevels(quantifier.getRangesOver(), levels);
            }
        }
    }

    @Nonnull
    private static List<Placeholder> placeholdersOf(@Nonnull final Iterable<? extends QueryPredicate> predicates) {
        final var builder = ImmutableList.<Placeholder>builder();
        for (final var predicate : predicates) {
            if (predicate instanceof Placeholder) {
                builder.add((Placeholder)predicate);
            }
        }
        return builder.build();
    }

    /**
     * Dumps the index's entry layout, then every level of the candidate graph with the placeholders it carries, then
     * the sort argument and what the candidate advertises.
     */
    @Nonnull
    private static String report(@Nonnull final String label,
                                 @Nonnull final Expansion expansion,
                                 @Nullable final KeyExpression primaryKeyOrNull) {
        final var matchCandidate = expansion.matchCandidate();
        final var index = expansion.index();
        final var report = new StringBuilder();

        report.append("########## ").append(label).append(" ##########\n");
        report.append("=== index ===\n");
        report.append("  type                        : ").append(index.getType()).append('\n');
        report.append("  root key expression         : ").append(index.getRootExpression()).append('\n');
        if (index.getRootExpression() instanceof GroupingKeyExpression) {
            final var grouping = (GroupingKeyExpression)index.getRootExpression();
            report.append("  groupingCount / groupedCount: ").append(grouping.getGroupingCount())
                    .append(" / ").append(grouping.getGroupedCount()).append('\n');
        }
        report.append("  primaryKeyComponentPositions: ")
                .append(Arrays.toString(index.getPrimaryKeyComponentPositions())).append('\n');
        if (primaryKeyOrNull != null) {
            final var trimmed = Lists.newArrayList(primaryKeyOrNull.normalizeKeyForPositions());
            index.trimPrimaryKey(trimmed);
            report.append("  primary key after trimming  : ").append(trimmed).append('\n');
        } else {
            report.append("  primary key                 : (none handed to the visitor)\n");
        }

        report.append("=== candidate graph, top level first ===\n");
        for (final var expression : levelsTopDown(matchCandidate)) {
            report.append("  ").append(expression.getClass().getSimpleName()).append('\n');
            if (expression instanceof MatchableSortExpression) {
                report.append("      sortParameterIds: ")
                        .append(((MatchableSortExpression)expression).getSortParameterIds()).append('\n');
                report.append("      isReverse       : ")
                        .append(((MatchableSortExpression)expression).isReverse()).append('\n');
            }
            if (expression instanceof SelectExpression) {
                final var select = (SelectExpression)expression;
                final var placeholders = placeholdersOf(select.getPredicates());
                if (placeholders.isEmpty()) {
                    report.append("      placeholders    : (none)\n");
                } else {
                    for (final var placeholder : placeholders) {
                        report.append("      placeholder     : ").append(placeholder.getParameterAlias())
                                .append(" -> ").append(placeholder.getValue()).append('\n');
                    }
                }
                final var resultValues = select.getResultValues();
                for (int i = 0; i < resultValues.size(); i++) {
                    report.append("      result[").append(i).append("]       : ")
                            .append(resultValues.get(i)).append('\n');
                }
            }
        }

        report.append("=== candidate ===\n");
        report.append("  getSargableAliases(): ").append(matchCandidate.getSargableAliases()).append('\n');
        report.append("  getOrderingAliases(): ").append(matchCandidate.getOrderingAliases()).append('\n');
        return report.toString();
    }

    private static void write(@Nonnull final String fileName, @Nonnull final String report) throws Exception {
        System.out.println(report);
        // the build's test tasks log only failures and do not capture stdout, so the dump also goes to a file
        Files.writeString(Path.of(System.getProperty("experiment.dir", "/tmp"), fileName), report);
    }

    @Test
    void dumpValueIndexOnCAndB() throws Exception {
        final var index = new Index("value_on_c_b", Key.Expressions.concatenateFields("c", "b"), IndexTypes.VALUE);
        final var expansion = expand(index,
                info -> new ValueIndexExpansionVisitor(info.getIndex(), info.getIndexedRecordTypes()),
                primaryKey());
        write("experiment-value-index.txt",
                report("VALUE index on (C, B), primary key (A, B, D)", expansion, primaryKey()));
    }

    @Test
    void dumpAggregateIndexMaxAGroupByDcb() throws Exception {
        // MAX(A) GROUP BY D, C, B -- the grouping columns lead the key, the aggregated column is the grouped one
        final var index = new Index("max_a_by_d_c_b",
                Key.Expressions.field("a").groupBy(Key.Expressions.concatenateFields("d", "c", "b")),
                IndexTypes.MAX_EVER_LONG);
        // an aggregate index appends no primary key; AggregateIndexExpansionVisitor verifies it was handed none
        final var expansion = expand(index,
                info -> new AggregateIndexExpansionVisitor(info.getIndex(), info.getIndexedRecordTypes()),
                null);
        write("experiment-aggregate-index.txt",
                report("MAX_EVER_LONG index: MAX(A) GROUP BY D, C, B", expansion, null));
    }

    @Test
    void dumpWindowIndexRankAPartitionByDC() throws Exception {
        //
        // RANK(A) OVER (PARTITION BY D, C). The primary key is (A, B, D), and the index key covers A and D, so B is
        // the only primary key column left to append. The candidate should therefore advertise, in order: the two
        // partitioning columns, the window value, and B.
        //
        final var index = new Index("rank_a_by_d_c",
                Key.Expressions.field("a").groupBy(Key.Expressions.concatenateFields("d", "c")),
                IndexTypes.RANK);
        final var expansion = expand(index,
                info -> new WindowIndexExpansionVisitor(info.getIndex(), info.getIndexedRecordTypes()),
                primaryKey());
        write("experiment-window-index.txt",
                report("RANK index: RANK(A) OVER (PARTITION BY D, C), primary key (A, B, D)", expansion,
                        primaryKey()));
    }
}
