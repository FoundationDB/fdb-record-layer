/*
 * WindowIndexExpansionVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.WindowExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RankWindowValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.DefaultValueSimplificationRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public class WindowIndexExpansionVisitor extends KeyExpressionExpansionVisitor implements ExpansionVisitor<KeyExpressionExpansionVisitor.VisitorState> {

    /**
     * The window value each supported window index type materializes, built straight from the call site. This
     * deliberately bypasses {@link BuiltInFunction} encapsulation: encapsulating a window function produces a
     * {@link com.apple.foundationdb.record.query.plan.cascades.values.CompileTimeWindowValue}, which is the
     * placeholder
     * plan generation creates for a SQL call site and which the planner rewrite then eliminates. A match candidate
     * needs
     * the value the index actually materializes, so it is constructed directly.
     */
    @Nonnull
    static final Supplier<Map<String, Function<CallSiteArguments, WindowValue>>> windowFunctionsMap =
            Suppliers.memoize(WindowIndexExpansionVisitor::computeWindowFunctionsMap);

    @Nonnull
    private final Index index;
    @Nonnull
    protected final GroupingKeyExpression windowKeyExpression;
    @Nonnull
    private final List<RecordType> recordTypes;

    public WindowIndexExpansionVisitor(@Nonnull Index index, @Nonnull Collection<RecordType> recordTypes) {
        Preconditions.checkArgument(supportsWindowIndexType(index.getType()));
        this.index = index;
        var rootExpression = index.getRootExpression();
        Verify.verify(rootExpression instanceof GroupingKeyExpression);
        this.windowKeyExpression = ((GroupingKeyExpression)index.getRootExpression());
        this.recordTypes = ImmutableList.copyOf(recordTypes);
    }

    @Nonnull
    private GraphExpansion expandIndexKey(@Nonnull final Quantifier.ForEach baseQuantifier) {
        // set the split point to expose the partitioning columns only as placeholders.
        int splitPoint = windowKeyExpression.getGroupingCount();
        final var state = VisitorState.of(Lists.newArrayList(), Lists.newArrayList(),
                baseQuantifier, ImmutableList.of(), splitPoint,
                0, false, false);
        return pop(windowKeyExpression.getWholeKey().expand(push(state)));
    }

    @Nonnull
    private GraphExpansion expandAllIndexKeys(@Nonnull final Quantifier.ForEach baseQuantifier) {
        int splitPoint = -1;
        final var state = VisitorState.of(Lists.newArrayList(), Lists.newArrayList(),
                baseQuantifier, ImmutableList.of(), splitPoint,
                0, true, false);
        return pop(windowKeyExpression.getWholeKey().expand(push(state)));
    }

    @Override
    @Nonnull
    public MatchCandidate expand(@Nonnull final Set<String> availableRecordTypeNames,
                                 @Nonnull final Set<String> queriedRecordTypeNames,
                                 @Nonnull final Type.Record baseType,
                                 @Nonnull final AccessHint accessHint,
                                 @Nullable final KeyExpression primaryKey,
                                 final boolean isReverse) {
        Debugger.updateIndex(PredicateWithValueAndRanges.class, old -> 0);

        // 1. expand the FUSE box.
        final Quantifier.ForEach fuseQuantifier = Quantifier.forEach(ExpansionVisitor.createFuseReference(availableRecordTypeNames,
                queriedRecordTypeNames, baseType, null, accessHint)); // todo consider setting the record type key alias if possible.
        final IndexKeysAndValues keysAndValues = computeIndexKeysAndValues(fuseQuantifier);
        final GraphExpansion indexKeyExpansion = expandIndexKey(fuseQuantifier);
        final int indexKeyColumnCount = keysAndValues.keys().size();
        expandRemainingPrimaryKeyColumns(primaryKey, keysAndValues, fuseQuantifier);
        final List<Value> primaryKeyColumns =
                ImmutableList.copyOf(keysAndValues.keys().subList(indexKeyColumnCount, keysAndValues.keys().size()));

        // 2. expand pre-window select
        final GraphExpansion allKeysExpansion = expandAllIndexKeys(fuseQuantifier);
        final GraphExpansion preWindowSelectExpansion =
                expandPreWindowSelectExpression(fuseQuantifier, indexKeyExpansion, allKeysExpansion);
        final List<Placeholder> partitioningPlaceholders = preWindowSelectExpansion.getPlaceholders();
        final Quantifier.ForEach preWindowSelectQun =
                Quantifier.forEach(Reference.initialOf(preWindowSelectExpansion.buildSelect()));

        // The entry key is expanded over the base quantifier and, where the key unnests, over quantifiers of its own.
        // Its columns are taken from the whole-key expansion rather than from the key values that expansion registers:
        // an unnesting key registers a value written over the exploded quantifier it hides inside a select of its own,
        // which nothing above can reach, whereas the column it yields is the one the pre-window select flows. The
        // primary key columns it appends are plain base record columns and are taken as they are.
        final List<Value> indexKeyValues = ImmutableList.<Value>builder()
                .addAll(columnValues(allKeysExpansion.getResultColumns()))
                .addAll(primaryKeyColumns)
                .build();

        // Everything downstream of the pre-window select reasons in terms of what it flows, so the key columns are put
        // in that frame once, here, rather than each consumer pulling them up in its own.
        final List<Value> pulledUpIndexKeyValues = pullUpIfPossible(indexKeyValues, preWindowSelectQun);

        // 3. expand window
        final WindowExpression windowExpression =
                expandWindowExpression(preWindowSelectQun, allKeysExpansion, pulledUpIndexKeyValues,
                        fuseQuantifier.getFlowedObjectType().narrowRecordMaybe()
                                .orElseThrow(() -> new RecordCoreException("cannot create match candidate with non-record type")));

        // 4. expand post-window select
        final GraphExpansion postWindowSelectExpansion = expandPostWindowExpression(windowExpression, partitioningPlaceholders);
        final Reference postWindowSelectRef = Reference.initialOf(postWindowSelectExpansion.buildSelect());

        // Everything the candidate advertises as sargable is listed by the post-window select: the partitioning column(s),
        // then the window value itself, then the primary key columns that are not already among the partitioning ones.
        // The partitioning ones are advertised under the same parameters the pre-window select uses, so a predicate on
        // one of them binds the same parameter whichever level it matches at. The order follows the legacy windowed
        // candidate (see LegacyWindowedIndexScanMatchCandidate.orderingAliases) so that a bound prefix lines up with
        // the way a BY_RANK scan expects its comparisons: grouping equalities first, then the rank.
        final var placeHolderAliases = postWindowSelectExpansion.getPlaceholderAliases();

        final var maybeWithSort = placeHolderAliases.isEmpty()
                                  ? postWindowSelectRef // single group, sort by constant
                                  : Reference.initialOf(new MatchableSortExpression(placeHolderAliases,
                isReverse, Quantifier.forEach(postWindowSelectRef)));

        final var fuseQunAlias = fuseQuantifier.getAlias();
        final var traversal = Traversal.withRoot(maybeWithSort);
        return new WindowIndexScanMatchCandidate(index,
                recordTypes,
                traversal,
                fuseQuantifier.getFlowedObjectType().narrowRecordMaybe().orElseThrow(() -> new RecordCoreException("cannot create match candidate with non-record type")),
                fuseQunAlias,
                placeHolderAliases,
                indexKeyValues,
                ValueIndexExpansionVisitor.fullKey(index, primaryKey),
                primaryKey,
                postWindowSelectRef.get().getResultValue(),
                baseRcv(postWindowSelectRef.get().getResultValue(),
                        Iterables.getOnlyElement(postWindowSelectExpansion.getQuantifiers()).getAlias(),
                        windowExpression, fuseQunAlias));
    }

    @Nonnull
    private GraphExpansion expandPreWindowSelectExpression(@Nonnull final Quantifier.ForEach baseQuantifier,
                                                           @Nonnull final GraphExpansion keyExpansion,
                                                           @Nonnull final GraphExpansion allKeysExpansion) {
        final var selectExpansionVisitor = ImmutableList.<GraphExpansion>builder();
        selectExpansionVisitor.add(GraphExpansion.ofResultValue(baseQuantifier.getFlowedObjectValue()))
                .add(GraphExpansion.ofQuantifier(baseQuantifier));

        // An unnesting key expression brings quantifiers of its own. The columns it defines over them are what the
        // window partitions and orders by, so they are flowed next to the base record -- nothing above could reach
        // them through the base record's own object value. Both come from the whole-key expansion, which is also what
        // the window expression's values are built from, so the two agree on which quantifier a column refers to.
        final var unnestedAliases = Quantifiers.aliases(allKeysExpansion.getQuantifiers());
        for (final var column : allKeysExpansion.getResultColumns()) {
            if (!Sets.intersection(column.getValue().getCorrelatedTo(), unnestedAliases).isEmpty()) {
                selectExpansionVisitor.add(GraphExpansion.ofResultColumn(column));
            }
        }

        // a copy, because folding an index predicate's ranges in below replaces placeholders in place
        final var partitioningPlaceholders = Lists.newArrayList(keyExpansion.getPlaceholders());

        if (index.hasPredicate()) {
            final var filteredIndexPredicate = Objects.requireNonNull(index.getPredicate()).toPredicate(baseQuantifier.getFlowedObjectValue());
            final var valueRangesMaybe = IndexPredicateExpansion.dnfPredicateToRanges(filteredIndexPredicate);
            final var predicateExpansionBuilder = GraphExpansion.builder();
            if (valueRangesMaybe.isEmpty()) { // could not create DNF, store the predicate as-is.
                selectExpansionVisitor.add(GraphExpansion.ofPredicate(filteredIndexPredicate));
            } else {
                // A range on a partitioning column is folded into that column's placeholder, so a query predicate on it
                // still binds the same parameter and simply has to fall inside the index's own range. A range on any
                // other column has no placeholder to fold into and becomes a plain constraint.
                final var valueRanges = valueRangesMaybe.get();
                for (final var value : valueRanges.keySet()) {
                    final var ranges = ImmutableSet.copyOf(valueRanges.get(value));
                    final int placeholderIndex = indexOfPlaceholderOn(partitioningPlaceholders, value);
                    if (placeholderIndex < 0) {
                        predicateExpansionBuilder.addPredicate(
                                PredicateWithValueAndRanges.ofRanges(value, ranges));
                    } else {
                        partitioningPlaceholders.set(placeholderIndex,
                                partitioningPlaceholders.get(placeholderIndex).withExtraRanges(ranges));
                    }
                }
            }
            selectExpansionVisitor.add(predicateExpansionBuilder.build());
        }

        final GraphExpansion.Builder builder = GraphExpansion.ofOthers(selectExpansionVisitor.build()).toBuilder()
                .addAllPlaceholders(partitioningPlaceholders)
                .addAllPredicates(keyExpansion.getPredicates())
                .addAllQuantifiers(allKeysExpansion.getQuantifiers());

        return builder.build();
    }

    /**
     * The position of the placeholder sitting on the given value, or {@code -1} if none of them does.
     */
    private static int indexOfPlaceholderOn(@Nonnull final List<Placeholder> placeholders,
                                            @Nonnull final Value value) {
        for (int i = 0; i < placeholders.size(); i++) {
            if (placeholders.get(i).getValue().semanticEquals(value, AliasMap.emptyMap())) {
                return i;
            }
        }
        return -1;
    }

    @Nonnull
    private WindowExpression expandWindowExpression(@Nonnull final Quantifier.ForEach preWindowSelectQun,
                                                    @Nonnull final GraphExpansion allKeysExpansion,
                                                    @Nonnull final List<Value> indexKeyValues,
                                                    @Nonnull final Type.Record baseRecordType) {
        // The index key defines one split point: the leading columns are what the window is partitioned by, the
        // trailing ones are what the window function is computed over. Partitioning is not grouping -- a partition does
        // not collapse its rows, it only bounds where the function restarts -- so both sides keep flowing every row.
        // Deriving both from that single split lets the degenerate cases fall out on their own -- an empty tail means
        // every column partitions, an empty head means none do -- and guarantees the two slices tile the column list
        // rather than overlapping or leaving a gap.
        final List<Column<? extends Value>> baseColumns = allKeysExpansion.getResultColumns();
        final int partitioningCount = windowKeyExpression.getGroupingCount();
        final List<Value> partitioningColumns = columnValues(baseColumns.subList(0, partitioningCount));
        final List<Value> arguments = columnValues(baseColumns.subList(partitioningCount, baseColumns.size()));

        final Set<CorrelationIdentifier> constantCorrelations = Streams.concat(
                arguments.stream().flatMap(v -> v.getCorrelatedTo().stream()),
                partitioningColumns.stream().flatMap(v -> v.getCorrelatedTo().stream()),
                preWindowSelectQun.getCorrelatedTo().stream()).collect(ImmutableSet.toImmutableSet());
        final AliasMap aliasMap = AliasMap.identitiesFor(constantCorrelations);
        final List<Value> pulledUpArguments = pullUp(arguments, preWindowSelectQun, aliasMap);
        final List<Value> pulledUpPartitioningColumns = pullUp(partitioningColumns, preWindowSelectQun, aliasMap);

        // The index's grouped columns are the window's SORT CRITERIA, not arguments to the function. The key
        // expression "[TEAM, SCORE] group 1" reads as "partition by TEAM, order by SCORE" -- SCORE is what the rank is
        // taken over, which standard SQL spells as ORDER BY inside OVER. So they go in the window specification's
        // ordering and the argument list stays empty, which is the shape a query spelling rank() OVER (... ORDER BY
        // score) produces and therefore what it can be compared against.
        final ImmutableList<WindowOrderingPart> orderingParts = pulledUpArguments.stream()
                .map(value -> new WindowOrderingPart(value, OrderingPart.RequestedSortOrder.ASCENDING))
                .collect(ImmutableList.toImmutableList());

        final CallSiteArguments callsiteArguments = CallSiteArguments.ofPositional(ImmutableList.of())
                .withWindowSpecification(new CallSiteArguments.WindowSpecification(pulledUpPartitioningColumns,
                        orderingParts)); // window indexes do not support custom window frame specification yet.

        final WindowValue windowValue = constructWindowValue(callsiteArguments);

        //
        // Every column the pre-window select flows is passed through, the partitioning ones included: partitioning by a
        // column does not flow it, and the candidate's partitioning columns are sargable and so have to be reachable
        // above the window expression. The base record is passed through one field at a time, and each field keeps its
        // own shape -- a nested one stays nested. The post-window select's type is the result type of the plan built
        // from this candidate, and two things resolve against it: a primary key expression written in the record's
        // field names, which PrimaryKeyProperty translates against it, and a query selecting a nested field as a whole,
        // which cannot be rebuilt out of leaf columns because a nullable record must not be expanded into a record
        // constructor. A value index candidate exposes the base record's own shape for the same reasons; it goes one
        // step further and flows the base quantifier's object value itself, which a window expression cannot do because
        // it has to interpose the window value.
        //
        final var preWindowColumns = Values.deconstructRecord(QuantifiedObjectValue.of(preWindowSelectQun));
        final var baseRecordColumn = preWindowColumns.get(0);
        Verify.verify(baseRecordColumn.getResultType().equals(baseRecordType),
                "the pre-window select flows the base record as its first column");
        final List<Value> allValues = ImmutableList.<Value>builder()
                .addAll(Values.deconstructRecord(baseRecordColumn))
                .addAll(preWindowColumns.subList(1, preWindowColumns.size()))
                .build();
        final List<Value> passThroughValues = inEntryKeyOrder(allValues, indexKeyValues);

        return new WindowExpression(windowValue, pulledUpPartitioningColumns, passThroughValues, orderingParts,
                preWindowSelectQun);
    }

    /**
     * Expresses the given values over the pre-window select's quantifier, dropping any that cannot be reached from
     * what it flows. A record type key has no counterpart there, and neither has a column of a quantifier the select
     * does not range over.
     */
    @Nonnull
    private static List<Value> pullUpIfPossible(@Nonnull final List<Value> values,
                                                @Nonnull final Quantifier preWindowSelectQun) {
        final var pulledUpMap = preWindowSelectQun.getRangesOver().get().getResultValue()
                .pullUp(values, EvaluationContext.empty(), AliasMap.emptyMap(), ImmutableSet.of(),
                        preWindowSelectQun.getAlias());
        return values.stream()
                .map(pulledUpMap::get)
                .filter(pulledUp -> !pulledUp.isEmpty())
                .map(pulledUp -> pulledUp.iterator().next())
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    private List<Value> pullUp(@Nonnull final List<Value> values,
                               @Nonnull final Quantifier preWindowSelectQun,
                               @Nonnull final AliasMap aliasMap) {
        final var pulledUpMap = preWindowSelectQun.getRangesOver().get().getResultValue()
                .pullUp(values, EvaluationContext.empty(), aliasMap, ImmutableSet.of(),
                        preWindowSelectQun.getAlias());
        if (!pulledUpMap.keys().containsAll(values)) {
            throw new RecordCoreException("could not pull up window expression " + windowKeyExpression)
                    .addLogInfo(LogMessageKeys.VALUE, windowKeyExpression);
        }
        return values.stream()
                .map(value -> Iterables.getOnlyElement(Objects.requireNonNull(pulledUpMap.get(value))))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Orders the given columns the way the index entry orders them, so that what this candidate flows and what a scan
     * of it reads back have one shape rather than two.
     *
     * <p>The entry key is the index key followed by whatever the primary key adds to it; the columns handed in here are
     * every column of the base record that is not a partitioning one, in the order the record declares them. For
     * {@code partition by b order by c} over a table keyed on {@code (a, c)} the entry key is {@code (b, c,
     * recordTypeKey, a)}, so {@code c} and {@code a} are ordered by it and {@code d}, which the entry does not carry,
     * follows them.
     *
     * <p>A key column that no column here corresponds to leaves no trace: the record type key is part of every entry key
     * but is nothing the record declares, so it simply orders nothing. That is also why the result is not the entry
     * key with holes punched in it -- it is these columns, ordered.
     *
     * @param columns the columns to order, over {@code preWindowSelectQun}
     * @param entryKeyOrder the entry key's columns in key order, in the same frame as {@code columns}
     *
     * @return the same columns, in entry key order, the ones the entry does not carry last
     */
    @Nonnull
    private static List<Value> inEntryKeyOrder(@Nonnull final List<Value> columns,
                                               @Nonnull final List<Value> entryKeyOrder) {
        return ImmutableList.sortedCopyOf(
                Comparator.comparingInt((Value column) -> firstEntryKeyOrdinalIn(column, entryKeyOrder)), columns);
    }

    /**
     * The lowest entry key ordinal the given column carries, or {@link Integer#MAX_VALUE} if it carries none. A column
     * is a whole field of the base record, so it may carry several key columns at once -- a nested field the index key
     * reaches into carries one per key column inside it -- and the column is ordered by the first of them.
     */
    private static int firstEntryKeyOrdinalIn(@Nonnull final Value column,
                                              @Nonnull final List<Value> entryKeyOrder) {
        for (int ordinal = 0; ordinal < entryKeyOrder.size(); ordinal++) {
            if (relativeFieldNamesOf(entryKeyOrder.get(ordinal), column, AliasMap.emptyMap()).isPresent()) {
                return ordinal;
            }
        }
        return Integer.MAX_VALUE;
    }

    /**
     * Where the given entry key column sits inside the given column: the field names leading from one to the other, as
     * an empty list when they are the same column, or {@code Optional.empty()} when the key column is not inside it at
     * all. Only a field path can be descended into, so anything else matches only by being equal.
     *
     * @param keyValue a column of the entry key
     * @param column a column something flows, in the same frame as {@code keyValue}
     * @param aliasMap the aliases to hold equal
     *
     * @return the path from {@code column} down to {@code keyValue}, if it is inside it
     */
    @Nonnull
    static Optional<List<String>> relativeFieldNamesOf(@Nonnull final Value keyValue,
                                                       @Nonnull final Value column,
                                                       @Nonnull final AliasMap aliasMap) {
        if (keyValue.semanticEquals(column, aliasMap)) {
            return Optional.of(ImmutableList.of());
        }
        if (!(keyValue instanceof FieldValue keyFieldValue)) {
            return Optional.empty();
        }
        final var keyPath = keyFieldValue.getFieldPath();
        final FieldValue.FieldPath columnPath;
        if (column instanceof FieldValue columnFieldValue) {
            if (!keyFieldValue.getChild().semanticEquals(columnFieldValue.getChild(), aliasMap)) {
                return Optional.empty();
            }
            columnPath = columnFieldValue.getFieldPath();
        } else if (keyFieldValue.getChild().semanticEquals(column, aliasMap)) {
            columnPath = FieldValue.FieldPath.empty();
        } else {
            return Optional.empty();
        }
        final var keyOrdinals = keyPath.getFieldOrdinals();
        final var columnOrdinals = columnPath.getFieldOrdinals();
        if (columnOrdinals.length() >= keyOrdinals.length()) {
            return Optional.empty();
        }
        for (int i = 0; i < columnOrdinals.length(); i++) {
            if (columnOrdinals.get(i) != keyOrdinals.get(i)) {
                return Optional.empty();
            }
        }
        final var namesBuilder = ImmutableList.<String>builder();
        final var keyNames = keyPath.getOptionalFieldNames();
        for (int i = columnOrdinals.length(); i < keyNames.size(); i++) {
            if (keyNames.get(i).isEmpty()) {
                return Optional.empty();
            }
            namesBuilder.add(keyNames.get(i).get());
        }
        return Optional.of(namesBuilder.build());
    }


    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static Value baseRcv(@Nonnull final Value postWindowResultValue,
                                 @Nonnull final CorrelationIdentifier windowAlias,
                                 @Nonnull final WindowExpression windowExpression,
                                 @Nonnull final CorrelationIdentifier baseAlias) {
        final var fields = ((Type.Record)postWindowResultValue.getResultType()).getFields();
        final var postWindowColumns = Values.deconstructRecord(postWindowResultValue);
        Verify.verify(postWindowColumns.size() == fields.size(),
                "the post-window select flows a different number of columns than its type has fields");

        final var preWindowSelectQun = windowExpression.getInnerQuantifier();
        final var ruleSet = DefaultValueSimplificationRuleSet.instance();
        final var overPreWindowSelect = windowExpression.getResultValue()
                .pushDown(postWindowColumns.subList(0, postWindowColumns.size() - 1), ruleSet,
                        EvaluationContext.empty(), AliasMap.emptyMap(), ImmutableSet.of(), windowAlias);
        final var overBase = preWindowSelectQun.getRangesOver().get().getResultValue()
                .pushDown(overPreWindowSelect, ruleSet, EvaluationContext.empty(), AliasMap.emptyMap(),
                        ImmutableSet.of(), preWindowSelectQun.getAlias());

        final var rebase = AliasMap.ofAliases(baseAlias, IndexEntryToRecordValueHelper.baseRecordAlias());
        final var columns = ImmutableList.<Column<? extends Value>>builder();
        for (int ordinalNumber = 0; ordinalNumber < overBase.size(); ordinalNumber++) {
            final var field = fields.get(ordinalNumber);
            columns.add(Column.of(field,
                    PromoteValue.inject(overBase.get(ordinalNumber).rebase(rebase), field.getFieldType())));
        }
        final var windowField = fields.get(fields.size() - 1);
        columns.add(Column.of(windowField, new NullValue(windowField.getFieldType())));
        return RecordConstructorValue.ofColumns(columns.build());
    }

    @Nonnull
    private GraphExpansion expandPostWindowExpression(@Nonnull final WindowExpression windowExpression,
                                                      @Nonnull final List<Placeholder> partitioningPlaceholders) {
        final Quantifier.ForEach windowQun = Quantifier.forEach(Reference.initialOf(windowExpression));
        final var postWindowExpansionBuilder = GraphExpansion.builder().addQuantifier(windowQun);

        //
        // The window expression flows its partitioning columns, then its additional results, then the window value.
        // Each one is passed through under the name the window expression gave it, so that the type this select
        // produces -- which becomes the result type of the plan built from this candidate -- can still be resolved
        // against by name. PrimaryKeyProperty does exactly that, translating the index's primary key expression,
        // written in the user's column names, against it.
        //
        final var windowFields = ((Type.Record)windowExpression.getResultValue().getResultType()).getFields();
        for (int ordinalNumber = 0; ordinalNumber < windowFields.size(); ordinalNumber++) {
            postWindowExpansionBuilder.addResultColumn(
                    Column.of(windowFields.get(ordinalNumber).getFieldNameOptional(),
                            FieldValue.ofOrdinalNumber(windowQun.getFlowedObjectValue(), ordinalNumber)));
        }
        final var flowedColumnCount = windowFields.size();

        final List<Value> partitioningValues = windowExpression.getPartitioningValues();
        final AliasMap aliasMap = AliasMap.identitiesFor(windowExpression.getCorrelatedTo());

        // Only two things are sargable on a windowed index: the partitioning columns, which select whole partitions,
        // and the window value itself, which a BY_RANK scan turns into a range. The primary key components the entry
        // carries beyond them are NOT advertised -- a BY_RANK scan's range is over (partitioning..., rank), so there
        // is no way to constrain anything after it. They still flow as result columns, exactly as a value index flows
        // every column it covers, so a query may select them; they simply cannot be bound.
        // The partitioning columns were expanded against the base quantifier, so they have to travel up both levels
        // before a placeholder can be put on them here. The window value needs no such thing: it is flowed as the
        // window expression's last column and does not exist any lower down.
        final List<Value> partitioningReferences = pullUp(partitioningValues, windowQun, aliasMap);
        final var windowValueReference = FieldValue.ofOrdinalNumber(windowQun.getFlowedObjectValue(), flowedColumnCount - 1);

        //
        // The partitioning columns keep the parameter they already have one level down; everything else is minted here.
        //
        for (int i = 0; i < partitioningReferences.size(); i++) {
            final Placeholder placeholder = partitioningReferences.get(i)
                    .asPlaceholder(partitioningPlaceholders.get(i).getParameterAlias());
            postWindowExpansionBuilder.addPlaceholder(placeholder).addPredicate(placeholder);
        }
        final Placeholder windowValuePlaceholder = windowValueReference.asPlaceholder(newParameterAlias());
        postWindowExpansionBuilder.addPlaceholder(windowValuePlaceholder).addPredicate(windowValuePlaceholder);
        return postWindowExpansionBuilder.build();
    }

    @Nonnull
    private WindowValue constructWindowValue(@Nonnull final CallSiteArguments callSiteArguments) {
        final var windowValueFactory = windowFunctionsMap.get().get(index.getType());
        if (windowValueFactory == null) {
            throw new RecordCoreException("unknown window function " + index.getType());
        }
        return windowValueFactory.apply(callSiteArguments);
    }

    @Nonnull
    private static List<Value> columnValues(@Nonnull final List<Column<? extends Value>> columns) {
        return columns.stream().map(Column::getValue).collect(ImmutableList.toImmutableList());
    }

    // Rank(

    @Nonnull
    private IndexKeysAndValues computeIndexKeysAndValues(@Nonnull final Quantifier.ForEach baseQuantifier) {
        var rootExpression = windowKeyExpression.getWholeKey();
        final int keyValueSplitPoint;
        if (rootExpression instanceof final KeyWithValueExpression keyWithValueExpression) {
            keyValueSplitPoint = keyWithValueExpression.getSplitPoint();
            rootExpression = keyWithValueExpression.getInnerKey();
        } else {
            keyValueSplitPoint = -1;
        }

        final var keyValues = Lists.<Value>newArrayList();
        final var valueValues = Lists.<Value>newArrayList();

        final var initialState =
                VisitorState.of(keyValues,
                        valueValues,
                        baseQuantifier,
                        ImmutableList.of(),
                        keyValueSplitPoint,
                        0,
                        false,
                        true);

        pop(rootExpression.expand(push(initialState)))
                .toBuilder()
                .removeAllResultColumns()
                .build();
        return new IndexKeysAndValues(keyValues, valueValues);
    }

    public record IndexKeysAndValues(@Nonnull List<Value> keys, @Nonnull List<Value> values) {
    }

    private void expandRemainingPrimaryKeyColumns(@Nullable final KeyExpression primaryKey,
                                                  @Nonnull final IndexKeysAndValues keysAndValues,
                                                  @Nonnull final Quantifier.ForEach baseQuantifier) {
        if (primaryKey == null) {
            return;
        }
        final var keySize = keysAndValues.keys().size();
        // unfortunately we must copy as the returned list is not guaranteed to be mutable which is needed for the
        // trimPrimaryKey() function as it is causing a side-effect
        final var trimmedPrimaryKeys = Lists.newArrayList(primaryKey.normalizeKeyForPositions());
        index.trimPrimaryKey(trimmedPrimaryKeys);
        for (int i = 0; i < trimmedPrimaryKeys.size(); i++) {
            final var initialStateForKeyPart =
                    VisitorState.of(keysAndValues.keys(),
                            Lists.newArrayList(),
                            baseQuantifier,
                            ImmutableList.of(),
                            -1,
                            keySize + i,
                            false,
                            true);
            pop(trimmedPrimaryKeys.get(i).expand(push(initialStateForKeyPart)));
        }
    }

    @Nonnull
    private static Map<String, Function<CallSiteArguments, WindowValue>> computeWindowFunctionsMap() {
        final ImmutableMap.Builder<String, Function<CallSiteArguments, WindowValue>> mapBuilder = ImmutableMap.builder();
        mapBuilder.put(IndexTypes.RANK,
                callSiteArguments -> new RankWindowValue(callSiteArguments.getArgumentsList(),
                        Objects.requireNonNull(callSiteArguments.getWindowSpecification()).frameSpecification()));
        return mapBuilder.build();
    }

    private static boolean supportsWindowIndexType(@Nonnull String indexType) {
        return windowFunctionsMap.get().containsKey(indexType);
    }
}
