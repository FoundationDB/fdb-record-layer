/*
 * TextScan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.text.TextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.cursors.IntersectionCursor;
import com.apple.foundationdb.record.provider.foundationdb.cursors.IntersectionMultiCursor;
import com.apple.foundationdb.record.provider.foundationdb.cursors.ProbableIntersectionCursor;
import com.apple.foundationdb.record.provider.foundationdb.cursors.UnionCursor;
import com.apple.foundationdb.record.provider.foundationdb.cursors.UnorderedUnionCursor;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexMaintainer;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Encapsulate the information necessary to scan a text-based index. In particular, this
 * includes the work to translate the comparison type into the proper ranges of the index
 * to scan and then combine.
 *
 * <p>
 * <b>NOTE:</b> This API is still a work in progress and might change in a future without notice.
 * This class should not be used outside of the Record Layer project.
 * </p>
 */
@API(API.Status.INTERNAL)
public class TextScan implements PlanHashable {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Text-Scan");

    // Used by the text predicates that filter
    @Nonnull
    private static final Set<StoreTimer.Count> inCounts = ImmutableSet.of(FDBStoreTimer.Counts.QUERY_FILTER_GIVEN, FDBStoreTimer.Counts.QUERY_TEXT_FILTER_PLAN_GIVEN);
    @Nonnull
    private static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_TEXT_FILTER);
    @Nonnull
    private static final Set<StoreTimer.Count> successCounts = ImmutableSet.of(FDBStoreTimer.Counts.QUERY_FILTER_PASSED, FDBStoreTimer.Counts.QUERY_TEXT_FILTER_PLAN_PASSED );
    @Nonnull
    private static final Set<StoreTimer.Count> failureCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_DISCARDED);

    @Nonnull
    private final Index index;
    @Nullable
    private final ScanComparisons groupingComparisons;
    @Nonnull
    private final Comparisons.TextComparison textComparison;
    @Nullable
    private final ScanComparisons suffixComparisons;

    public TextScan(@Nonnull Index index,
                    @Nullable ScanComparisons groupingComparisons,
                    @Nonnull Comparisons.TextComparison textComparison,
                    @Nullable ScanComparisons suffixComparisons) {
        this.index = index;
        this.groupingComparisons = groupingComparisons;
        this.textComparison = textComparison;
        this.suffixComparisons = suffixComparisons;
    }

    // Get the comparand as a list of strings. This might involve tokenizing the
    // query string if the comparison didn't do that already.
    private List<String> getTokenList(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context, boolean removeStopWords) {
        final Object comparand = textComparison.getComparand(store, context);
        List<String> tokenList;
        if (comparand instanceof List<?>) {
            tokenList = ((List<?>)comparand).stream().map(Object::toString).collect(Collectors.toList());
        } else if (comparand instanceof String) {
            TextTokenizer tokenizer = TextIndexMaintainer.getTokenizer(index);
            int tokenizerVersion = TextIndexMaintainer.getIndexTokenizerVersion(index);
            tokenList = tokenizer.tokenizeToList((String)comparand, tokenizerVersion, TextTokenizer.TokenizerMode.QUERY);
        } else {
            throw new RecordCoreException("Comparand for text query of incompatible type: " + (comparand == null ? "null" : comparand.getClass()));
        }
        if (removeStopWords && tokenList.contains("")) {
            // Remove all stop words from this list
            tokenList = tokenList.stream().filter(s -> !s.isEmpty()).collect(Collectors.toList());
        }
        return tokenList;
    }

    private List<String> getTokenList(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context) {
        return getTokenList(store, context, true);
    }

    // As we get index entries back, we will compare their values and consider two entries
    // equal if and only if they match all entries after a prefix. In particular, that prefix
    // should consist of any grouping key columns (which are already equal by the way the
    // scan is done) as well as the token itself, which will definitely *not* be equal for
    // different index scans. The rest of the columns in that key determine the order
    // in which results are returned and so are necessary for determining equality. Within
    // those columns should be the primary key (in almost all cases), so this is sufficient
    // for making sure that the primary key at least must match.
    @Nonnull
    private static Function<IndexEntry, List<Object>> suffixComparisonKeyFunction(int firstEntries) {
        return indexEntry -> {
            Tuple key = indexEntry.getKey();
            return TupleHelpers.subTuple(key, firstEntries, key.size()).getItems();
        };
    }

    /**
     * Scan the store to produce a cursor of index entries that all satisfy the comparison.
     *
     * @param store the record store for the query
     * @param context the query evaluation context
     * @param continuation a continuation from a previous scan to resume query execution
     * @param scanProperties execution properties of this scan
     * @param <M> message type associated with the store and evaluation context
     * @return a cursor of index entries from the given scan
     */
    @Nonnull
    public <M extends Message> RecordCursor<IndexEntry> scan(@Nonnull FDBRecordStoreBase<M> store,
                                                             @Nonnull EvaluationContext context,
                                                             @Nullable byte[] continuation,
                                                             @Nonnull ScanProperties scanProperties) {
        final Tuple prefix = groupingComparisons != null ? groupingComparisons.toTupleRange(store, context).getHigh() : null;
        final TupleRange suffix = suffixComparisons != null ? suffixComparisons.toTupleRange(store, context) : null;
        final List<String> tokenList = getTokenList(store, context);
        return scan(store, context, prefix, suffix, index, tokenList, continuation, scanProperties);
    }

    @Nonnull
    @SuppressWarnings({"squid:S2095", "PMD.CloseResource"}) // try-with-resources - the two cursors returned cannot be closed because they are wrapped and returned
    private <M extends Message> RecordCursor<IndexEntry> scan(@Nonnull FDBRecordStoreBase<M> store,
                                                              @Nonnull EvaluationContext context,
                                                              @Nullable Tuple prefix, @Nullable TupleRange suffix,
                                                              @Nonnull Index index, @Nonnull List<String> tokenList,
                                                              @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        if (tokenList.isEmpty()) {
            return RecordCursor.empty();
        }
        final int prefixEntries = 1 + (prefix != null ? prefix.size() : 0);

        final Comparisons.Type comparisonType = textComparison.getType();
        if (comparisonType.equals(Comparisons.Type.TEXT_CONTAINS_PREFIX) || (tokenList.size() == 1 && (
                comparisonType.equals(Comparisons.Type.TEXT_CONTAINS_ALL_PREFIXES) || comparisonType.equals(Comparisons.Type.TEXT_CONTAINS_ANY_PREFIX)))) {
            if (tokenList.size() != 1) {
                throw new RecordCoreException("text prefix comparison included " + tokenList.size() + " comparands instead of one");
            }
            return scanTokenPrefix(store, tokenList.get(0), prefix, suffix, index, scanProperties).apply(continuation);
        } else if (tokenList.size() == 1) {
            // Other than prefix scanning, all of the other cases become this same range scan
            // over a single token when there is only one element. Note that intersection and union
            // plans throw an error when there are fewer than two children, so this special case
            // is necessary, not just nice to have.
            return scanToken(store, tokenList.get(0), prefix, suffix, index, scanProperties).apply(continuation);
        } else if (comparisonType.equals(Comparisons.Type.TEXT_CONTAINS_ALL)) {
            // Take the intersection of all children. Note that to handle skip and the returned row limit correctly,
            // the skip and limit are both removed and then applied later.
            final ScanProperties childScanProperties = scanProperties.with(ExecuteProperties::clearSkipAndLimit);
            List<Function<byte[], RecordCursor<IndexEntry>>> intersectionChildren = tokenList.stream()
                    .map(token -> scanToken(store, token, prefix, suffix, index, childScanProperties))
                    .collect(Collectors.toList());
            return IntersectionCursor.create(suffixComparisonKeyFunction(prefixEntries), scanProperties.isReverse(), intersectionChildren, continuation, store.getTimer())
                    .skip(scanProperties.getExecuteProperties().getSkip())
                    .limitRowsTo(scanProperties.getExecuteProperties().getReturnedRowLimit());
        } else if (comparisonType.equals(Comparisons.Type.TEXT_CONTAINS_ALL_PREFIXES)) {
            final Comparisons.TextContainsAllPrefixesComparison allPrefixesComparison = (Comparisons.TextContainsAllPrefixesComparison)textComparison;
            final ScanProperties childScanProperties = scanProperties.with(ExecuteProperties::clearSkipAndLimit);
            List<Function<byte[], RecordCursor<IndexEntry>>> intersectionChildren = tokenList.stream()
                    .map(token -> scanTokenPrefix(store, token, prefix, suffix, index, childScanProperties))
                    .collect(Collectors.toList());
            return ProbableIntersectionCursor.create(suffixComparisonKeyFunction(prefixEntries), intersectionChildren,
                        allPrefixesComparison.getExpectedRecords(),
                        allPrefixesComparison.getFalsePositivePercentage(),
                        continuation, store.getTimer()
                    )
                    .skip(scanProperties.getExecuteProperties().getSkip())
                    .limitRowsTo(scanProperties.getExecuteProperties().getReturnedRowLimit());
        } else if (comparisonType.equals(Comparisons.Type.TEXT_CONTAINS_ANY)) {
            // Take the union of all children. Note that to handle skip and the returned row limit correctly,
            // the skip is removed from the children and applied to the returned cursor. Also, the limit
            // is adjusted upwards and then must be applied again to returned union.
            final ScanProperties childScanProperties = scanProperties.with(ExecuteProperties::clearSkipAndAdjustLimit);
            List<Function<byte[], RecordCursor<IndexEntry>>> unionChildren = tokenList.stream()
                    .map(token -> scanToken(store, token, prefix, suffix, index, childScanProperties))
                    .collect(Collectors.toList());
            return UnionCursor.create(suffixComparisonKeyFunction(prefixEntries), scanProperties.isReverse(), unionChildren, continuation, store.getTimer())
                    .skip(scanProperties.getExecuteProperties().getSkip())
                    .limitRowsTo(scanProperties.getExecuteProperties().getReturnedRowLimit());
        } else if (comparisonType.equals(Comparisons.Type.TEXT_CONTAINS_ANY_PREFIX)) {
            final ScanProperties childScanProperties = scanProperties.with(ExecuteProperties::clearSkipAndAdjustLimit);
            List<Function<byte[], RecordCursor<IndexEntry>>> unionChildren = tokenList.stream()
                    .map(token -> scanTokenPrefix(store, token, prefix, suffix, index, childScanProperties))
                    .collect(Collectors.toList());
            return UnorderedUnionCursor.create(unionChildren, continuation, store.getTimer())
                    .skip(scanProperties.getExecuteProperties().getSkip())
                    .limitRowsTo(scanProperties.getExecuteProperties().getReturnedRowLimit());
        } else {
            // Apply the filter based on the position lists
            final Function<List<IndexEntry>, Boolean> predicate;
            if (comparisonType.equals(Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN) && textComparison instanceof Comparisons.TextWithMaxDistanceComparison) {
                int maxDistance = ((Comparisons.TextWithMaxDistanceComparison)textComparison).getMaxDistance();
                predicate = entries -> entriesContainAllWithin(entries, maxDistance);
            } else if (comparisonType.equals(Comparisons.Type.TEXT_CONTAINS_PHRASE)) {
                List<String> tokensWithStopWords = getTokenList(store, context, false);
                predicate = entries -> entriesContainPhrase(entries, tokensWithStopWords);
            } else {
                throw new RecordCoreException("unsupported comparison type for text query: " + comparisonType);
            }

            // It's either TEXT_CONTAINS_ALL_WITHIN_DISTANCE or TEXT_CONTAINS_PHRASE. In any case, we need to scan
            // all tokens, intersect, and then apply a filter on the returned list.
            final ScanProperties childScanProperties = scanProperties.with(ExecuteProperties::clearSkipAndLimit);
            List<Function<byte[], RecordCursor<IndexEntry>>> intersectionChildren = tokenList.stream().map(token -> scanToken(store, token, prefix, suffix, index, childScanProperties)).collect(Collectors.toList());
            final RecordCursor<List<IndexEntry>> intersectionCursor = IntersectionMultiCursor.create(suffixComparisonKeyFunction(prefixEntries), scanProperties.isReverse(), intersectionChildren, continuation, store.getTimer());
            return intersectionCursor
                    .filterInstrumented(predicate, store.getTimer(), inCounts, duringEvents, successCounts, failureCounts)
                    .map(indexEntries -> indexEntries.get(0))
                    .skip(scanProperties.getExecuteProperties().getSkip())
                    .limitRowsTo(scanProperties.getExecuteProperties().getReturnedRowLimit());
        }
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    private static List<List<Integer>> getPositionsLists(@Nonnull List<IndexEntry> entries) {
        final List<List<Integer>> positionLists = new ArrayList<>(entries.size());
        for (IndexEntry entry : entries) {
            positionLists.add((List<Integer>) entry.getValue().get(0));
        }
        return positionLists;
    }

    @Nullable
    private static Boolean entriesContainAllWithin(@Nonnull List<IndexEntry> entries, int maxDistance) {
        if (entries.isEmpty()) {
            return null;
        }
        List<List<Integer>> positionLists = getPositionsLists(entries);
        if (positionLists.stream().anyMatch(List::isEmpty)) {
            // Remove any empty lists. They indicate that the token is so prevalent
            // that the position list information is not retained.
            positionLists = positionLists.stream().filter(list -> !list.isEmpty()).collect(Collectors.toList());
            if (positionLists.isEmpty()) {
                // If they are all empty, then we assume that they were all close.
                return Boolean.TRUE;
            }
        }

        PriorityQueue<NonnullPair<Integer, Iterator<Integer>>> minQueue = new PriorityQueue<>(positionLists.size(), Comparator.comparingInt(Pair::getLeft));
        int max = Integer.MIN_VALUE;
        for (List<Integer> positionList : positionLists) {
            Iterator<Integer> positionIterator = positionList.iterator();
            int value = positionIterator.next();
            max = Math.max(max, value);
            minQueue.add(NonnullPair.of(value, positionIterator));
        }

        while (true) {
            // Pop the smallest position off of the queue and check to see
            // if it is within maxDistance of the current largest value.
            NonnullPair<Integer, Iterator<Integer>> minElem = minQueue.poll();
            int min = minElem.getLeft();
            if (max - min <= maxDistance) {
                // Current span is within maximum allowed. Return true.
                return Boolean.TRUE;
            }
            Iterator<Integer> minIterator = minElem.getRight();
            if (minIterator.hasNext()) {
                // Advance this iterator and place it back in the queue with the
                // new associated value.
                int nextValue = minIterator.next();
                max = Math.max(max, nextValue);
                minQueue.add(NonnullPair.of(nextValue, minIterator));
            } else {
                // Exhausted one of the position lists. We didn't find a span that
                // was less than or equal to the maximum allowed span.
                break;
            }
        }
        return Boolean.FALSE;
    }

    @Nonnull
    @SuppressWarnings("PMD.ForLoopCanBeForeach") // avoids an extra iterator allocation
    private static List<List<Integer>> getPositionListsAndDeltas(@Nonnull List<IndexEntry> entries, @Nonnull List<String> tokensWithStopWords, @Nonnull List<Integer> deltas) {
        List<List<Integer>> positionLists = getPositionsLists(entries);

        // Construct an expected offset list between positions for each token list
        if (tokensWithStopWords.contains("") || positionLists.stream().anyMatch(List::isEmpty)) {
            // For every stop word in the original phrase, we need to increase the delta by one.
            // For every word with no position list, we also need to increase the delta by one,
            // but we also need to remove it from the lists of position lists.
            List<List<Integer>> newPositionLists = new ArrayList<>(positionLists.size());
            Iterator<List<Integer>> positionListIterator = positionLists.iterator();
            int currentDelta = 1;
            for (String token : tokensWithStopWords) {
                if (token.isEmpty()) {
                    currentDelta += 1;
                } else {
                    List<Integer> nextPositionList = positionListIterator.next();
                    if (nextPositionList.isEmpty()) {
                        currentDelta += 1;
                    } else {
                        newPositionLists.add(nextPositionList);
                        deltas.add(currentDelta);
                        currentDelta = 1;
                    }
                }
            }
            positionLists = newPositionLists;
        } else {
            for (int i = 0; i < positionLists.size(); i++) {
                deltas.add(1);
            }
        }

        return positionLists;
    }

    @Nullable
    private static Boolean entriesContainPhrase(@Nonnull List<IndexEntry> entries, @Nonnull List<String> tokensWithStopWords) {
        if (entries.isEmpty()) {
            return null;
        }
        final List<Integer> deltas = new ArrayList<>(entries.size());
        final List<List<Integer>> positionLists = getPositionListsAndDeltas(entries, tokensWithStopWords, deltas);
        if (positionLists.isEmpty()) {
            // Nothing has position list information, so we assume they were all close enough
            return Boolean.TRUE;
        }

        // Determine if there is a moment where all of the position lists are arranged so that
        // there is an position from each such that the difference between their positions matches the
        // delta mask.
        List<Integer> currentValues = new ArrayList<>(entries.size());
        List<Iterator<Integer>> positionIterators = new ArrayList<>(entries.size());
        for (List<Integer> positionList : positionLists) {
            Iterator<Integer> positionIterator = positionList.iterator();
            currentValues.add(positionIterator.next());
            positionIterators.add(positionIterator);
        }
        while (true) {
            int expectedPosition = currentValues.get(0);
            boolean allMatched = true;
            for (int i = 1; i < currentValues.size(); i++) {
                expectedPosition += deltas.get(i);
                int currentValue = currentValues.get(i);
                Iterator<Integer> positionIterator = positionIterators.get(i);
                while (currentValue < expectedPosition && positionIterator.hasNext()) {
                    currentValue = positionIterator.next();
                }
                if (currentValue < expectedPosition) {
                    // The position iterator ran out, so this token's position list is
                    // exhausted and we are never going to find a solution.
                    return Boolean.FALSE;
                } else {
                    currentValues.set(i, currentValue);
                    if (currentValue > expectedPosition) {
                        // We aren't going to find a match with this first token.
                        // Don't bother looking for more with these values of the
                        // positions.
                        allMatched = false;
                        break;
                    }
                }
            }
            if (allMatched) {
                // We found a set of tokens where all matched expected positions
                return Boolean.TRUE;
            } else {
                // Didn't find one with this being the first token.
                // Move on to the next token.
                Iterator<Integer> firstPositionIterator = positionIterators.get(0);
                if (firstPositionIterator.hasNext()) {
                    currentValues.set(0, firstPositionIterator.next());
                } else {
                    break;
                }
            }
        }

        return Boolean.FALSE;
    }

    @Nonnull
    private <M extends Message> Function<byte[], RecordCursor<IndexEntry>> scanTokenPrefix(@Nonnull FDBRecordStoreBase<M> store, @Nonnull String token, @Nullable Tuple prefix, @Nullable TupleRange suffix,
                                                                                           @Nonnull Index index, @Nonnull ScanProperties scanProperties) {
        if (suffix != null) {
            // This is equivalent to having two inequality comparisons, and it is therefore disallowed.
            throw new RecordCoreException("text prefix comparison included inequality scan comparison");
        }
        return (byte[] continuation) -> {
            TupleRange scanRange = TupleRange.prefixedBy(token);
            if (prefix != null) {
                scanRange = scanRange.prepend(prefix);
            }
            return store.scanIndex(index, IndexScanType.BY_TEXT_TOKEN, scanRange, continuation, scanProperties);
        };
    }

    @Nonnull
    private <M extends Message> Function<byte[], RecordCursor<IndexEntry>> scanToken(@Nonnull FDBRecordStoreBase<M> store, @Nonnull String token, @Nullable Tuple prefix, @Nullable TupleRange suffix,
                                                                                     @Nonnull Index index, @Nonnull ScanProperties scanProperties) {
        return (byte[] continuation) -> {
            TupleRange scanRange;
            if (suffix != null) {
                scanRange = suffix.prepend(Tuple.from(token));
            } else {
                scanRange = TupleRange.allOf(Tuple.from(token));
            }
            if (prefix != null) {
                scanRange = scanRange.prepend(prefix);
            }
            return store.scanIndex(index, IndexScanType.BY_TEXT_TOKEN, scanRange, continuation, scanProperties);
        };
    }

    /**
     * Determines whether this scan might return duplicate results for the same
     * record. This can happen if this is a prefix scan (as the same prefix might
     * correspond to multiple tokens in the same document) or if the index expression
     * itself creates duplicates.
     *
     * @return <code>true</code> if this scan might return multiple entries for the same record
     */
    public boolean createsDuplicates() {
        // TODO: This is actually too conservative
        //  If there is a repeated field in the index expression but the grouping key selects exactly one,
        //  then this doesn't actually create duplicates.
        return !textComparison.getType().isEquality() || index.getRootExpression().createsDuplicates();
    }

    /**
     * Get the index being scanned.
     *
     * @return the index being scanned
     */
    @Nonnull
    public Index getIndex() {
        return index;
    }

    /**
     * Get any grouping comparisons necessary to scan only within one grouping key.
     * These comparisons should evaluate to "all of" a given tuple range. If the
     * index does not have any grouping keys, this might return <code>null</code>
     * or an empty {@link ScanComparisons} object.
     *
     * @return the scan comparisons necessary to scan over the value of one grouping key
     */
    @Nullable
    public ScanComparisons getGroupingComparisons() {
        return groupingComparisons;
    }

    /**
     * Get the comparison performed on the text field. This will be some operation
     * like checking the field for the presence of one or more tokens. This might
     * end up producing multiple scans when run that are executed in parallel and
     * combined.
     *
     * @return the comparison performed on the index's text field
     */
    @Nonnull
    public Comparisons.TextComparison getTextComparison() {
        return textComparison;
    }

    /**
     * Get any comparisons performed on fields of the index following the text field.
     * This could be done to satisfy additional predicates on those fields after the
     * text predicate itself is satisfied. If there are no such comparisons, then this
     * might return <code>null</code> or an empty {@link ScanComparisons} object.
     *
     * @return any scan comparisons performed after the text predicate is satisfied
     */
    @Nullable
    public ScanComparisons getSuffixComparisons() {
        return suffixComparisons;
    }

    @Nonnull
    @Override
    public String toString() {
        return "TextScan(" + index.getName() + " " + groupingComparisons + ", " + textComparison + ", " + suffixComparisons + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null) {
            return false;
        } else if (!getClass().isInstance(o)) {
            return false;
        }
        TextScan that = (TextScan) o;
        return this.index.equals(that.index) && Objects.equals(groupingComparisons, that.groupingComparisons)
                && this.textComparison.equals(that.textComparison) && Objects.equals(suffixComparisons, that.suffixComparisons);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index.getName(), textComparison, groupingComparisons, suffixComparisons);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return PlanHashable.planHash(mode, textComparison, groupingComparisons, suffixComparisons) + index.getName().hashCode();
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, index.getName(), textComparison, groupingComparisons, suffixComparisons);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }
}
