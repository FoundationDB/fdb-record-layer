/*
 * QueryToKeyMatcher.java
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

package com.apple.foundationdb.record.query;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.BaseKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.Comparisons.Comparison;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComparison;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComponent;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.QueryKeyExpressionWithComparison;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;
import com.apple.foundationdb.record.query.plan.planning.FilterSatisfiedMask;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Match a query filter, represented by a {@link QueryComponent}, to a {@link KeyExpression}.
 * In particular, this will find a sequence of comparisons that can be applied to the elements
 * of a key expression in order to satisfy some (or all) of the query. For example, if the query
 * filter is <code>WHERE x = 1 AND y = 2</code> and this is matched against <code>concat(x, y)</code>,
 * then this will return <code>[EQUALS 1, EQUALS 2]</code>. This indicates that if one, for example,
 * had an index on <code>concat(x, y)</code>, then scanning over all index entries beginning with
 * <code>(1, 2)</code> would satisfy the query.
 *
 * <p>
 * There are two methods that attempt to match the query to a key expression. The first is the
 * {@link #matchesSatisfyingQuery(KeyExpression, FilterSatisfiedMask) matchesSatisfyingQuery()}
 * method. This should be used if one wants to make sure that one can use the given key expression
 * to retrieve all records that match the given query (i.e., the whether the fields included in the
 * key expression contain enough information to satisfy the entire query). If there are unsatisfied components
 * in the filter, this will return a match with type {@link MatchType#NO_MATCH NO_MATCH}.
 * The other mode is the {@link #matchesCoveringKey(KeyExpression, FilterSatisfiedMask) matchesCoveringKey()}
 * method. This attempts to find a list of comparisons such that every column of the key expression has been set
 * by some component of the filter. However, there may be components of the filter that are unsatisfied.
 * </p>
 *
 * <p>
 * By way of example, if the filter is <code>WHERE x = 1 AND y = 2</code> and the expression
 * is just on field <code>x</code>, then if one calls the <code>matchesSatisfyingQuery</code> method,
 * the matcher will return <code>NO_MATCH</code>, but it will return <code>[EQUALS 1]</code> if the
 * <code>matchesCoveringKey</code> method is called. By contrast, if the filter is <code>WHERE x = 1 AND y = 2</code>
 * and the expression is <code>concat(x, y, z)</code>, then calling <code>matchesSatisfyingQuery</code>
 * will return <code>[EQUALS 1, EQUALS 2]</code>, but calling <code>matchesCoveringKey</code> will
 * return <code>NO_MATCH</code>.
 * </p>
 */
@API(API.Status.INTERNAL)
public class QueryToKeyMatcher {

    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryToKeyMatcher.class);

    @Nonnull
    private static final List<Class<? extends KeyExpression>> KNOWN_KEY_EXPRESSIONS = ImmutableList.of(
            FieldKeyExpression.class,
            ThenKeyExpression.class,
            NestingKeyExpression.class,
            GroupingKeyExpression.class,
            KeyWithValueExpression.class,
            FunctionKeyExpression.class,
            LiteralKeyExpression.class,
            EmptyKeyExpression.class,
            RecordTypeKeyExpression.class,
            DimensionsKeyExpression.class
    );

    /**
     * Mode to control the kind of matching behavior desired.
     */
    private enum MatchingMode {
        /**
         * Attempt to satisfy the entire query. If the <code>QueryToKeyMatcher</code>
         * is configured in this mode, it should only return a match of non-{@link MatchType#NO_MATCH NO_MATCH}
         * type if the entire root query can be satisfied by matching it to this
         * key expression. There may be extraneous fields included as sub-expressions
         * in the matched expression.
         */
        SATISFY_QUERY,
        /**
         * Attempt to cover an entire key. If the <code>QueryToKeyMatcher</code>
         * is configured in this mode, it should only return a match of non-{@link MatchType#NO_MATCH NO_MATCH}
         * type if the full key expression is covered by the given query. There may be
         * unsatisfied components of the root query that this key does not satisfy.
         */
        COVER_KEY
    }

    @Nonnull
    private final QueryComponent rootQuery;

    /**
     * Whether a match was a successful, and if it was, whether the resulting keys have a single value.
     */
    public enum MatchType {
        EQUALITY,
        INEQUALITY,
        NO_MATCH
    }

    public QueryToKeyMatcher(@Nonnull QueryComponent rootQuery) {
        this.rootQuery = rootQuery;
    }

    /**
     * Attempt to match the expression to this matcher's root query and use every column in the key.
     * This will not track which components of the query are used when covering the given
     * expression. To do that, call {@link #matchesCoveringKey(KeyExpression, FilterSatisfiedMask)}
     * and provide a non-<code>null</code> value for the <code>filterMask</code> parameter.
     *
     * @param expression the key expression to match the root query to
     * @return a match if the entire expression is covered by the root query
     */
    @Nonnull
    public Match matchesCoveringKey(@Nonnull KeyExpression expression) {
        return matchesCoveringKey(expression, null);
    }

    /**
     * Attempt to match the expression to this matcher's root query and use every column in the key.
     * This will track which components of the query were used when covering the given expression
     * if <code>filterMask</code> is non-<code>null</code>.
     *
     * @param expression the key expression to match the root query to
     * @param filterMask a mask over this matcher's root query to track which filters have been satisfied
     * @return a match if the entire expression is covered by the root query
     */
    @Nonnull
    public Match matchesCoveringKey(@Nonnull KeyExpression expression, @Nullable FilterSatisfiedMask filterMask) {
        return matches(rootQuery, expression, MatchingMode.COVER_KEY, filterMask);
    }

    /**
     * Attempt to match the expression to this matcher's root query and satisfy every filter.
     * Most of the time when calling this method, tracking which filters have been satisfied should
     * be unnecessary as either they will be satisfied or this function will return a match
     * with <code>NO_MATCH</code> as its match type. However, if this is needed, one can
     * call {@link #matchesSatisfyingQuery(KeyExpression, FilterSatisfiedMask)} and provide a
     * non-<code>null</code> value for the <code>filterMask</code> parameter.
     *
     * @param expression the key expression to match the root query to
     * @return a match if the entire expression is satisfied by this expression
     */
    @Nonnull
    public Match matchesSatisfyingQuery(@Nonnull KeyExpression expression) {
        return matchesSatisfyingQuery(expression, null);
    }

    /**
     * Attempt to match the expression to this matcher's root query and satisfy every filter.
     * Most of the time when calling this method, tracking which filters have been satisfied should
     * be unnecessary as either they will be satisfied or this function will return a match
     * with <code>NO_MATCH</code> as its match type. However, if this is needed, one can
     * provide a non-<code>null</code> value for the <code>filterMask</code> parameter.
     *
     * @param expression the key expression to match the root query to
     * @param filterMask a mask over this matcher's root query to track which filters have been satisfied
     * @return a match if the entire expression is satisfied by this expression
     */
    @Nonnull
    public Match matchesSatisfyingQuery(@Nonnull KeyExpression expression, @Nullable FilterSatisfiedMask filterMask) {
        return matches(rootQuery, expression, MatchingMode.SATISFY_QUERY, filterMask);
    }

    @Nonnull
    private Match matches(@Nonnull QueryComponent query, @Nonnull KeyExpression key,
                          @Nonnull MatchingMode matchingMode, @Nullable FilterSatisfiedMask filterMask) {
        if (key instanceof GroupingKeyExpression) {
            KeyExpression group = extractGroupingKey((GroupingKeyExpression) key, matchingMode);
            return group == null ? Match.none() : matches(query, group, matchingMode, filterMask);
        }
        if (key instanceof DimensionsKeyExpression) {
            KeyExpression group = extractPrefixKey((DimensionsKeyExpression) key, matchingMode);
            return group == null ? Match.none() : matches(query, group, matchingMode, filterMask);
        }
        if (key instanceof KeyWithValueExpression) {
            KeyExpression onlyKey = extractKeyFromKeyWithValue((KeyWithValueExpression)key, matchingMode);
            return onlyKey == null ? Match.none() : matches(query, onlyKey, matchingMode, filterMask);
        }
        if (query instanceof NestedField) {
            return matches(((NestedField) query), key, matchingMode, filterMask);
        }
        if (query instanceof FieldWithComparison) {
            return matches(((FieldWithComparison) query), key, matchingMode, filterMask);
        }
        if (query instanceof OneOfThemWithComparison) {
            return matches(((OneOfThemWithComparison) query), key, matchingMode, filterMask);
        }
        if (query instanceof OneOfThemWithComponent) {
            return matches(((OneOfThemWithComponent) query), key, matchingMode, filterMask);
        }
        if (query instanceof AndComponent) {
            return matches((AndComponent)query, key, matchingMode, filterMask);
        }
        if (query instanceof RecordTypeKeyComparison) {
            return matches((RecordTypeKeyComparison)query, key, matchingMode, filterMask);
        }
        if (query instanceof QueryKeyExpressionWithComparison) {
            return matches((QueryKeyExpressionWithComparison)query, key, filterMask);
        }
        // Other component types (e.g., Or and Not components) are not handled by this
        // matcher and just return Match.none()
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(KeyValueLogMessage.of("unable to match query filter type",
                    LogMessageKeys.KEY_EXPRESSION, key,
                    LogMessageKeys.FILTER, query));
        }
        return Match.none();
    }

    @Nonnull
    private Match matches(@Nonnull AndComponent query, @Nonnull KeyExpression key,
                          @Nonnull MatchingMode matchingMode, @Nullable FilterSatisfiedMask filterMask) {
        final List<QueryComponent> listOfQueries = query.getChildren();
        final Iterator<KeyExpression> keyChildIterator;
        final int keyChildSize = key.getColumnSize();
        if (key instanceof ThenKeyExpression) {
            List<KeyExpression> children = ((ThenKeyExpression)key).getChildren();
            keyChildIterator = children.iterator();
        } else {
            keyChildIterator = Iterators.singletonIterator(key);
        }

        // Keep a local mask so that if we can only partially fulfill queries we don't pollute the main
        // one with our state
        FilterSatisfiedMask localMask = FilterSatisfiedMask.of(query);
        localMask.setExpression(key);
        List<Comparison> comparisons = new ArrayList<>(keyChildSize);
        while (keyChildIterator.hasNext()) {
            KeyExpression exp = keyChildIterator.next();

            // Look for a query segment that matches this expression
            boolean found = false;
            boolean foundInequality = false;
            final Iterator<FilterSatisfiedMask> childMaskIterator = localMask.getChildren().iterator();
            for (QueryComponent querySegment : listOfQueries) {
                Match match = matches(querySegment, exp, matchingMode, childMaskIterator.next());
                if (match.getType() != MatchType.NO_MATCH) {
                    found = true;
                    comparisons.addAll(match.getComparisons());
                    if (match.getType() == MatchType.INEQUALITY) {
                        foundInequality = true;
                    }
                    break;
                }
            }

            if (!found) {
                return Match.none();
            }

            // Only the last comparison in the list can be inequality.
            if (localMask.allSatisfied() || foundInequality) {
                break;
            }
        }

        if (matchingMode.equals(MatchingMode.SATISFY_QUERY) && !localMask.allSatisfied()
                || matchingMode.equals(MatchingMode.COVER_KEY) && comparisons.size() < keyChildSize) {
            // Either we do not have enough comparisons to have covered the full key, or there are some
            // filters that have not yet been satisfied.
            return Match.none();
        }

        if (filterMask != null) {
            filterMask.mergeWith(localMask);
        }
        return new Match(comparisons);
    }

    @Nonnull
    private Match matches(@Nonnull NestedField query, @Nonnull KeyExpression key,
                          @Nonnull MatchingMode matchingMode, @Nullable FilterSatisfiedMask filterMask) {
        if (key instanceof NestingKeyExpression) {
            return matches(query, (NestingKeyExpression) key, matchingMode, filterMask);
        } else if (key instanceof ThenKeyExpression) {
            final List<KeyExpression> children = ((ThenKeyExpression) key).getChildren();
            // Then should express in its contract, but this is good backup
            if (children.isEmpty() || matchingMode.equals(MatchingMode.COVER_KEY)) {
                return Match.none();
            } else {
                return matches(query, children.get(0), matchingMode, filterMask);
            }
        } else {
            return noMatchOrUnexpected(key);
        }

    }

    @Nonnull
    private Match matches(@Nonnull FieldWithComparison query, @Nonnull KeyExpression key,
                          @Nonnull MatchingMode matchingMode, @Nullable FilterSatisfiedMask filterMask) {
        if (key instanceof ThenKeyExpression) {
            final List<KeyExpression> children = ((ThenKeyExpression) key).getChildren();
            // Then should express in its contract, but this is good backup
            if (children.isEmpty() || matchingMode.equals(MatchingMode.COVER_KEY)) {
                return Match.none();
            } else {
                return matches(query, children.get(0), matchingMode, filterMask);
            }
        } else if (key instanceof FieldKeyExpression) {
            return matches(query, ((FieldKeyExpression) key), filterMask);
        } else {
            return noMatchOrUnexpected(key);
        }
    }

    @Nonnull
    private Match matches(@Nonnull OneOfThemWithComparison query, @Nonnull KeyExpression key,
                          @Nonnull MatchingMode matchingMode, @Nullable FilterSatisfiedMask filterMask) {
        if (key instanceof ThenKeyExpression) {
            final List<KeyExpression> children = ((ThenKeyExpression) key).getChildren();
            // Then should express in its contract, but this is good backup
            if (children.isEmpty() || matchingMode.equals(MatchingMode.COVER_KEY)) {
                return Match.none();
            } else {
                return matches(query, children.get(0), matchingMode, filterMask);
            }
        } else if (key instanceof FieldKeyExpression) {
            return matches(query, ((FieldKeyExpression)key), filterMask);
        } else {
            return noMatchOrUnexpected(key);
        }
    }

    @Nonnull
    private Match matches(@Nonnull OneOfThemWithComponent query, @Nonnull KeyExpression key,
                          @Nonnull MatchingMode matchingMode, @Nullable FilterSatisfiedMask filterMask) {
        if (key instanceof NestingKeyExpression) {
            return matches(query, (NestingKeyExpression)key, matchingMode, filterMask);
        } else {
            return noMatchOrUnexpected(key);
        }
    }

    @Nonnull
    private Match matches(@Nonnull NestedField query, @Nonnull NestingKeyExpression key,
                          @Nonnull MatchingMode matchingMode, @Nullable FilterSatisfiedMask filterMask) {
        if (key.getParent().getFanType() != KeyExpression.FanType.None) {
            // in theory, maybe, concatenate could work with certain things, but for now, no.
            return Match.none();
        } else {
            if (Objects.equals(query.getFieldName(), key.getParent().getFieldName())) {
                FilterSatisfiedMask childMask = filterMask != null ? filterMask.getChild(query.getChild()) : null;
                Match childMatch = matches(query.getChild(), key.getChild(), matchingMode, childMask);
                if (childMask != null && childMask.isSatisfied() && filterMask.getExpression() == null) {
                    filterMask.setExpression(key);
                }
                return childMatch;
            } else {
                return Match.none();
            }
        }
    }

    @Nonnull
    private Match matches(@Nonnull OneOfThemWithComponent query, @Nonnull NestingKeyExpression key,
                          @Nonnull MatchingMode matchingMode, @Nullable FilterSatisfiedMask filterMask) {
        if (key.getParent().getFanType() != KeyExpression.FanType.FanOut) {
            return Match.none();
        } else {
            if (Objects.equals(query.getFieldName(), key.getParent().getFieldName())) {
                FilterSatisfiedMask childMask = filterMask != null ? filterMask.getChild(query.getChild()) : null;
                Match childMatch = matches(query.getChild(), key.getChild(), matchingMode, childMask);
                if (childMask != null && childMask.isSatisfied() && filterMask.getExpression() == null) {
                    filterMask.setExpression(key);
                }
                return childMatch;
            } else {
                return Match.none();
            }
        }
    }

    @Nonnull
    private Match matches(@Nonnull FieldWithComparison query, @Nonnull FieldKeyExpression key,
                          @Nullable FilterSatisfiedMask filterMask) {
        if (!Objects.equals(query.getFieldName(), key.getFieldName())) {
            return Match.none();
        }
        if (key.getFanType() != KeyExpression.FanType.None) {
            return Match.none(); // fanout matches OneOfThemWithComparison
        }
        if (filterMask != null) {
            filterMask.setSatisfied(true);
            filterMask.setExpression(key);
        }
        return new Match(query.getComparison());
    }

    @Nonnull
    private Match matches(@Nonnull OneOfThemWithComparison query, @Nonnull FieldKeyExpression key,
                          @Nullable FilterSatisfiedMask filterMask) {
        if (!Objects.equals(query.getFieldName(), key.getFieldName())) {
            return Match.none();
        }
        if (key.getFanType() != KeyExpression.FanType.FanOut) {
            return Match.none();
        }
        if (filterMask != null) {
            filterMask.setSatisfied(true);
            filterMask.setExpression(key);
        }
        return new Match(query.getComparison());
    }

    @Nonnull
    private Match matches(@Nonnull RecordTypeKeyComparison query, @Nonnull KeyExpression key,
                          @Nonnull MatchingMode matchingMode, @Nullable FilterSatisfiedMask filterMask) {
        if (!(key instanceof RecordTypeKeyExpression ||
                (matchingMode.equals(MatchingMode.SATISFY_QUERY) && key instanceof ThenKeyExpression && ((ThenKeyExpression)key).getChildren().get(0) instanceof RecordTypeKeyExpression))) {
            return noMatchOrUnexpected(key);
        }
        if (filterMask != null) {
            filterMask.setSatisfied(true);
            filterMask.setExpression(key);
        }
        return new Match(query.getComparison());
    }

    @Nonnull
    private Match matches(@Nonnull QueryKeyExpressionWithComparison query, @Nonnull KeyExpression key,
                          @Nullable FilterSatisfiedMask filterMask) {
        if (!Objects.equals(query.getKeyExpression(), key)) {
            return Match.none();
        }
        if (filterMask != null) {
            filterMask.setSatisfied(true);
            filterMask.setExpression(key);
        }
        return new Match(query.getComparison());
    }

    @Nonnull
    private Match noMatchOrUnexpected(@Nonnull KeyExpression key) {
        if (KNOWN_KEY_EXPRESSIONS.stream().anyMatch(expressionClass -> expressionClass.isInstance(key))) {
            return Match.none();
        } else {
            return unexpected(key);
        }
    }

    @Nonnull
    private Match unexpected(@Nonnull KeyExpression key) {
        throw new KeyExpression.InvalidExpressionException("Unexpected Key Expression type " + key.getClass());
    }

    private KeyExpression extractGroupingKey(GroupingKeyExpression grouping, MatchingMode matchingMode) {
        try {
            return grouping.getGroupingSubKey();
        } catch (BaseKeyExpression.UnsplittableKeyExpressionException err) {
            if (matchingMode == MatchingMode.SATISFY_QUERY) {
                return extractPrefixUntilSplittable(grouping.getWholeKey(), grouping.getGroupingCount());
            }
        }
        return null;
    }

    private KeyExpression extractPrefixKey(DimensionsKeyExpression dimensionsKeyExpression, MatchingMode matchingMode) {
        try {
            return dimensionsKeyExpression.getPrefixSubKey();
        } catch (BaseKeyExpression.UnsplittableKeyExpressionException err) {
            if (matchingMode == MatchingMode.SATISFY_QUERY) {
                return extractPrefixUntilSplittable(dimensionsKeyExpression.getWholeKey(), dimensionsKeyExpression.getPrefixSize());
            }
        }
        return null;
    }

    @Nullable
    private KeyExpression extractKeyFromKeyWithValue(KeyWithValueExpression keyWithValue, MatchingMode matchingMode) {
        try {
            return keyWithValue.getKeyExpression();
        } catch (BaseKeyExpression.UnsplittableKeyExpressionException err) {
            // We can get here if the KeyWithValueExpression splits something that cannot be split, e.g.,
            // a function key expression. There's still a possibility that a prefix of the key still can
            // cover the entire query, so keep trying a more and more selective prefix of the key until
            // we no longer hit the unsplittable exception
            if (matchingMode == MatchingMode.SATISFY_QUERY) {
                return extractPrefixUntilSplittable(keyWithValue.getInnerKey(), keyWithValue.getSplitPoint());
            }
        }
        return null;
    }

    @Nonnull
    private KeyExpression extractPrefixUntilSplittable(KeyExpression wholeKeyExpression, int originalSplitPoint) {
        int adjustedSplitPoint = originalSplitPoint - 1;
        while (adjustedSplitPoint >= 0) {
            try {
                return wholeKeyExpression.getSubKey(0, adjustedSplitPoint);
            } catch (BaseKeyExpression.UnsplittableKeyExpressionException innerErr) {
                adjustedSplitPoint--;
            }
        }
        // We should always be able to extract 0 columns, so this should be unreachable code
        throw new RecordCoreException("unable to extract splittable prefix from key expression")
                .addLogInfo(LogMessageKeys.KEY_EXPRESSION, wholeKeyExpression)
                .addLogInfo(LogMessageKeys.COLUMN_SIZE, originalSplitPoint);
    }

    /**
     * The result of matching a particular {@link KeyExpression}.
     */
    public static class Match {
        @Nonnull
        private final List<Comparison> comparisons;
        @Nonnull
        private final MatchType type;

        private Match(@Nonnull MatchType type) {
            comparisons = Collections.emptyList();
            this.type = type;
        }

        public Match(@Nonnull List<Comparison> inComparisons) {
            comparisons = new ArrayList<>();
            comparisons.addAll(inComparisons);
            type = inComparisons.get(inComparisons.size() - 1).getType().isEquality() ? MatchType.EQUALITY : MatchType.INEQUALITY;
        }

        public Match(@Nonnull Comparison comparison) {
            comparisons = Collections.singletonList(comparison);
            type = comparison.getType().isEquality() ? MatchType.EQUALITY : MatchType.INEQUALITY;
        }

        @Nonnull
        private static Match none() {
            return new Match(MatchType.NO_MATCH);
        }

        @Nonnull
        public Key.Evaluated getEquality() {
            return getEquality(null, null);
        }

        @Nonnull
        public Key.Evaluated getEquality(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            List<Object> evaluated = new ArrayList<>();
            for (Comparison comparison : comparisons) {
                if (comparison.getType().isEquality()) {
                    evaluated.add(comparison.getComparand(store, context));
                } else {
                    throw new RecordCoreException(
                            "Match is not equal, has comparison of type " + comparison.getType());
                }
            }
            return Key.Evaluated.concatenate(evaluated);
        }

        @Nonnull
        public List<Comparison> getEqualityComparisons() {
            if (comparisons.isEmpty() || comparisons.get(comparisons.size() - 1).getType().isEquality()) {
                return comparisons;
            } else {
                return comparisons.subList(0, comparisons.size() - 1);
            }
        }

        @Nonnull
        public List<Comparison> getComparisons() {
            return comparisons;
        }

        @Nonnull
        public Comparison getFirstComparison() {
            return comparisons.get(0);
        }

        @Nonnull
        public MatchType getType() {
            return type;
        }
    }
}
