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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Key;
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
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.Comparisons.Comparison;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComparison;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Match {@link QueryComponent} to {@link KeyExpression}.
 *
 * The result of a successful equality match is a set of values for the keys in the key expression.
 * For example, given <code>WHERE x = 1 AND y = 2</code> matched against <code>concat(x, y)</code>, <code>1, 2</code>.
 *
 */
public class QueryToKeyMatcher {
    private final QueryComponent rootQuery;


    /**
     * Whether a match was a successful, and if it was, whether the resulting keys have a single value.
     */
    public enum MatchType {
        EQUALITY,
        INEQUALITY,
        NO_MATCH
    }

    public QueryToKeyMatcher(QueryComponent rootQuery) {
        this.rootQuery = rootQuery;
    }

    public Match matches(KeyExpression expression) {
        return matches(rootQuery, expression);
    }

    private Match matches(AndComponent query, ThenKeyExpression key) {
        List<QueryComponent> listOfQueries = query.getChildren();

        Map<String, QueryComponent> mapFromKeyPathToQuery = new HashMap<>();
        for (QueryComponent querySegment : listOfQueries) {
            String path = pathFromQuery(querySegment);
            if (path == null) {
                throw new Query.InvalidExpressionException("We only support Query.And([Field.WithComparison, ]) " +
                        querySegment.getClass());
            }
            mapFromKeyPathToQuery.put(path, querySegment);
        }

        if (mapFromKeyPathToQuery.size() != listOfQueries.size()) {
            throw new Query.InvalidExpressionException("We do not support Query.And([f(a), f(a)]), " +
                    " no two elements in the list of queries we 'and' with each other can use the same key. " + query
                    .getClass());
        }

        List<Comparison> comparisons = new ArrayList<>();

        for (int index = 0; index < key.getChildren().size(); index++) {
            KeyExpression exp = key.getChildren().get(index);
            String path = pathFromKey(exp);
            if (path == null) {
                return Match.none();
            }

            QueryComponent querySegment = mapFromKeyPathToQuery.get(path);
            if (querySegment == null) {
                return Match.none();
            }

            Match match = matches(querySegment, exp);
            if (match.getType() == MatchType.NO_MATCH) {
                return Match.none();
            }

            Comparison comp = match.getFirstComparison();
            comparisons.add(comp);

            // Only the last comparison in the list can be inequality.
            if (comparisons.size() >= listOfQueries.size() || !comp.getType().isEquality()) {
                break;
            }
        }

        if (comparisons.size() != listOfQueries.size()) {
            return Match.none();
        }

        return new Match(comparisons);
    }

    private Match matches(QueryComponent query, KeyExpression key) {
        if (key instanceof GroupingKeyExpression) {
            return matches(query, ((GroupingKeyExpression) key).getWholeKey());
        }
        if (key instanceof KeyWithValueExpression) {
            return matches(query, ((KeyWithValueExpression) key).getKeyExpression());
        }
        // Both of these COULD be used for matching by the planner, but they aren't today, so...
        if (key instanceof FunctionKeyExpression
                || key instanceof LiteralKeyExpression) {
            return Match.none();
        }
        if (query instanceof NestedField) {
            return matches(((NestedField) query), key);
        }
        if (query instanceof FieldWithComparison) {
            return matches(((FieldWithComparison) query), key);
        }
        if (query instanceof OneOfThemWithComparison) {
            return matches(((OneOfThemWithComparison) query), key);
        }
        if (query instanceof OneOfThemWithComponent) {
            return matches(((OneOfThemWithComponent) query), key);
        }
        if (query instanceof AndComponent && key instanceof ThenKeyExpression) {
            return matches((AndComponent)query, (ThenKeyExpression)key);
        }
        if (query instanceof RecordTypeKeyComparison) {
            return matches((RecordTypeKeyComparison)query, key);
        }

        throw new Query.InvalidExpressionException(
                "Only fields and Nested Fields are allowed, for now " + query.getClass());
    }

    private Match matches(NestedField query, KeyExpression key) {
        if (key instanceof NestingKeyExpression) {
            return matches(query, (NestingKeyExpression) key);
        } else if (key instanceof ThenKeyExpression) {
            final List<KeyExpression> children = ((ThenKeyExpression) key).getChildren();
            // Then should express in its contract, but this is good backup
            if (children.isEmpty()) {
                return Match.none();
            } else {
                return matches(query, children.get(0));
            }
        } else if (key instanceof FieldKeyExpression) {
            return Match.none();
        } else if (key instanceof EmptyKeyExpression) {
            return Match.none();
        } else {
            return unexpected(key);
        }

    }

    private Match matches(FieldWithComparison query, KeyExpression key) {
        if (key instanceof NestingKeyExpression) {
            return Match.none();
        } else if (key instanceof ThenKeyExpression) {
            final List<KeyExpression> children = ((ThenKeyExpression) key).getChildren();
            // Then should express in its contract, but this is good backup
            if (children.isEmpty()) {
                return Match.none();
            } else {
                return matches(query, children.get(0));
            }
        } else if (key instanceof GroupingKeyExpression) {
            return matches(query, ((GroupingKeyExpression) key).getWholeKey());
        } else if (key instanceof FieldKeyExpression) {
            return matches(query, ((FieldKeyExpression) key));
        } else if (key instanceof EmptyKeyExpression) {
            return Match.none();
        } else {
            return unexpected(key);
        }
    }

    private Match matches(OneOfThemWithComparison query, KeyExpression key) {
        if (key instanceof NestingKeyExpression) {
            return Match.none();
        } else if (key instanceof ThenKeyExpression) {
            final List<KeyExpression> children = ((ThenKeyExpression) key).getChildren();
            // Then should express in its contract, but this is good backup
            if (children.isEmpty()) {
                return Match.none();
            } else {
                return matches(query, children.get(0));
            }
        } else if (key instanceof GroupingKeyExpression) {
            return matches(query, ((GroupingKeyExpression) key).getWholeKey());
        } else if (key instanceof FieldKeyExpression) {
            return matches(query, ((FieldKeyExpression) key));
        } else if (key instanceof EmptyKeyExpression) {
            return Match.none();
        } else {
            return unexpected(key);
        }
    }

    private Match matches(OneOfThemWithComponent query, KeyExpression key) {
        if (key instanceof NestingKeyExpression) {
            return matches(query, (NestingKeyExpression) key);
        } else if (key instanceof ThenKeyExpression ||
                   key instanceof FieldKeyExpression ||
                   key instanceof GroupingKeyExpression ||
                   key instanceof EmptyKeyExpression) {
            return Match.none();
        } else {
            return unexpected(key);
        }
    }

    private Match matches(NestedField query, NestingKeyExpression key) {
        if (key.getParent().getFanType() != KeyExpression.FanType.None) {
            // in theory, maybe, concatenate could work with certain things, but for now, no.
            return Match.none();
        } else {
            if (Objects.equals(query.getFieldName(), key.getParent().getFieldName())) {
                return matches(query.getChild(), key.getChild());
            } else {
                return Match.none();
            }
        }
    }

    private Match matches(OneOfThemWithComponent query, NestingKeyExpression key) {
        if (key.getParent().getFanType() != KeyExpression.FanType.FanOut) {
            return Match.none();
        } else {
            if (Objects.equals(query.getFieldName(), key.getParent().getFieldName())) {
                return matches(query.getChild(), key.getChild());
            } else {
                return Match.none();
            }
        }
    }

    private Match matches(FieldWithComparison query, FieldKeyExpression key) {
        if (!Objects.equals(query.getFieldName(), key.getFieldName())) {
            return Match.none();
        }
        if (key.getFanType() != KeyExpression.FanType.None) {
            return Match.none(); // fanout matches OneOfThemWithComparison
        }
        return new Match(query.getComparison());
    }

    private Match matches(OneOfThemWithComparison query, FieldKeyExpression key) {
        if (!Objects.equals(query.getFieldName(), key.getFieldName())) {
            return Match.none();
        }
        if (key.getFanType() != KeyExpression.FanType.FanOut) {
            return Match.none();
        }
        return new Match(query.getComparison());
    }

    private Match matches(RecordTypeKeyComparison query, KeyExpression key) {
        if (!(key instanceof RecordTypeKeyExpression ||
                (key instanceof ThenKeyExpression && ((ThenKeyExpression)key).getChildren().get(0) instanceof RecordTypeKeyExpression))) {
            return Match.none();
        }
        return new Match(query.getComparison());
    }

    private Match unexpected(KeyExpression key) {
        throw new KeyExpression.InvalidExpressionException("Unexpected Key Expression type " + key.getClass());
    }

    private static String pathFromQuery(QueryComponent query) {
        if (query instanceof FieldWithComparison) {
            return ((FieldWithComparison)query).getFieldName();
        }
        if (query instanceof NestedField) {
            String child = pathFromQuery(((NestedField)query).getChild());
            if (child == null) {
                return null;
            }
            return ((NestedField)query).getFieldName() + "/" + child;
        }
        if (query instanceof OneOfThemWithComparison) {
            return ((OneOfThemWithComparison)query).getFieldName();
        }
        if (query instanceof OneOfThemWithComponent) {
            String child = pathFromQuery(((OneOfThemWithComponent)query).getChild());
            if (child == null) {
                return null;
            }
            return ((OneOfThemWithComponent)query).getFieldName() + "/" + child;
        }
        if (query instanceof RecordTypeKeyComparison) {
            return "@";
        }
        return null;
    }

    private static String pathFromKey(KeyExpression key) {
        if (key instanceof FieldKeyExpression) {
            return ((FieldKeyExpression)key).getFieldName();
        }
        if (key instanceof NestingKeyExpression) {
            String child = pathFromKey(((NestingKeyExpression)key).getChild());
            if (child == null) {
                return null;
            }
            return ((NestingKeyExpression)key).getParent().getFieldName() + "/" + child;
        }
        if (key instanceof RecordTypeKeyExpression) {
            return "@";
        }
        return null;
    }

    /**
     * The result of matching a particular {@link KeyExpression}.
     */
    public static class Match {
        private final List<Comparison> comparisons;
        private final MatchType type;

        private Match(MatchType type) {
            comparisons = Collections.emptyList();
            this.type = type;
        }

        public Match(List<Comparison> inComparisons) {
            comparisons = new ArrayList<>();
            comparisons.addAll(inComparisons);
            type = inComparisons.get(inComparisons.size() - 1).getType().isEquality() ? MatchType.EQUALITY : MatchType.INEQUALITY;
        }

        public Match(Comparison comparison) {
            comparisons = Collections.singletonList(comparison);
            type = comparison.getType().isEquality() ? MatchType.EQUALITY : MatchType.INEQUALITY;
        }

        private static Match none() {
            return new Match(MatchType.NO_MATCH);
        }

        public Key.Evaluated getEquality() {
            return getEquality(null);
        }

        public Key.Evaluated getEquality(@Nullable EvaluationContext context) {
            List<Object> evaluated = new ArrayList<>();
            for (Comparison comparison : comparisons) {
                if (comparison.getType().isEquality()) {
                    evaluated.add(comparison.getComparand(context));
                } else {
                    throw new RecordCoreException(
                            "Match is not equal, has comparison of type " + comparison.getType());
                }
            }
            return Key.Evaluated.concatenate(evaluated);
        }

        public List<Comparison> getEqualityComparisons() {
            if (comparisons.isEmpty() || comparisons.get(comparisons.size() - 1).getType().isEquality()) {
                return comparisons;
            } else {
                return comparisons.subList(0, comparisons.size() - 1);
            }
        }

        public Comparison getFirstComparison() {
            return comparisons.get(0);
        }

        public MatchType getType() {
            return type;
        }
    }
}
