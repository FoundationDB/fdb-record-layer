/*
 * KeyExpressionComparisons.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionWithChild;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionWithChildren;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionWithoutChildren;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.query.expressions.ComponentWithComparison;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * A data structure that represents a {@link KeyExpression} with a {@link ComparisonRange} on some of its sub-expressions.
 *
 * <p>
 * A {@code KeyExpressionComparisons} is an analogue of a {@link ScanComparisons} that maintains information about which
 * parts of a {@link KeyExpression} correspond to each comparison. It maintains a (private) tree that mirrors the
 * underlying key expression and maps each part of the key expression to a {@link ComparisonRange} that represents
 * a contiguous range of values for that part of the key expression. The {@link #matchWith(ComponentWithComparison)}
 * method can then take a {@link ComponentWithComparison} and try to add it to the existing comparisons.
 * </p>
 *
 * <p>
 * The {@code KeyExpressionComparisons} encapsulates the core logic of marking parts of a complex key expression as
 * satisfied by various types of {@code Comparisons.Comparison}s and attempting to match more comparisons to the
 * remaining parts of the expression. It is also designed to support queries about compatible sort orders, given the
 * set of existing comparisons.
 * <!-- TODO Add ability to check sort orders (after accounting for equality comparisons) to a KeyExpressionComparisons. -->
 * </p>
 *
 * <p>
 * While this sounds a lot like a whole query planner, the scope of a {@code KeyExpressionComparisons} is intentionally
 * very limited. In particular, it supports matching with a {@link ComponentWithComparison} rather than a more
 * general {@link com.apple.foundationdb.record.query.expressions.QueryComponent}. As a result, the logic for handling
 * Boolean operations, nested messages, and other complexities does not belong in {@code KeyExpressionComparisons}.
 * This is especially important becuase it relies on {@code instanceof} checking to properly match query components with
 * compatible key expressions; this code is vastly simpler because it only needs to consider
 * {@link ComponentWithComparison}s.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class KeyExpressionComparisons {
    @Nonnull
    private final KeyExpressionWithComparison root;

    public KeyExpressionComparisons(@Nonnull KeyExpression keyExpression) {
        this.root = KeyExpressionWithComparison.from(this, keyExpression);
    }

    private KeyExpressionComparisons(@Nonnull KeyExpressionWithComparison root) {
        this.root = root;
    }

    @Nonnull
    public Optional<KeyExpressionComparisons> matchWith(@Nonnull ComponentWithComparison component) {
        return root.matchWith(component).map(KeyExpressionComparisons::new);
    }

    @Nonnull
    public ScanComparisons toScanComparisons() {
        ScanComparisons.Builder builder = new ScanComparisons.Builder();
        root.addToScanComparisonsBuilder(builder);
        return builder.build();
    }

    private enum MatchedComparisonType {
        NOT_MATCHED,
        MATCHED,
        INEQUALITY_MATCHED;

        public static MatchedComparisonType from(@Nonnull ComparisonRange comparisonRange) {
            if (comparisonRange.isEmpty()) {
                return NOT_MATCHED;
            } else if (comparisonRange.isInequality()) {
                return INEQUALITY_MATCHED;
            } else {
                return MATCHED;
            }
        }
    }

    private static class KeyExpressionWithComparison {
        @Nonnull
        private final KeyExpressionComparisons root;
        @Nonnull
        private final KeyExpression keyExpression;
        @Nonnull
        private final List<KeyExpressionWithComparison> children;
        @Nonnull
        private final ComparisonRange comparison;

        private KeyExpressionWithComparison(@Nonnull KeyExpressionComparisons root,
                                            @Nonnull KeyExpression keyExpression,
                                            @Nonnull List<KeyExpressionWithComparison> children,
                                            @Nonnull ComparisonRange comparison) {
            this.root = root;
            this.keyExpression = keyExpression;
            this.children = children;
            this.comparison = comparison;
        }

        @Nonnull
        public MatchedComparisonType getMatchedComparisonType() {
            if (keyExpression instanceof KeyExpressionWithoutChildren) {
                return MatchedComparisonType.from(comparison);
            }
            if (keyExpression instanceof ThenKeyExpression) {
                boolean seenInequality = false;
                for (KeyExpressionWithComparison child : children) {
                    switch (child.getMatchedComparisonType()) {
                        case NOT_MATCHED:
                            return MatchedComparisonType.NOT_MATCHED;
                        case INEQUALITY_MATCHED:
                            seenInequality = true;
                            break;
                        default:
                            continue;
                    }
                }
                if (seenInequality) {
                    return MatchedComparisonType.INEQUALITY_MATCHED;
                } else {
                    return MatchedComparisonType.MATCHED;
                }
            }
            // TODO support other possible key expressions here
            return MatchedComparisonType.NOT_MATCHED;
        }

        @Nullable
        public ComparisonRange getComparison() {
            return comparison;
        }

        @Nonnull
        private KeyExpressionWithComparison withComparison(@Nonnull ComparisonRange comparison) {
            return new KeyExpressionWithComparison(root, keyExpression, children, comparison);
        }

        @Nonnull
        private KeyExpressionWithComparison withChild(@Nonnull KeyExpressionWithComparison newChild) {
            return withChildren(Collections.singletonList(newChild));
        }

        @Nonnull
        private KeyExpressionWithComparison withChildren(@Nonnull List<KeyExpressionWithComparison> newChildren) {
            return new KeyExpressionWithComparison(root, keyExpression, newChildren, comparison);
        }

        @Nonnull
        public Optional<KeyExpressionWithComparison> matchWith(@Nonnull ComponentWithComparison component) {
            if (keyExpression instanceof ThenKeyExpression) {
                return matchWithThen(component);
            }
            if (keyExpression instanceof GroupingKeyExpression) {
                return children.get(0).matchWith(component).map(this::withChild);
            }
            if (keyExpression instanceof KeyWithValueExpression) {
                return children.get(0).matchWith(component).map(this::withChild);
            }
            if (keyExpression instanceof FieldKeyExpression && component instanceof FieldWithComparison) {
                FieldWithComparison fieldWithComparison = (FieldWithComparison) component;
                if (fieldWithComparison.getFieldName().equals(((FieldKeyExpression)keyExpression).getFieldName())) {
                    return comparison.tryToAdd(fieldWithComparison.getComparison()).map(this::withComparison);
                }
                return Optional.empty();
            }
            if (keyExpression instanceof RecordTypeKeyExpression && component instanceof RecordTypeKeyComparison) {
                return comparison.tryToAdd(component.getComparison()).map(this::withComparison);
            }
            return Optional.empty();
        }

        private Optional<KeyExpressionWithComparison> matchWithThen(@Nonnull ComponentWithComparison component) {
            Optional<KeyExpressionWithComparison> matchedChildResult = Optional.empty();
            int currentChild = -1;
            boolean shouldContinue = true;
            while (shouldContinue && currentChild < children.size()) {
                currentChild++;
                KeyExpressionWithComparison child = children.get(currentChild);
                Optional<KeyExpressionWithComparison> childResult = child.matchWith(component);
                if (childResult.isPresent()) {
                    matchedChildResult = childResult;
                    // Found a match, so stop looking.
                    shouldContinue = false;
                } else if (!child.getMatchedComparisonType().equals(MatchedComparisonType.MATCHED)) {
                    // One of the following cases applies:
                    // (1) There's no recorded match for this child. We have to match the current component to this
                    //     child, otherwise we can't match it at all (at least until other matches happen first).
                    // (2) We already have an inequality match for this child. We can try to match the current child, but
                    //     we can't add comparisons to a later child, so we have to stop if this match fails.
                    shouldContinue = false;
                }
            }

            if (matchedChildResult.isPresent()) {
                List<KeyExpressionWithComparison> newChildren = new ArrayList<>(children);
                newChildren.set(currentChild, matchedChildResult.get());
                return Optional.of(withChildren(newChildren));
            }
            return Optional.empty();
        }

        public void addToScanComparisonsBuilder(@Nonnull ScanComparisons.Builder builder) {
            if (keyExpression instanceof KeyExpressionWithoutChildren) {
                if (!comparison.isEmpty()) {
                    builder.addComparisonRange(comparison);
                }
            } else if (keyExpression instanceof ThenKeyExpression) {
                for (KeyExpressionWithComparison child : children) {
                    child.addToScanComparisonsBuilder(builder);
                }
            } else if (keyExpression instanceof GroupingKeyExpression) {
                children.get(0).addToScanComparisonsBuilder(builder);
            } else if (keyExpression instanceof KeyWithValueExpression) {
                children.get(0).addToScanComparisonsBuilder(builder);
            }
        }

        public static KeyExpressionWithComparison from(@Nonnull KeyExpressionComparisons root, @Nonnull KeyExpression keyExpression) {
            if (keyExpression instanceof KeyExpressionWithoutChildren) {
                return new KeyExpressionWithComparison(root, keyExpression, Collections.emptyList(), ComparisonRange.EMPTY);
            } else if (keyExpression instanceof KeyExpressionWithChild) {
                return new KeyExpressionWithComparison(root, keyExpression,
                        Collections.singletonList(from(root, ((KeyExpressionWithChild)keyExpression).getChild())),
                        ComparisonRange.EMPTY);
            } else if (keyExpression instanceof KeyExpressionWithChildren) {
                ImmutableList.Builder<KeyExpressionWithComparison> builder = ImmutableList.builder();
                for (KeyExpression child : ((KeyExpressionWithChildren)keyExpression).getChildren()) {
                    builder.add(from(root, child));
                }
                return new KeyExpressionWithComparison(root, keyExpression, builder.build(), ComparisonRange.EMPTY);
            } else {
                throw new RecordCoreException("found key expression that does not implement proper interfaces");
            }
        }
    }
}
