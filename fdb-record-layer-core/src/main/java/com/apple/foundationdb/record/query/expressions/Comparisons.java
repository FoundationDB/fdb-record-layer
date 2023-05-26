/*
 * Comparisons.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.QueryHashable;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TupleFieldsProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.InvertibleFunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.provider.common.text.TextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextTokenizerRegistry;
import com.apple.foundationdb.record.provider.common.text.TextTokenizerRegistryImpl;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.cursors.ProbableIntersectionCursor;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.util.HashUtils;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Internal;
import com.google.protobuf.ProtocolMessageEnum;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Helper methods for building {@link Comparison}s.
 */
@API(API.Status.STABLE)
public class Comparisons {
    public static final Comparison LIST_EMPTY = new ListComparison(Type.EQUALS, Collections.emptyList());

    private Comparisons() {
    }

    // A simple wrapper around bytes that performs unsigned comparisons with
    // other instances of UnsignedBytes, which is useful for comparisons consistent
    // with those done on byte arrays by the underlying database.
    private static class UnsignedBytes implements Comparable<UnsignedBytes> {
        @Nonnull
        private byte[] data;

        public UnsignedBytes(@Nonnull byte[] data) {
            this.data = data;
        }

        @Override
        public int compareTo(@Nonnull UnsignedBytes other) {
            return ByteArrayUtil.compareUnsigned(data, other.data);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof UnsignedBytes && this.compareTo((UnsignedBytes)o) == 0;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }

    // Java built-in UUID does signed compare, making it incompatible with the order of toString() and with the FDB Tuple encoding
    // and with almost every other language's implementation (https://bugs.openjdk.java.net/browse/JDK-7025832).
    private static class UnsignedUUID implements Comparable<UnsignedUUID> {
        private final long mostSignificantBits;
        private final long leastSignificantBits;

        private UnsignedUUID(long mostSignificantBits, long leastSignificantBits) {
            this.mostSignificantBits = mostSignificantBits;
            this.leastSignificantBits = leastSignificantBits;
        }

        public long getMostSignificantBits() {
            return mostSignificantBits;
        }

        public long getLeastSignificantBits() {
            return leastSignificantBits;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UnsignedUUID that = (UnsignedUUID)o;
            return this.mostSignificantBits == that.mostSignificantBits &&
                   this.leastSignificantBits == that.leastSignificantBits;
        }

        @Override
        public int hashCode() {
            return Objects.hash(mostSignificantBits, leastSignificantBits);
        }

        @Override
        public int compareTo(@Nonnull UnsignedUUID that) {
            int msbCompare = Long.compareUnsigned(this.mostSignificantBits, that.mostSignificantBits);
            if (msbCompare != 0) {
                return msbCompare;
            }
            return Long.compareUnsigned(this.leastSignificantBits, that.leastSignificantBits);
        }
    }

    @SuppressWarnings("rawtypes")
    private static Comparable toComparable(@Nullable Object obj) {
        if (obj == null) {
            return null;
        } else if (obj instanceof ByteString) {
            return new UnsignedBytes(((ByteString) obj).toByteArray());
        } else if (obj instanceof byte[]) {
            return new UnsignedBytes((byte[])obj);
        } else if (obj instanceof UUID) {
            UUID uuid = (UUID)obj;
            return new UnsignedUUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        } else if (obj instanceof Comparable) {
            return (Comparable) obj;
        } else {
            throw new RecordCoreException("Tried to compare non-comparable object " + obj.getClass());
        }
    }

    @SuppressWarnings("rawtypes")
    private static Object toClassWithRealEquals(@Nullable Object obj) {
        if (obj == null) {
            return null;
        } else if (obj instanceof ByteString) {
            return obj;
        } else if (obj instanceof byte[]) {
            return ByteString.copyFrom((byte[])obj);
        } else if (obj instanceof Comparable) {
            return obj;
        } else if (obj instanceof List) {
            return obj;
        } else {
            throw new RecordCoreException("Tried to compare non-comparable object " + obj.getClass());
        }
    }

    @SuppressWarnings("unchecked")
    public static int compare(@Nullable Object fieldValue, @Nullable Object comparand) {
        if (fieldValue == null) {
            if (comparand == null) {
                return 0;
            } else {
                return -1;
            }
        } else if (comparand == null) {
            return 1;
        } else {
            return toComparable(fieldValue).compareTo(toComparable(comparand));
        }
    }

    @Nullable
    @SpotBugsSuppressWarnings("NP_BOOLEAN_RETURN_NULL")
    private static Boolean compareEquals(Object value, Object comparand) {
        if (value == null || comparand == null) {
            return null;
        } else {
            return toClassWithRealEquals(value).equals(toClassWithRealEquals(comparand));
        }
    }

    @Nullable
    @SpotBugsSuppressWarnings("NP_BOOLEAN_RETURN_NULL")
    private static Boolean compareStartsWith(@Nullable Object value, @Nullable Object comparand) {
        if (value == null || comparand == null) {
            return null;
        } else if (comparand instanceof String) {
            return ((String)value).startsWith((String)comparand);
        } else if ((comparand instanceof ByteString) || (comparand instanceof byte[])) {
            final byte[] bcomp = (comparand instanceof byte[]) ?
                    (byte[])comparand : ((ByteString)comparand).toByteArray();
            final byte[] bval =  (value instanceof byte[]) ?
                    (byte[])value : ((ByteString)value).toByteArray();
            return ((bval.length >= bcomp.length) &&
                    Arrays.equals(Arrays.copyOfRange(bval, 0, bcomp.length), bcomp));
        } else if (comparand instanceof List<?>) {
            return compareListStartsWith(value, (List<?>) comparand);
        } else {
            throw new RecordCoreException("Illegal comparand value type: " + comparand);
        }
    }

    private static Boolean compareListStartsWith(@Nullable Object value, @Nullable List<?> comparand) {
        if (value instanceof List<?>) {
            List<?> list = (List<?>) value;
            for (int i = 0; i < comparand.size(); i++) {
                if (i > list.size()) {
                    return false;
                }
                if (!comparand.get(i).equals(list.get(i))) {
                    return false;
                }
            }
            return true;
        } else {
            throw new RecordCoreException("value from record did not match comparand");
        }
    }

    @Nullable
    @SpotBugsSuppressWarnings("NP_BOOLEAN_RETURN_NULL")
    private static Boolean compareIn(@Nullable Object value, @Nullable Object comparand) {
        if (value == null || comparand == null) {
            return null;
        }
        if ((comparand instanceof List<?>)) {
            boolean hasNull = false;
            value = toClassWithRealEquals(value);
            for (Object comparandItem : (List<?>) comparand) {
                if (value.equals(toClassWithRealEquals(comparandItem))) {
                    return true;
                }
                hasNull |= comparandItem == null;
            }
            return hasNull ? null : false;
        } else {
            throw new RecordCoreException("IN comparison with a non-list type" + comparand.getClass());
        }
    }

    @Nullable
    private static Boolean compareTextContainsSingle(@Nonnull Iterator<? extends CharSequence> valueIterator, @Nonnull String comparandToken) {
        if (comparandToken.isEmpty()) {
            // The comparand is a stop word. We cannot make a determination
            // one way or the other.
            return null;
        }
        while (valueIterator.hasNext()) {
            final String nextToken = valueIterator.next().toString();
            if (!nextToken.isEmpty() && nextToken.equals(comparandToken)) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    @Nullable
    private static Boolean compareTextContainsPrefix(@Nonnull Iterator<? extends CharSequence> valueIterator, @Nonnull String comparandToken) {
        if (comparandToken.isEmpty()) {
            // The comparand is a stop word. We cannot make a determination
            // one way or the other.
            return null;
        }
        while (valueIterator.hasNext()) {
            final String nextToken = valueIterator.next().toString();
            if (!nextToken.isEmpty() && nextToken.startsWith(comparandToken)) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    @Nonnull
    private static Set<String> getComparandSet(@Nonnull List<String> comparandList) {
        if (comparandList.isEmpty()) {
            return Collections.emptySet();
        } else if (comparandList.size() == 1) {
            final String comparand = comparandList.get(0);
            if (comparand.isEmpty()) {
                return Collections.emptySet();
            } else {
                return Collections.singleton(comparand);
            }
        } else {
            final Set<String> comparandSet = new HashSet<>(comparandList);
            comparandSet.remove("");
            return comparandSet;
        }
    }

    @Nullable
    private static Boolean compareTextContainsAll(@Nonnull Iterator<? extends CharSequence> valueIterator, @Nonnull List<String> comparand) {
        final Set<String> comparandSet = getComparandSet(comparand);
        if (comparandSet.isEmpty()) {
            return null;
        }
        if (comparandSet.size() == 1) {
            return compareTextContainsSingle(valueIterator, comparandSet.iterator().next());
        }

        final Set<String> matchedSet = new HashSet<>((int)(comparandSet.size() * 1.5));
        while (valueIterator.hasNext()) {
            final String nextToken = valueIterator.next().toString();
            if (!nextToken.isEmpty() && comparandSet.contains(nextToken)) {
                matchedSet.add(nextToken);

                if (matchedSet.size() == comparandSet.size()) {
                    // We've found as many as are in the comparand set, so we know the sets are the same.
                    return Boolean.TRUE;
                }
            }
        }
        return Boolean.FALSE;
    }

    @Nullable
    private static Boolean compareTextContainsAllWithin(@Nonnull Iterator<? extends CharSequence> valueIterator, @Nonnull List<String> comparand, int maxDistance) {
        final Set<String> comparandSet = getComparandSet(comparand);
        if (comparandSet.isEmpty()) {
            return null;
        }
        if (comparandSet.size() == 1) {
            return compareTextContainsSingle(valueIterator, comparandSet.iterator().next());
        }

        // Maintain a queue of the last maxDistance tokens. Then keep a histogram
        // of the number of times we've seen each token we care about in that
        // range. Then we know we've seen all of them when the size of the
        // map is equal to the size of the set.
        final Map<String, Integer> seenMap = new HashMap<>(comparandSet.size());
        final Queue<String> lastTokensQueue = new ArrayDeque<>(maxDistance);
        while (valueIterator.hasNext()) {
            final String nextToken = valueIterator.next().toString();
            if (!nextToken.isEmpty() && comparandSet.contains(nextToken)) {
                // We have a match. Add one (or set the count to 1) for the
                // matched token.
                seenMap.merge(nextToken, 1, Integer::sum);
                if (seenMap.size() == comparandSet.size()) {
                    return Boolean.TRUE;
                }
            }

            // Update the histogram and the queue, removing the old
            // queue entry and then adding this next token if we
            // have hit the end of the road.
            if (lastTokensQueue.size() == maxDistance) {
                final String lastToken = lastTokensQueue.poll();
                seenMap.computeIfPresent(lastToken, (ignore, currentCount) -> {
                    if (currentCount > 1) {
                        return currentCount - 1;
                    } else {
                        // Gone to zero. Remove from map.
                        return null;
                    }
                });
            }
            lastTokensQueue.offer(nextToken);
        }

        return Boolean.FALSE;
    }

    @Nullable
    private static Boolean compareTextContainsAny(@Nonnull Iterator<? extends CharSequence> valueIterator, @Nonnull List<String> comparand) {
        final Set<String> comparandSet = getComparandSet(comparand);
        if (comparandSet.isEmpty()) {
            return null;
        }
        if (comparandSet.size() == 1) {
            return compareTextContainsSingle(valueIterator, comparandSet.iterator().next());
        }

        while (valueIterator.hasNext()) {
            final String nextToken = valueIterator.next().toString();
            if (!nextToken.isEmpty() && comparandSet.contains(nextToken)) {
                // Found a match. Return immediately.
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    @Nullable
    private static Boolean compareTextContainsAllPrefixes(@Nonnull Iterator<? extends CharSequence> valueIterator, @Nonnull List<String> comparand) {
        final Set<String> comparandSet = getComparandSet(comparand);
        if (comparandSet.isEmpty()) {
            return null;
        }
        if (comparandSet.size() == 1) {
            return compareTextContainsPrefix(valueIterator, comparandSet.iterator().next());
        }

        final Set<String> matchedSet = new HashSet<>((int)(comparandSet.size() * 1.5));
        while (valueIterator.hasNext()) {
            final String nextToken = valueIterator.next().toString();
            if (!nextToken.isEmpty()) {
                for (String comparandElement : comparandSet) {
                    if (nextToken.startsWith(comparandElement)) {
                        matchedSet.add(comparandElement);
                    }
                }

                if (matchedSet.size() == comparandSet.size()) {
                    // We've found as many as are in the comparand set, so we know the sets are the same.
                    return Boolean.TRUE;
                }
            }
        }
        return Boolean.FALSE;
    }

    @Nullable
    private static Boolean compareTextContainsAnyPrefix(@Nonnull Iterator<? extends CharSequence> valueIterator, @Nonnull List<String> comparand) {
        final Set<String> comparandSet = getComparandSet(comparand);
        if (comparandSet.isEmpty()) {
            return null;
        }
        if (comparandSet.size() == 1) {
            return compareTextContainsPrefix(valueIterator, comparandSet.iterator().next());
        }

        while (valueIterator.hasNext()) {
            final String nextToken = valueIterator.next().toString();
            if (!nextToken.isEmpty()) {
                for (String comparandElement : comparandSet) {
                    if (nextToken.startsWith(comparandElement)) {
                        // Found a match. Return immediately.
                        return Boolean.TRUE;
                    }
                }
            }
        }
        return Boolean.FALSE;
    }

    @Nullable
    private static Boolean compareTextContainsPhrase(@Nonnull Iterator<? extends CharSequence> valueIterator, @Nonnull List<String> comparand) {
        // Remove any leading or trailing stop words from the phrase search.
        int firstNonStopWord = 0;
        while (firstNonStopWord < comparand.size() && comparand.get(firstNonStopWord).isEmpty()) {
            firstNonStopWord++;
        }
        if (firstNonStopWord == comparand.size()) {
            // There were only stop words in the phrase. Cannot tell.
            return null;
        }
        int lastNonStopWord = comparand.size();
        while (lastNonStopWord > firstNonStopWord && comparand.get(lastNonStopWord - 1).isEmpty()) {
            lastNonStopWord--;
        }
        comparand = comparand.subList(firstNonStopWord, lastNonStopWord);

        // Handle trivial cases.
        if (comparand.isEmpty()) {
            return null;
        } else if (comparand.size() == 1) {
            return compareTextContainsSingle(valueIterator, comparand.get(0));
        }

        // Keep a queue of iterators, each one representing a position in
        // the comparand list.
        final Queue<Iterator<String>> positions = new ArrayDeque<>(comparand.size());
        final String firstComparand = comparand.get(0);

        while (valueIterator.hasNext()) {
            final String nextToken = valueIterator.next().toString();

            // Go through all current iterators through the comparand
            // in the queue and advance them. If they match, then the
            // phrase has been matched through that iterator, so
            // we should return
            int currPositionSize = positions.size();
            for (int i = 0; i < currPositionSize; i++) {
                final Iterator<String> comparandIterator = positions.poll();
                final String comparandToken = comparandIterator.next();
                // If comparand token is a stop word, then this will
                // match any token in the string during an index-based query,
                // so for result parity, this needs to advance the iterator
                // even if we know that it's not a stop word in the original
                // text.
                if (comparandToken.isEmpty() || comparandToken.equals(nextToken)) {
                    if (comparandIterator.hasNext()) {
                        positions.offer(comparandIterator);
                    } else {
                        return Boolean.TRUE;
                    }
                }
            }

            if (nextToken.equals(firstComparand)) {
                final Iterator<String> newIterator = comparand.iterator();
                // advance once to account for equaling the first comparand token
                newIterator.next();
                positions.offer(newIterator);
            }
        }

        return Boolean.FALSE;
    }

    /**
     * The type for a {@link Comparison} predicate.
     */
    public enum Type {
        EQUALS(true),
        NOT_EQUALS,
        LESS_THAN,
        LESS_THAN_OR_EQUALS,
        GREATER_THAN,
        GREATER_THAN_OR_EQUALS,
        STARTS_WITH,
        NOT_NULL(false, true),
        IS_NULL(true, true),
        IN,
        TEXT_CONTAINS_ALL(true),
        TEXT_CONTAINS_ALL_WITHIN(true),
        TEXT_CONTAINS_ANY(true),
        TEXT_CONTAINS_PHRASE(true),
        TEXT_CONTAINS_PREFIX,
        TEXT_CONTAINS_ALL_PREFIXES,
        TEXT_CONTAINS_ANY_PREFIX,
        @API(API.Status.EXPERIMENTAL)
        SORT(false);

        private final boolean isEquality;
        private final boolean isUnary;

        Type() {
            this(false);
        }

        Type(boolean isEquality) {
            this(isEquality, false);
        }

        Type(boolean isEquality, boolean isUnary) {
            this.isEquality = isEquality;
            this.isUnary = isUnary;
        }

        public boolean isEquality() {
            return isEquality;
        }

        public boolean isUnary() {
            return isUnary;
        }
    }

    @Nullable
    public static Type invertComparisonType(@Nonnull final Comparisons.Type type) {
        if (type.isUnary()) {
            return null;
        }
        switch (type) {
            case EQUALS:
                return Type.NOT_EQUALS;
            case LESS_THAN:
                return Type.GREATER_THAN_OR_EQUALS;
            case LESS_THAN_OR_EQUALS:
                return Type.GREATER_THAN;
            case GREATER_THAN:
                return Type.LESS_THAN_OR_EQUALS;
            case GREATER_THAN_OR_EQUALS:
                return Type.LESS_THAN;
            default:
                return null;
        }
    }

    @Nullable
    @SpotBugsSuppressWarnings("NP_BOOLEAN_RETURN_NULL")
    public static Boolean evalComparison(@Nonnull Type type, @Nullable Object value, @Nullable Object comparand) {
        if (value == null) {
            return null;
        }
        switch (type) {
            case STARTS_WITH:
                return compareStartsWith(value, comparand);
            case IN:
                return compareIn(value, comparand);
            case EQUALS:
                return compareEquals(value, comparand);
            case NOT_EQUALS:
                if (comparand == null) {
                    return null;
                }
                return !compareEquals(value, comparand);
            case LESS_THAN:
                return compare(value, comparand) < 0;
            case LESS_THAN_OR_EQUALS:
                return compare(value, comparand) <= 0;
            case GREATER_THAN:
                return compare(value, comparand) > 0;
            case GREATER_THAN_OR_EQUALS:
                return compare(value, comparand) >= 0;
            default:
                throw new RecordCoreException("Unsupported comparison type: " + type);
        }
    }

    @Nullable
    @SuppressWarnings("rawtypes")
    @SpotBugsSuppressWarnings("NP_BOOLEAN_RETURN_NULL")
    public static Boolean evalListComparison(@Nonnull Type type, @Nullable Object value, @Nullable List comparand) {
        if (value == null) {
            return null;
        }
        switch (type) {
            case EQUALS:
                return value.equals(comparand);
            case NOT_EQUALS:
                return !value.equals(comparand);
            case STARTS_WITH:
                return compareListStartsWith(value, comparand);
            case IN:
                return compareIn(value, comparand);
            default:
                throw new RecordCoreException("Only equals/not equals/starts with are supported for lists");
        }
    }

    /**
     * A comparison between a value associated with someplace in the record (such as a field) and a value associated
     * with the plan (such as a constant or a bound parameter).
     */
    public interface Comparison extends PlanHashable, QueryHashable, Correlated<Comparison> {
        /**
         * Evaluate this comparison for the value taken from the target record.
         * @param store the record store for the query
         * @param context the evaluation context for getting the other comparison value
         * @param value the value taken from the record
         * @return the tri-valued logic result of the comparison
         */
        @Nullable
        Boolean eval(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context, @Nullable Object value);

        /**
         * Validate that this comparison is compatible with a given record field.
         * @param descriptor the Protobuf descriptor for the proposed comparison field
         * @param fannedOut whether a repeated field fans out into multiple comparisons or is treated as a single list value
         */
        void validate(@Nonnull Descriptors.FieldDescriptor descriptor, boolean fannedOut);

        /**
         * Get the comparison type.
         * @return the comparison type
         */
        @Nonnull
        Type getType();

        @Nonnull
        Comparison withType(@Nonnull Type newType);

        /**
         * Get the comparison value without any bindings.
         * @return the value to be compared
         */
        @Nullable
        default Object getComparand() {
            return getComparand(null, null);
        }

        /**
         * Get the comparison value from the evaluation context.
         * @param store the record store for the query
         * @param context the context for query evaluation
         * @return the value to be compared
         */
        @Nullable
        Object getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context);

        /**
         * Get whether the comparison is with the result of a multi-column key.
         * If so, {@link #getComparand} will return a {@link com.apple.foundationdb.tuple.Tuple}.
         * @return {@code true} if the comparand is for multiple key columns
         */
        default boolean hasMultiColumnComparand() {
            return false;
        }

        /**
         * Get the printed representation of the comparison less the comparison operator itself.
         * @return the typeless string
         */
        @Nonnull
        String typelessString();

        @Nonnull
        default Comparison withParameterRelationshipMap(@Nonnull ParameterRelationshipGraph parameterRelationshipGraph) {
            return this;
        }

        @Nonnull
        @Override
        default Set<CorrelationIdentifier> getCorrelatedTo() {
            return ImmutableSet.of();
        }

        @Nonnull
        @Override
        default Comparison rebase(@Nonnull AliasMap translationMap) {
            return translateCorrelations(TranslationMap.rebaseWithAliasMap(translationMap));
        }

        @Nonnull
        Comparison translateCorrelations(@Nonnull TranslationMap translationMap);

        @Override
        default boolean semanticEquals(@Nullable Object other, @Nonnull AliasMap aliasMap) {
            return this.equals(other);
        }

        @Override
        default int semanticHashCode() {
            return hashCode();
        }
    }

    public static String toPrintable(@Nullable Object value) {
        if (value instanceof ByteString) {
            return toPrintable(((ByteString)value).toByteArray());
        } else if (value instanceof byte[]) {
            return ByteArrayUtil2.loggable((byte[])value);
        } else {
            return Objects.toString(value);
        }
    }

    /**
     * A comparison with a constant value.
     */
    public static class SimpleComparison implements Comparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Simple-Comparison");

        @Nonnull
        private final Type type;
        @Nonnull
        protected final Object comparand;

        public SimpleComparison(@Nonnull Type type, @Nonnull Object comparand) {
            this.type = type;
            this.comparand = comparand;
        }

        @Override
        public void validate(@Nonnull Descriptors.FieldDescriptor fieldDescriptor, boolean fannedOut) {
            if (!fannedOut && fieldDescriptor.isRepeated()) {
                throw new RecordCoreException("Scalar comparison on repeated field",
                        "fieldName", fieldDescriptor.getFullName(),
                        "comparandType", comparand.getClass());
            }
            if (!validForComparand(fieldDescriptor)) {
                throw new RecordCoreException("Comparison value of incorrect type",
                        "fieldName", fieldDescriptor.getFullName(),
                        "fieldType", fieldDescriptor.getJavaType(),
                        "comparandType", comparand.getClass());
            }
        }

        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        private boolean validForComparand(@Nonnull Descriptors.FieldDescriptor fieldDescriptor) {
            switch (fieldDescriptor.getJavaType()) {
                case BOOLEAN:
                    return comparand instanceof Boolean;
                case BYTE_STRING:
                    return comparand instanceof ByteString || comparand instanceof byte[];
                case DOUBLE:
                    return comparand instanceof Double;
                case FLOAT:
                    return comparand instanceof Float;
                case INT:
                    return comparand instanceof Integer;
                case LONG:
                    return comparand instanceof Long;
                case STRING:
                    return comparand instanceof String;
                case ENUM:
                    return comparand instanceof ProtocolMessageEnum &&
                          fieldDescriptor.getEnumType().equals(((ProtocolMessageEnum) comparand).getDescriptorForType());
                case MESSAGE:
                    final Descriptors.Descriptor descriptor = fieldDescriptor.getMessageType();
                    if (!TupleFieldsHelper.isTupleField(descriptor)) {
                        return false;
                    }
                    if (descriptor == TupleFieldsProto.UUID.getDescriptor()) {
                        return comparand instanceof UUID;
                    }
                    return validForComparand(descriptor.findFieldByName("value"));
                default:
                    return false;
            }
        }

        @Nonnull
        @Override
        public Object getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            return comparand;
        }

        @Nonnull
        @Override
        public Type getType() {
            return type;
        }

        @Nonnull
        @Override
        public Comparison withType(@Nonnull final Type newType) {
            if (type == newType) {
                return this;
            }
            return new SimpleComparison(newType, comparand);
        }

        @Nullable
        @Override
        public Boolean eval(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context, @Nullable Object value) {
            return evalComparison(type, value, getComparand(store, context));
        }

        @Nonnull
        @Override
        public String typelessString() {
            return toPrintable(comparand);
        }

        @Override
        public String toString() {
            return type + " " + typelessString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SimpleComparison that = (SimpleComparison) o;
            return type == that.type &&
                    Objects.equals(comparand, that.comparand);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, comparand);
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            switch (hashKind) {
                case LEGACY:
                    return type.name().hashCode() + PlanHashable.objectPlanHash(hashKind, comparand);
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, type, comparand);
                case STRUCTURAL_WITHOUT_LITERALS:
                    return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, type);
                default:
                    throw new UnsupportedOperationException("Hash Kind " + hashKind.name() + " is not supported");
            }
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            switch (hashKind) {
                case STRUCTURAL_WITH_LITERALS:
                    return HashUtils.queryHash(hashKind, BASE_HASH, type, comparand);
                case STRUCTURAL_WITHOUT_LITERALS:
                    return HashUtils.queryHash(hashKind, BASE_HASH, type);
                default:
                    throw new UnsupportedOperationException("Hash Kind " + hashKind.name() + " is not supported");
            }
        }

        @Nonnull
        @Override
        public Comparison translateCorrelations(@Nonnull final TranslationMap translationMap) {
            return this;
        }
    }

    /**
     * Exception thrown when comparison evaluation needs the query context, but none was supplied.
     */
    @SuppressWarnings("serial")
    public static class EvaluationContextRequiredException extends RecordCoreException {
        private static final Supplier<EvaluationContextRequiredException> INSTANCE_SUPPLIER =
                Suppliers.memoize(() -> new EvaluationContextRequiredException("unable to evaluate comparison without context and/or store"));

        private EvaluationContextRequiredException(String msg) {
            super(msg, null, false, false);
        }

        @Nonnull
        public static EvaluationContextRequiredException instance() {
            return INSTANCE_SUPPLIER.get();
        }
    }

    public static final Object COMPARISON_SKIPPED_BINDING = new Object() {
        @Override
        public String toString() {
            return "SKIP_COMPARISON";
        }
    };

    /**
     * A comparison against a parameter.
     */
    public interface ComparisonWithParameter extends Comparison {
        @Nonnull
        String getParameter();
    }

    /**
     * A comparison with a bound parameter, as opposed to a literal constant in the query.
     */
    public static class ParameterComparison implements ComparisonWithParameter {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Parameter-Comparison");

        @Nonnull
        private final Type type;
        @Nonnull
        protected final String parameter;
        @Nullable
        protected final Bindings.Internal internal;
        @Nonnull
        protected final ParameterRelationshipGraph parameterRelationshipGraph;
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier;

        public ParameterComparison(@Nonnull Type type,
                                   @Nonnull String parameter) {
            this(type, parameter, null, ParameterRelationshipGraph.unbound());
        }

        public ParameterComparison(@Nonnull Type type, @Nonnull String parameter, @Nullable Bindings.Internal internal) {
            this(type, parameter, internal, ParameterRelationshipGraph.unbound());
        }

        public ParameterComparison(@Nonnull Type type, @Nonnull String parameter, @Nullable Bindings.Internal internal, @Nonnull ParameterRelationshipGraph parameterRelationshipGraph) {
            checkInternalBinding(parameter, internal);
            this.type = type;
            this.parameter = parameter;
            this.internal = internal;
            if (type.isUnary()) {
                throw new RecordCoreException("Unary comparison type " + type + " cannot be bound to a parameter");
            }
            this.parameterRelationshipGraph = parameterRelationshipGraph;
            this.hashCodeSupplier = Suppliers.memoize(this::computeHashCode);
        }

        @Override
        public void validate(@Nonnull Descriptors.FieldDescriptor descriptor, boolean fannedOut) {
            // No additional validation.
        }

        @Nonnull
        @Override
        public Type getType() {
            return type;
        }

        @Nonnull
        @Override
        public Comparison withType(@Nonnull final Type newType) {
            if (type == newType) {
                return this;
            }
            return new ParameterComparison(newType, parameter, internal, parameterRelationshipGraph);
        }

        public boolean isCorrelation() {
            return internal == Bindings.Internal.CORRELATION;
        }

        public boolean isCorrelatedTo(@Nonnull final CorrelationIdentifier alias) {
            if (!isCorrelation()) {
                return false;
            }
            return Bindings.Internal.CORRELATION.identifier(getParameter()).equals(alias.getId());
        }

        @Nullable
        @Override
        public Object getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            if (context == null) {
                throw EvaluationContextRequiredException.instance();
            }
            if (isCorrelation()) {
                return Objects.requireNonNull(((QueryResult)context.getBinding(parameter))).getDatum();
            } else {
                return context.getBinding(parameter);
            }
        }

        @Nonnull
        @Override
        public Comparison translateCorrelations(@Nonnull final TranslationMap translationMap) {
            if (isCorrelation()) {
                final var aliasMap = translationMap.getAliasMapMaybe().orElseThrow(() -> new RecordCoreException("must have aliasMap"));
                final var alias = CorrelationIdentifier.of(Bindings.Internal.CORRELATION.identifier(parameter));
                final var translatedAlias = aliasMap.getTargetOrDefault(alias, alias);
                return new ParameterComparison(type,
                        Bindings.Internal.CORRELATION.bindingName(translatedAlias.getId()),
                        Bindings.Internal.CORRELATION,
                        parameterRelationshipGraph);
            } else {
                return this;
            }
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            if (!isCorrelation()) {
                return ImmutableSet.of();
            }
            return ImmutableSet.of(getAlias());
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            ParameterComparison that = (ParameterComparison) other;
            if (type != that.type) {
                return false;
            }

            //
            // Either this parameter is a proper correlation in which case the alias map needs to be consulted,
            // or, if it is a non-correlation like an extracted literal we need to consult the parameter relationship
            // graph.
            //
            if (isCorrelation() && that.isCorrelation()) {
                return aliasMap.containsMapping(getAlias(), that.getAlias());
            }

            if (!getParameter().equals(that.getParameter())) {
                return false;
            }
            
            return Objects.equals(relatedByEquality(), that.relatedByEquality());
        }

        @Nullable
        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Boolean eval(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context, @Nullable Object value) {
            // this is at evaluation time --> always use the context binding
            final Object comparand = getComparand(store, context);
            if (comparand == null) {
                return null;
            } else if (comparand == COMPARISON_SKIPPED_BINDING) {
                return Boolean.TRUE;
            } else {
                return evalComparison(type, value, comparand);
            }
        }

        @Nonnull
        @Override
        public String typelessString() {
            return "$" + parameter;
        }

        @Override
        public String toString() {
            return type + " " + typelessString();
        }

        @Nonnull
        @Override
        public String getParameter() {
            return parameter;
        }

        @Nonnull
        public CorrelationIdentifier getAlias() {
            if (!isCorrelation()) {
                throw new IllegalStateException("caller should check for type of binding before calling this method");
            }
            return CorrelationIdentifier.of(Bindings.Internal.CORRELATION.identifier(parameter));
        }

        @Override
        @SpotBugsSuppressWarnings("EQ_UNUSUAL")
        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        public boolean equals(Object o) {
            final AliasMap aliasMap;
            if (isCorrelation()) {
                final var alias = CorrelationIdentifier.of(Bindings.Internal.CORRELATION.identifier(parameter));
                aliasMap = AliasMap.identitiesFor(ImmutableSet.of(alias));
            } else {
                aliasMap = AliasMap.emptyMap();
            }
            return semanticEquals(o, aliasMap);
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        public int computeHashCode() {
            return Objects.hash(type, relatedByEquality());
        }

        private Set<String> relatedByEquality() {
            if (!parameterRelationshipGraph.isUnbound()) {
                if (parameterRelationshipGraph.containsParameter(parameter)) {
                    return parameterRelationshipGraph.getRelatedParameters(parameter, ParameterRelationshipGraph.RelationshipType.EQUALS);
                }
            }
            return ImmutableSet.of(getParameter());
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            switch (hashKind) {
                case LEGACY:
                    return type.name().hashCode() + (isCorrelation() ? 0 : parameter.hashCode());
                case FOR_CONTINUATION:
                case STRUCTURAL_WITHOUT_LITERALS:
                    if (isCorrelation()) {
                        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, type);
                    } else {
                        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, type, parameter);
                    }
                default:
                    throw new UnsupportedOperationException("Hash Kind " + hashKind.name() + " is not supported");
            }
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return HashUtils.queryHash(hashKind, BASE_HASH, type, parameter);
        }

        @Nonnull
        @Override
        public Comparison withParameterRelationshipMap(@Nonnull final ParameterRelationshipGraph parameterRelationshipGraph) {
            Verify.verify(this.parameterRelationshipGraph.isUnbound());
            return new ParameterComparison(type, parameter, internal, parameterRelationshipGraph);
        }

        @Nonnull
        private static String checkInternalBinding(@Nonnull String parameter, @Nullable Bindings.Internal internal) {
            if (internal == null && Bindings.Internal.isInternal(parameter)) {
                throw new RecordCoreException(
                        "Parameter is internal, parameters cannot start with \"" + Bindings.Internal.PREFIX + "\"");
            }
            return parameter;
        }
    }

    /**
     * A comparison with a bound parameter, as opposed to a literal constant in the query.
     */
    public static class ValueComparison implements Comparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Value-Comparison");
        @Nonnull
        private final Type type;
        @Nonnull
        private final Value comparandValue;
        @Nonnull
        protected final ParameterRelationshipGraph parameterRelationshipGraph;
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier;

        public ValueComparison(@Nonnull final Type type,
                               @Nonnull final Value comparandValue) {
            this(type, comparandValue, ParameterRelationshipGraph.unbound());
        }

        public ValueComparison(@Nonnull final Type type,
                               @Nonnull final Value comparandValue,
                               @Nonnull final ParameterRelationshipGraph parameterRelationshipGraph) {
            this.type = type;
            this.comparandValue = comparandValue;
            if (type.isUnary()) {
                throw new RecordCoreException("Unary comparison type " + type + " cannot be bound to a value");
            }
            this.parameterRelationshipGraph = parameterRelationshipGraph;
            this.hashCodeSupplier = Suppliers.memoize(this::computeHashCode);
        }

        @Override
        public void validate(@Nonnull Descriptors.FieldDescriptor descriptor, boolean fannedOut) {
            // No additional validation.
        }

        @Nonnull
        @Override
        public Type getType() {
            return type;
        }

        @Nonnull
        @Override
        public Comparison withType(@Nonnull final Type newType) {
            if (type == newType) {
                return this;
            }
            return new ValueComparison(newType, comparandValue, parameterRelationshipGraph);
        }

        @Nonnull
        public Value getComparandValue() {
            return comparandValue;
        }

        @Nullable
        @Override
        public Object getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            if (context == null) {
                throw EvaluationContextRequiredException.instance();
            }
            return comparandValue.eval(store, context);
        }

        @Nonnull
        @Override
        public Comparison translateCorrelations(@Nonnull final TranslationMap translationMap) {
            if (comparandValue.getCorrelatedTo()
                    .stream()
                    .noneMatch(translationMap::containsSourceAlias)) {
                return this;
            }

            return new ValueComparison(type, comparandValue.translateCorrelations(translationMap), parameterRelationshipGraph);
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            return comparandValue.getCorrelatedTo();
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final var that = (ValueComparison) other;
            if (type != that.type) {
                return false;
            }

            return comparandValue.semanticEquals(that.comparandValue, aliasMap);
        }

        @Nullable
        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Boolean eval(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context, @Nullable Object v) {
            // this is at evaluation time --> always use the context binding
            final Object comparand = getComparand(store, context);
            if (comparand == null) {
                return null;
            } else if (comparand == COMPARISON_SKIPPED_BINDING) {
                return Boolean.TRUE;
            } else {
                return evalComparison(type, v, comparand);
            }
        }

        @Nonnull
        @Override
        public String typelessString() {
            return comparandValue.toString();
        }

        @Override
        public String toString() {
            return type + " " + typelessString();
        }

        @Override
        @SpotBugsSuppressWarnings("EQ_UNUSUAL")
        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        public boolean equals(Object o) {
            return semanticEquals(o, AliasMap.identitiesFor(comparandValue.getCorrelatedTo()));
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        public int computeHashCode() {
            return Objects.hash(type, relatedByEquality());
        }

        private Set<String> relatedByEquality() {
            return ImmutableSet.of();
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            switch (hashKind) {
                case LEGACY:
                case FOR_CONTINUATION:
                case STRUCTURAL_WITHOUT_LITERALS:
                    return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, type);
                default:
                    throw new UnsupportedOperationException("Hash Kind " + hashKind.name() + " is not supported");
            }
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return HashUtils.queryHash(hashKind, BASE_HASH, type);
        }

        @Nonnull
        @Override
        public Comparison withParameterRelationshipMap(@Nonnull final ParameterRelationshipGraph parameterRelationshipGraph) {
            Verify.verify(this.parameterRelationshipGraph.isUnbound());
            return new ValueComparison(type, comparandValue, parameterRelationshipGraph);
        }
    }

    /**
     * A comparison with a list of values.
     */
    public static class ListComparison implements Comparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("List-Comparison");

        @Nonnull
        private final Type type;
        @Nonnull
        @SuppressWarnings("rawtypes")
        private final List comparand;
        @Nullable
        private final Descriptors.FieldDescriptor.JavaType javaType;

        @SuppressWarnings({"rawtypes", "unchecked"})
        public ListComparison(@Nonnull Type type, @Nonnull List comparand) {
            this.type = type;
            switch (this.type) {
                case EQUALS:
                case NOT_EQUALS:
                case STARTS_WITH:
                case IN:
                    break;
                default:
                    throw new RecordCoreException("ListComparison only supports EQUALS, NOT_EQUALS, STARTS_WITH and IN");
            }
            if (comparand == null || (this.type == Type.IN && comparand.stream().anyMatch(o -> o == null))) {
                throw new NullPointerException("List comparand is null, or contains null");
            }
            if (comparand.isEmpty()) {
                javaType = null;
            } else {
                javaType = getJavaType(comparand.get(0));
                for (Object o : comparand) {
                    if (getJavaType(o) != javaType) {
                        throw new RecordCoreException("all comparand values must have the same type, first was " +
                                javaType + " found another of type " + getJavaType(o));
                    }
                }
            }
            this.comparand = comparand;
        }

        private static Descriptors.FieldDescriptor.JavaType getJavaType(@Nonnull Object o) {
            if (o instanceof Boolean) {
                return JavaType.BOOLEAN;
            } else if (o instanceof ByteString || o instanceof byte[]) {
                return JavaType.BYTE_STRING;
            } else if (o instanceof Double) {
                return JavaType.DOUBLE;
            } else if (o instanceof Float) {
                return JavaType.FLOAT;
            } else if (o instanceof Long) {
                return JavaType.LONG;
            } else if (o instanceof Integer) {
                return JavaType.INT;
            } else if (o instanceof String) {
                return JavaType.STRING;
            } else if (o instanceof Internal.EnumLite) {
                return JavaType.ENUM;
            } else {
                throw new RecordCoreException(o.getClass() + " is an invalid type for a comparand");
            }
        }

        @Override
        public void validate(@Nonnull Descriptors.FieldDescriptor fieldDescriptor, boolean fannedOut) {
            if (type.equals(Type.IN)) {
                if (!fannedOut && fieldDescriptor.isRepeated()) {
                    throw new RecordCoreException("In comparison with non-scalar field " + fieldDescriptor.getName());
                }
            } else {
                if (!fieldDescriptor.isRepeated() || fannedOut) {
                    throw new RecordCoreException("Invalid list comparison on scalar field " + fieldDescriptor.getName());
                }
            }
            if (javaType != null && javaType != fieldDescriptor.getJavaType()) {
                throw new RecordCoreException("Value " + comparand +
                        " not of correct type for " + fieldDescriptor.getFullName());
            }
        }

        @Nonnull
        @Override
        @SuppressWarnings("rawtypes")
        public List getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            return comparand;
        }

        @Nonnull
        @Override
        public Comparison translateCorrelations(@Nonnull final TranslationMap translationMap) {
            return this;
        }

        @Nonnull
        @Override
        public Type getType() {
            return type;
        }

        @Nonnull
        @Override
        public Comparison withType(@Nonnull final Type newType) {
            if (type == newType) {
                return this;
            }
            return new ListComparison(newType, comparand);
        }

        @Nullable
        @Override
        public Boolean eval(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context, @Nullable Object value) {
            return evalListComparison(type, value, getComparand(store, context));
        }

        @Nonnull
        @Override
        public String typelessString() {
            return comparand.toString();
        }

        @Override
        public String toString() {
            return type + " " + typelessString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ListComparison that = (ListComparison) o;
            return type == that.type &&
                    Objects.equals(comparand, that.comparand) &&
                    javaType == that.javaType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, comparand, javaType);
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            switch (hashKind) {
                case LEGACY:
                    return type.name().hashCode() + PlanHashable.iterablePlanHash(hashKind, comparand) + PlanHashable.objectPlanHash(hashKind, javaType);
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, type, comparand, javaType);
                case STRUCTURAL_WITHOUT_LITERALS:
                    return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, type, javaType);
                default:
                    throw new UnsupportedOperationException("Hash Kind " + hashKind.name() + " is not supported");
            }
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            switch (hashKind) {
                case STRUCTURAL_WITH_LITERALS:
                    return HashUtils.queryHash(hashKind, BASE_HASH, type, comparand, javaType);
                case STRUCTURAL_WITHOUT_LITERALS:
                    // Query hash without literals ignores comparand.
                    return HashUtils.queryHash(hashKind, BASE_HASH, type, javaType);
                default :
                    throw new UnsupportedOperationException("Hash Kind " + hashKind.name() + " is not supported");
            }
        }
    }

    /**
     * A unary predicate for special nullity checks, such as {@code NULL} and {@code NOT NULL}.
     */
    public static class NullComparison implements Comparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Null-Comparison");

        @Nonnull
        private final Type type;

        public NullComparison(@Nonnull Type type) {
            this.type = type;
        }

        @Nullable
        @Override
        public Boolean eval(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context, @Nullable Object value) {
            if (type == Type.IS_NULL) {
                return value == null;
            } else {
                return value != null;
            }
        }

        @Override
        public void validate(@Nonnull Descriptors.FieldDescriptor descriptor, boolean fannedOut) {
            if (!fannedOut && descriptor.isRepeated()) {
                throw new RecordCoreException("Nullability comparison on repeated field " + descriptor.getName());
            }
        }

        @Nonnull
        @Override
        public Type getType() {
            return type;
        }

        @Nonnull
        @Override
        public Comparison withType(@Nonnull final Type newType) {
            if (type == newType) {
                return this;
            }
            return new NullComparison(newType);
        }

        @Nullable
        @Override
        public Object getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            // Requires special handling in TupleRange.
            return null;
        }

        @Nonnull
        @Override
        public Comparison translateCorrelations(@Nonnull final TranslationMap translationMap) {
            return this;
        }

        @Nonnull
        @Override
        public String typelessString() {
            return "NULL";
        }

        @Override
        public String toString() {
            return type.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NullComparison that = (NullComparison) o;
            return type == that.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type);
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            switch (hashKind) {
                case LEGACY:
                    return type.name().hashCode();
                case FOR_CONTINUATION:
                case STRUCTURAL_WITHOUT_LITERALS:
                    return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, type);
                default:
                    throw new UnsupportedOperationException("Hash Kind " + hashKind.name() + " is not supported");
            }
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return HashUtils.queryHash(hashKind, BASE_HASH, type);
        }
    }

    /**
     * A predicate for comparisons to things unknown or opaque to the planner. We only know it is equal to some value.
     */
    public static class OpaqueEqualityComparison implements Comparison {
        @Nullable
        @Override
        public Boolean eval(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context, @Nullable Object value) {
            return false;
        }

        @Override
        public void validate(@Nonnull final Descriptors.FieldDescriptor descriptor, final boolean fannedOut) {
            throw new UnsupportedOperationException("comparison should not be used in a plan");
        }

        @Nonnull
        @Override
        public Type getType() {
            return Type.EQUALS;
        }

        @Nonnull
        @Override
        public Comparison withType(@Nonnull final Type newType) {
            return this;
        }

        @Nullable
        @Override
        public Object getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            return null;
        }

        @Nonnull
        @Override
        public Comparison translateCorrelations(@Nonnull final TranslationMap translationMap) {
            return this;
        }

        @Nonnull
        @Override
        public String typelessString() {
            return ":?:";
        }

        @Override
        public String toString() {
            return Type.EQUALS + " " + typelessString();
        }

        @Override
        public boolean equals(Object o) {
            // same as standard object implementation
            return this == o;
        }

        @Override
        public int hashCode() {
            // same as standard object implementation
            return System.identityHashCode(this);
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            throw new UnsupportedOperationException("Hash Kind " + hashKind.name() + " is not supported");
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            throw new UnsupportedOperationException("Hash Kind " + hashKind.name() + " is not supported");
        }
    }

    /**
     * A text-style comparison, such as containing a given set of tokens.
     */
    public static class TextComparison implements Comparison {
        private static final TextTokenizerRegistry TOKENIZER_REGISTRY = TextTokenizerRegistryImpl.instance();
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Text-Comparison");

        @Nonnull
        private final Type type;
        @Nullable
        private final List<String> tokenList;
        @Nullable
        private final String tokenStr;
        @Nullable
        private final String tokenizerName;
        @Nonnull
        private final String fallbackTokenizerName;

        public TextComparison(@Nonnull Type type, @Nonnull String tokens, @Nullable String tokenizerName, @Nonnull String fallbackTokenizerName) {
            this.type = type;
            this.tokenList = null;
            this.tokenStr = tokens;
            this.tokenizerName = tokenizerName;
            this.fallbackTokenizerName = fallbackTokenizerName;
        }

        public TextComparison(@Nonnull Type type, @Nonnull List<String> tokens, @Nullable String tokenizerName, @Nonnull String fallbackTokenizerName) {
            this.type = type;
            this.tokenList = tokens;
            this.tokenStr = null;
            this.tokenizerName = tokenizerName;
            this.fallbackTokenizerName = fallbackTokenizerName;
        }

        @Nonnull
        @Override
        public Comparison translateCorrelations(@Nonnull final TranslationMap translationMap) {
            return this;
        }

        @Nonnull
        private Iterator<? extends CharSequence> tokenize(@Nonnull String text, @Nonnull TextTokenizer.TokenizerMode tokenizerMode) {
            final TextTokenizer tokenizer = TOKENIZER_REGISTRY.getTokenizer(tokenizerName == null ? fallbackTokenizerName : tokenizerName);
            return tokenizer.tokenize(text, tokenizer.getMaxVersion(), tokenizerMode);
        }

        @Nullable
        Boolean evalComparison(@Nonnull Iterator<? extends CharSequence> textIterator, @Nonnull List<String> comparand) {
            switch (type) {
                case TEXT_CONTAINS_ALL:
                    return compareTextContainsAll(textIterator, comparand);
                case TEXT_CONTAINS_ANY:
                    return compareTextContainsAny(textIterator, comparand);
                case TEXT_CONTAINS_PHRASE:
                    return compareTextContainsPhrase(textIterator, comparand);
                case TEXT_CONTAINS_PREFIX:
                    if (comparand.size() != 1) {
                        throw new RecordCoreArgumentException("Cannot evaluate prefix comparison with multiple tokens");
                    }
                    return compareTextContainsPrefix(textIterator, comparand.get(0));
                case TEXT_CONTAINS_ANY_PREFIX:
                    return compareTextContainsAnyPrefix(textIterator, comparand);
                case TEXT_CONTAINS_ALL_PREFIXES:
                    return compareTextContainsAllPrefixes(textIterator, comparand);
                default:
                    throw new RecordCoreException("Cannot evaluate text comparison of type: " + type);
            }
        }

        @Nullable
        @Override
        public Boolean eval(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context, @Nullable Object value) {
            if (value == null) {
                return null;
            }
            if (!(value instanceof String)) {
                throw new RecordCoreArgumentException("Text comparison applied against non-string value")
                        .addLogInfo(LogMessageKeys.COMPARISON_VALUE, value);
            }
            final List<String> comparandTokens = getComparandTokens();
            if (comparandTokens == null) {
                return null;
            }
            final String text = (String) value;
            final Iterator<? extends CharSequence> textIterator = tokenize(text, TextTokenizer.TokenizerMode.INDEX);
            return evalComparison(textIterator, comparandTokens);
        }

        @Override
        public void validate(@Nonnull Descriptors.FieldDescriptor descriptor, boolean fannedOut) {
            if (descriptor.getType() != Descriptors.FieldDescriptor.Type.STRING) {
                throw new RecordCoreException("Text comparison on non-string field");
            } else if (!fannedOut && descriptor.isRepeated()) {
                throw new RecordCoreException("Text comparison on repeated field without fan out");
            }
        }

        @Nullable
        public String getTokenizerName() {
            return tokenizerName;
        }

        @Nonnull
        public String getFallbackTokenizerName() {
            return fallbackTokenizerName;
        }

        @Nonnull
        @Override
        public Type getType() {
            return type;
        }

        @Nonnull
        @Override
        public Comparison withType(@Nonnull final Type newType) {
            if (type == newType) {
                return this;
            }
            if (tokenList == null) {
                return new TextComparison(newType, Objects.requireNonNull(tokenStr), tokenizerName, fallbackTokenizerName);
            } else {
                return new TextComparison(newType, tokenList, tokenizerName, fallbackTokenizerName);
            }
        }

        @Nullable
        private List<String> getComparandTokens() {
            if (tokenList != null) {
                return tokenList;
            } else if (tokenStr != null) {
                return Lists.newArrayList(Iterators.transform(tokenize(tokenStr, TextTokenizer.TokenizerMode.QUERY), CharSequence::toString));
            } else {
                return null;
            }
        }

        @Nullable
        @Override
        public Object getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            if (tokenList != null) {
                return tokenList;
            } else {
                return tokenStr;
            }
        }

        @Nonnull
        @Override
        public String typelessString() {
            final Object comparand = getComparand(null, EvaluationContext.EMPTY);
            if (comparand == null) {
                return "null";
            } else {
                return comparand.toString();
            }
        }

        @Nonnull
        @Override
        public String toString() {
            return type.name() + " " + typelessString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TextComparison that = (TextComparison) o;
            return type == that.type &&
                Objects.equals(getComparand(), that.getComparand()) &&
                   Objects.equals(tokenizerName, that.tokenizerName) &&
                   Objects.equals(fallbackTokenizerName, that.fallbackTokenizerName);
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            switch (hashKind) {
                case LEGACY:
                    return PlanHashable.objectsPlanHash(hashKind, type, getComparand(), tokenizerName, fallbackTokenizerName);
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, type, getComparand(), tokenizerName, fallbackTokenizerName);
                case STRUCTURAL_WITHOUT_LITERALS:
                    return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, type, tokenizerName, fallbackTokenizerName);
                default:
                    throw new UnsupportedOperationException("Hash Kind " + hashKind.name() + " is not supported");
            }
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            switch (hashKind) {
                case STRUCTURAL_WITH_LITERALS:
                    return HashUtils.queryHash(hashKind, BASE_HASH, type, getComparand(), tokenizerName, fallbackTokenizerName);
                case STRUCTURAL_WITHOUT_LITERALS:
                    return HashUtils.queryHash(hashKind, BASE_HASH, type, tokenizerName, fallbackTokenizerName);
                default:
                    throw new UnsupportedOperationException("Hash Kind " + hashKind.name() + " is not supported");
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(type.name(), getComparand(), tokenizerName, fallbackTokenizerName);
        }
    }

    /**
     * A {@link TextComparison} that must be satisfied within a certain number of text tokens.
     */
    public static class TextWithMaxDistanceComparison extends TextComparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Text-With-Max-Distance-Comparison");

        private final int maxDistance;

        public TextWithMaxDistanceComparison(@Nonnull String tokens, int maxDistance, @Nullable String tokenizerName, @Nonnull String fallbackTokenizerName) {
            super(Type.TEXT_CONTAINS_ALL_WITHIN, tokens, tokenizerName, fallbackTokenizerName);
            this.maxDistance = maxDistance;
        }

        public TextWithMaxDistanceComparison(@Nonnull List<String> tokens, int maxDistance, @Nullable String tokenizerName, @Nonnull String fallbackTokenizerName) {
            super(Type.TEXT_CONTAINS_ALL_WITHIN, tokens, tokenizerName, fallbackTokenizerName);
            this.maxDistance = maxDistance;
        }

        @Override
        Boolean evalComparison(@Nonnull Iterator<? extends CharSequence> textIterator, @Nonnull List<String> comparand) {
            if (getType() != Type.TEXT_CONTAINS_ALL_WITHIN) {
                throw new RecordCoreException("Cannot evaluate text comparison of type: " + getType());
            }
            return compareTextContainsAllWithin(textIterator, comparand, maxDistance);
        }

        /**
         * Get the maximum distance allowed between tokens in the source document allowed by
         * this filter.
         * @return the maximum distance allowed between tokens by this filter
         */
        public int getMaxDistance() {
            return maxDistance;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TextWithMaxDistanceComparison that = (TextWithMaxDistanceComparison) o;
            return super.equals(that) && maxDistance == that.maxDistance;
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            switch (hashKind) {
                case LEGACY:
                    return super.planHash(hashKind) * 31 + maxDistance;
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, super.planHash(hashKind), maxDistance);
                case STRUCTURAL_WITHOUT_LITERALS:
                    return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, super.planHash(hashKind));
                default:
                    throw new UnsupportedOperationException("Hash kind " + hashKind + " is not supported");
            }
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            switch (hashKind) {
                case STRUCTURAL_WITH_LITERALS:
                    return HashUtils.queryHash(hashKind, BASE_HASH, super.queryHash(hashKind), maxDistance);
                case STRUCTURAL_WITHOUT_LITERALS:
                    return HashUtils.queryHash(hashKind, BASE_HASH, super.queryHash(hashKind));
                default:
                    throw new UnsupportedOperationException("Hash kind " + hashKind + " is not supported");
            }
        }

        @Override
        public int hashCode() {
            return super.hashCode() * 31 + maxDistance;
        }

        @Nonnull
        @Override
        public String toString() {
            return String.format("%s(%d) %s", getType().name(), maxDistance, typelessString());
        }
    }

    /**
     * A {@link TextComparison} that checks for all prefixes. It carries additional meta-data about whether the
     * comparison is "strict" or not, i.e., whether it is allowed to return false positives.
     */
    @API(API.Status.EXPERIMENTAL)
    public static class TextContainsAllPrefixesComparison extends TextComparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Text-Contains-All-Prefixes-Comparison");

        private final boolean strict;
        private final long expectedRecords;
        private final double falsePositivePercentage;

        public TextContainsAllPrefixesComparison(@Nonnull String tokenPrefixes, boolean strict, @Nullable String tokenizerName, @Nonnull String fallbackTokenizerName) {
            this(tokenPrefixes, strict, ProbableIntersectionCursor.DEFAULT_EXPECTED_RESULTS, ProbableIntersectionCursor.DEFAULT_FALSE_POSITIVE_PERCENTAGE, tokenizerName, fallbackTokenizerName);
        }

        public TextContainsAllPrefixesComparison(@Nonnull String tokenPrefixes, boolean strict, long expectedRecords, double falsePositivePercentage,
                                                 @Nullable String tokenizerName, @Nonnull String fallbackTokenizerName) {
            super(Type.TEXT_CONTAINS_ALL_PREFIXES, tokenPrefixes, tokenizerName, fallbackTokenizerName);
            this.strict = strict;
            this.expectedRecords = expectedRecords;
            this.falsePositivePercentage = falsePositivePercentage;
        }

        public TextContainsAllPrefixesComparison(@Nonnull List<String> tokenPrefixes, boolean strict, @Nullable String tokenizerName, @Nonnull String fallbackTokenizerName) {
            this(tokenPrefixes, strict, ProbableIntersectionCursor.DEFAULT_EXPECTED_RESULTS, ProbableIntersectionCursor.DEFAULT_FALSE_POSITIVE_PERCENTAGE,
                    tokenizerName, fallbackTokenizerName);
        }

        public TextContainsAllPrefixesComparison(@Nonnull List<String> tokenPrefixes, boolean strict, long expectedRecords, double falsePositivePercentage,
                                                 @Nullable String tokenizerName, @Nonnull String fallbackTokenizerName) {
            super(Type.TEXT_CONTAINS_ALL_PREFIXES, tokenPrefixes, tokenizerName, fallbackTokenizerName);
            this.strict = strict;
            this.expectedRecords = expectedRecords;
            this.falsePositivePercentage = falsePositivePercentage;
        }

        /**
         * Whether this comparison should be strictly evaluated. This is used during query planning
         * to determine whether it is acceptable to return false positives.
         *
         * @return {@code false} if false positives are acceptable and {@code true} otherwise
         */
        public boolean isStrict() {
            return strict;
        }

        /**
         * Get the expected number of records for each token of this predicate. This tweaks the behavior of this
         * predicate when run against an index scan. In particular, this informs how much memory to use for internal
         * data structures as part of the scan. If the number provided is larger than the number that actually gets
         * read, then the scan is less memory efficient. If the number provided is smaller than the number that
         * actually gets read, then the scan may have more false positives than expected.
         *
         * @return the expected number of insertions per child of
         */
        public long getExpectedRecords() {
            return expectedRecords;
        }

        /**
         * Get the configured false positive percentage for each token scan of this predicate. This is used, along
         * with {@link #getExpectedRecords()}, determines the size of internal data structures used as part of the
         * scan. In general, the lower this number, the more memory is used. This number refers to the false positive
         * percentage of determining if an <i>individual</i> prefix is in the indexed text field of a record while
         * scanning.
         *
         * @return the rate of false positives used by probabilistic data structures
         */
        public double getFalsePositivePercentage() {
            return falsePositivePercentage;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TextContainsAllPrefixesComparison that = (TextContainsAllPrefixesComparison) o;
            return super.equals(that) && strict == that.strict;
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            switch (hashKind) {
                case LEGACY:
                    return super.planHash(hashKind) * (strict ? -1 : 1);
                case FOR_CONTINUATION:
                case STRUCTURAL_WITHOUT_LITERALS:
                    return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, super.planHash(hashKind), strict);
                default:
                    throw new UnsupportedOperationException("Hash kind " + hashKind + " is not supported");
            }
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return HashUtils.queryHash(hashKind, BASE_HASH, super.queryHash(hashKind), strict);
        }

        @Override
        public int hashCode() {
            return super.hashCode() * (strict ? -1 : 1);
        }

        @Nonnull
        @Override
        public String toString() {
            return String.format("%s(%s) %s", getType().name(), strict ? "strictly" : "approximately", typelessString());
        }
    }

    /**
     * Comparison wrapping another one and answering {@code true} to {@link #hasMultiColumnComparand}.
     */
    public static class MultiColumnComparison implements Comparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Multi-Column-Comparison");

        @Nonnull
        private final Comparison inner;

        public MultiColumnComparison(@Nonnull final Comparison inner) {
            this.inner = inner;
        }

        @Nullable
        @Override
        public Boolean eval(@Nonnull final FDBRecordStoreBase<?> store, @Nonnull final EvaluationContext context, @Nullable final Object value) {
            return inner.eval(store, context, value);
        }

        @Override
        public void validate(@Nonnull final Descriptors.FieldDescriptor descriptor, final boolean fannedOut) {
            inner.validate(descriptor, fannedOut);
        }

        @Nonnull
        @Override
        public Type getType() {
            return inner.getType();
        }

        @Nonnull
        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Comparison withType(@Nonnull final Type newType) {
            final var newInner = inner.withType(newType);
            if (newInner == inner) {
                return this;
            }
            return new MultiColumnComparison(newInner);
        }

        @Nonnull
        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Comparison translateCorrelations(@Nonnull final TranslationMap translationMap) {
            final var translatedInner = inner.translateCorrelations(translationMap);
            if (inner == translatedInner) {
                return this;
            } else {
                return new MultiColumnComparison(translatedInner);
            }
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            return inner.getCorrelatedTo();
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            MultiColumnComparison that = (MultiColumnComparison) other;
            return this.inner.semanticEquals(that.inner, aliasMap);
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            switch (hashKind) {
                case LEGACY:
                    return inner.planHash(hashKind);
                case FOR_CONTINUATION:
                case STRUCTURAL_WITHOUT_LITERALS:
                    return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, inner);
                default:
                    throw new UnsupportedOperationException("Hash kind " + hashKind + " is not supported");
            }
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return HashUtils.queryHash(hashKind, BASE_HASH, inner);
        }

        @Nullable
        @Override
        public Object getComparand() {
            return inner.getComparand();
        }

        @Nullable
        @Override
        public Object getComparand(@Nullable final FDBRecordStoreBase<?> store, @Nullable final EvaluationContext context) {
            return inner.getComparand(store, context);
        }

        @Override
        public boolean hasMultiColumnComparand() {
            return true;
        }

        @Nonnull
        @Override
        public String typelessString() {
            return inner.typelessString();
        }

        @Override
        public int hashCode() {
            return inner.hashCode();
        }

        @Override
        @SpotBugsSuppressWarnings("EQ_UNUSUAL")
        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        public boolean equals(final Object o) {
            return semanticEquals(o, AliasMap.identitiesFor(inner.getCorrelatedTo()));
        }

        @Override
        public String toString() {
            return inner.toString();
        }
    }

    /**
     * Comparison that is built on applying function's inverse to the comparand of a different comparison.
     * This is to support certain algebraic operations on queries. For example, if a query contains a
     * clause like {@code f(x) = $val} for some column {@code x} and some parameter {@code val}, then
     * this comparison can be used to transform the predicate into {@code x = f^-1($val)}, which can
     * be easier to evaluate. Note also that some functions may not be injective, that is, there may be
     * multiple inputs that all map to the same output. For that reason, the predicate {@code f(x) = $val}
     * may sometimes get transformed into {@code x IN f^-1($val)}.
     *
     * <p>
     * In most cases, users should not construct this comparison on their own, but
     * some planner operations may create this in internal structures.
     * </p>
     */
    @API(API.Status.INTERNAL)
    public static class InvertedFunctionComparison implements Comparison {
        @Nonnull
        private final InvertibleFunctionKeyExpression function;
        @Nonnull
        private final Comparison originalComparison;
        @Nonnull
        private final Type type;

        private InvertedFunctionComparison(@Nonnull InvertibleFunctionKeyExpression function,
                                           @Nonnull Comparison originalComparison,
                                           @Nonnull Type type) {
            this.function = function;
            this.originalComparison = originalComparison;
            this.type = type;
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return PlanHashable.planHash(hashKind, function, originalComparison);
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return HashUtils.queryHash(hashKind, function, originalComparison);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final InvertedFunctionComparison that = (InvertedFunctionComparison)o;
            return Objects.equals(function, that.function) && Objects.equals(originalComparison, that.originalComparison);
        }

        @Override
        public int hashCode() {
            return Objects.hash(function, originalComparison);
        }

        @Nullable
        @Override
        public Boolean eval(@Nonnull final FDBRecordStoreBase<?> store, @Nonnull final EvaluationContext context, @Nullable final Object value) {
            Object comparand = getComparand(store, context);
            return evalComparison(type, value, comparand);
        }

        @Override
        public void validate(@Nonnull final Descriptors.FieldDescriptor descriptor, final boolean fannedOut) {
            originalComparison.validate(descriptor, fannedOut);
        }

        @Nonnull
        @Override
        public Type getType() {
            return type;
        }

        @Nonnull
        @Override
        public Comparison withType(@Nonnull final Type newType) {
            return from(function, originalComparison.withType(newType));
        }

        @Nullable
        @Override
        public Object getComparand(@Nullable final FDBRecordStoreBase<?> store, @Nullable final EvaluationContext context) {
            Object originalComparandValue = originalComparison.getComparand(store, context);
            if (originalComparison.getType() == Type.IN) {
                if (!(originalComparandValue instanceof List<?>)) {
                    throw new RecordCoreException("cannot evaluate IN comparison on non-list type");
                }
                List<?> underlyingList = (List<?>) originalComparandValue;
                List<Object> finalValues = new ArrayList<>(underlyingList.size());
                for (Object obj : underlyingList) {
                    Key.Evaluated evaluated = Key.Evaluated.scalar(obj);
                    List<Key.Evaluated> inverse = function.evaluateInverse(evaluated);
                    inverse.stream()
                            .map(this::getSingletonPreImage)
                            .forEach(finalValues::add);
                }
                return finalValues;
            } else {
                Key.Evaluated evaluated = Key.Evaluated.scalar(originalComparandValue);
                List<Key.Evaluated> inverse = function.evaluateInverse(evaluated);
                if (getType() == Type.IN) {
                    return inverse.stream()
                            .map(this::getSingletonPreImage)
                            .collect(Collectors.toList());
                } else {
                    Key.Evaluated preImage = inverse.get(0);
                    return getSingletonPreImage(preImage);
                }
            }
        }

        private Object getSingletonPreImage(Key.Evaluated preImage) {
            if (preImage.size() != 1) {
                throw new RecordCoreException("unable to get singleton pre-image for function")
                        .addLogInfo(LogMessageKeys.FUNCTION, function.getName());
            }
            return preImage.getObject(0);
        }

        @Nonnull
        @Override
        public String typelessString() {
            return function.getName() + "^-1(" + originalComparison.typelessString() + ")";
        }

        @Override
        public String toString() {
            return getType() + " " + typelessString();
        }

        @Nonnull
        @Override
        @SuppressWarnings({"PMD.CompareObjectsWithEquals"}) // used here for referential equality
        public Comparison translateCorrelations(@Nonnull final TranslationMap translationMap) {
            Comparison translated = originalComparison.translateCorrelations(translationMap);
            if (translated == originalComparison) {
                return this;
            } else {
                return new InvertedFunctionComparison(function, translated, type);
            }
        }

        /**
         * Create an inverted function comparison from an invertible function and a pre-existing comparison.
         * This will create a new comparison that evaluates the inverse of the given function against the
         * original comparison's comparand. So, for example, if the original comparison is {@code = 2} and the function
         * is the exponential function, this will produce a comparison that is equivalent to {@code = log(2)}.
         *
         * <p>
         * This comparison currently has the following limitations:
         * </p>
         *
         * <ul>
         *     <li>The function must be a unary function (i.e., it must take single-column inputs and produce
         *          single-column outputs.)</li>
         *     <li>The comparison must be of type {@link Type#EQUALS EQUALS} or {@link Type#IN IN}.</li>
         * </ul>
         *
         * @param function a unary invertible function key expression
         * @param originalComparison a comparison
         * @return a new comparison that applies the inverse of the given function to the comparand of the
         *     original comparison
         */
        public static InvertedFunctionComparison from(@Nonnull InvertibleFunctionKeyExpression function,
                                                      @Nonnull Comparison originalComparison) {
            if (function.getMinArguments() != 1 || function.getMaxArguments() != 1 || function.getColumnSize() != 1) {
                throw new RecordCoreArgumentException("only unary functions can be inverted")
                        .addLogInfo(LogMessageKeys.FUNCTION, function.getName());
            }
            final Type underlyingType = originalComparison.getType();
            if (underlyingType != Type.IN && underlyingType != Type.EQUALS) {
                throw new RecordCoreArgumentException("cannot create inverted function comparison of given comparison type")
                        .addLogInfo(LogMessageKeys.FUNCTION, function.getName())
                        .addLogInfo(LogMessageKeys.COMPARISON_TYPE, underlyingType);
            }
            final Type newType = function.isInjective() ? underlyingType : Type.IN;
            return new InvertedFunctionComparison(function, originalComparison, newType);
        }
    }
}
