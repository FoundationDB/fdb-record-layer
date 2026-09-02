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
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.Bindings.Internal;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TupleFieldsProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.InvertibleFunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.planprotos.PComparison;
import com.apple.foundationdb.record.planprotos.PComparison.PComparisonType;
import com.apple.foundationdb.record.planprotos.PDistanceRankValueComparison;
import com.apple.foundationdb.record.planprotos.PInvertedFunctionComparison;
import com.apple.foundationdb.record.planprotos.PListComparison;
import com.apple.foundationdb.record.planprotos.PMultiColumnComparison;
import com.apple.foundationdb.record.planprotos.PNullComparison;
import com.apple.foundationdb.record.planprotos.POpaqueEqualityComparison;
import com.apple.foundationdb.record.planprotos.PParameterComparison;
import com.apple.foundationdb.record.planprotos.PSimpleComparison;
import com.apple.foundationdb.record.planprotos.PValueComparison;
import com.apple.foundationdb.record.provider.common.text.TextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextTokenizerRegistry;
import com.apple.foundationdb.record.provider.common.text.TextTokenizerRegistryImpl;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.cursors.ProbableIntersectionCursor;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.UsesValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.WithValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LikeOperatorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolMessageEnum;

import org.jspecify.annotations.Nullable;

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
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Helper methods for building {@link Comparison}s.
 */
@API(API.Status.UNSTABLE)
public class Comparisons {
    public static final Comparison LIST_EMPTY = new ListComparison(Type.EQUALS, Collections.emptyList());

    private Comparisons() {
    }

    // A simple wrapper around bytes that performs unsigned comparisons with
    // other instances of UnsignedBytes, which is useful for comparisons consistent
    // with those done on byte arrays by the underlying database.
    private static class UnsignedBytes implements Comparable<UnsignedBytes> {
        private byte[] data;

        public UnsignedBytes(byte[] data) {
            this.data = data;
        }

        @Override
        public int compareTo(UnsignedBytes other) {
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
        public int compareTo(UnsignedUUID that) {
            int msbCompare = Long.compareUnsigned(this.mostSignificantBits, that.mostSignificantBits);
            if (msbCompare != 0) {
                return msbCompare;
            }
            return Long.compareUnsigned(this.leastSignificantBits, that.leastSignificantBits);
        }
    }

    @SuppressWarnings("rawtypes")
    private static Comparable toComparable(Object obj) {
        if (obj instanceof ByteString) {
            return new UnsignedBytes(((ByteString) obj).toByteArray());
        } else if (obj instanceof byte[]) {
            return new UnsignedBytes((byte[])obj);
        } else if (obj instanceof UUID) {
            UUID uuid = (UUID)obj;
            return new UnsignedUUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        } else if (obj instanceof EnumLite) {
            return ((EnumLite)obj).getNumber();
        } else if (obj instanceof Comparable) {
            return (Comparable) obj;
        } else {
            throw new RecordCoreException("Tried to compare non-comparable object " + obj.getClass());
        }
    }

    public static Object toClassWithRealEquals(Object obj) {
        if (obj instanceof ByteString) {
            return obj;
        } else if (obj instanceof byte[]) {
            return ByteString.copyFrom((byte[])obj);
        } else if (obj instanceof EnumLite) {
            return ((EnumLite)obj).getNumber();
        } else if (obj instanceof Comparable) {
            return obj;
        } else if (obj instanceof List) {
            return obj;
        } else if (obj instanceof RealVector) {
            return obj;
        } else {
            throw new RecordCoreException("Tried to compare non-comparable object " + obj.getClass());
        }
    }

    @SuppressWarnings("unchecked")
    public static int compare(Object fieldValue, Object comparand) {
        return toComparable(fieldValue).compareTo(toComparable(comparand));
    }

    private static boolean compareEquals(Object value, Object comparand) {
        if (value instanceof Message) {
            return MessageHelpers.compareMessageEquals(value, comparand);
        } else {
            return toClassWithRealEquals(value).equals(toClassWithRealEquals(comparand));
        }
    }

    private static boolean compareNotDistinctFrom(@Nullable Object value, @Nullable Object comparand) {
        if (value == null && comparand == null) {
            return true;
        } else if (value == null || comparand == null) {
            return false;
        } else {
            if (value instanceof Message) {
                return MessageHelpers.compareMessageEquals(value, comparand);
            } else {
                return toClassWithRealEquals(Objects.requireNonNull(value)).equals(toClassWithRealEquals(Objects.requireNonNull(comparand)));
            }
        }
    }

    @Nullable
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

    @Nullable
    private static Boolean compareLike(@Nullable Object value, @Nullable Object pattern) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof String)) {
            throw new RecordCoreException("Illegal comparand value type: " + value);
        }
        if (!(pattern instanceof Message)) {
            throw new RecordCoreException("Illegal pattern value type: " + pattern);
        }
        return LikeOperatorValue.likeOperation((String)value, (Message)pattern);
    }

    public static Boolean compareListEquals(@Nullable Object value, List<?> comparand) {
        if (value instanceof List<?>) {
            List<?> list = (List<?>) value;
            if (list.size() != comparand.size()) {
                return false;
            }
            return compareListStartsWith(value, comparand);
        } else {
            throw new RecordCoreException("value from record did not match comparand");
        }
    }

    private static Boolean compareListStartsWith(@Nullable Object value, List<?> comparand) {
        if (value instanceof List<?>) {
            List<?> list = (List<?>) value;
            for (int i = 0; i < comparand.size(); i++) {
                if (i > list.size()) {
                    return false;
                }
                if (comparand.get(i) == null && list.get(i) == null) {
                    continue;
                }
                if (comparand.get(i) == null || list.get(i) == null) {
                    return false;
                } else if (!toClassWithRealEquals(comparand.get(i)).equals(toClassWithRealEquals(list.get(i)))) {
                    return false;
                }
            }
            return true;
        } else {
            throw new RecordCoreException("value from record did not match comparand");
        }
    }

    @Nullable
    private static Boolean compareIn(@Nullable Object value, @Nullable Object comparand) {
        if (value == null || comparand == null) {
            return null;
        }
        if ((comparand instanceof List<?>)) {
            boolean hasNull = false;
            value = (value instanceof Message) ? value : toClassWithRealEquals(value);
            for (Object comparandItem : (List<?>) comparand) {
                if (value instanceof Message) {
                    if (MessageHelpers.compareMessageEquals(value, comparandItem)) {
                        return true;
                    }
                } else {
                    if (comparandItem == null) {
                        hasNull = true;
                    } else if (toClassWithRealEquals(value).equals(toClassWithRealEquals(comparandItem))) {
                        return true;
                    }
                }
            }
            return hasNull ? null : false;
        } else {
            throw new RecordCoreException("IN comparison with a non-list type" + comparand.getClass());
        }
    }

    @Nullable
    private static Boolean compareTextContainsSingle(Iterator<? extends CharSequence> valueIterator, String comparandToken) {
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
    private static Boolean compareTextContainsPrefix(Iterator<? extends CharSequence> valueIterator, String comparandToken) {
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

    private static Set<String> getComparandSet(List<String> comparandList) {
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
    private static Boolean compareTextContainsAll(Iterator<? extends CharSequence> valueIterator, List<String> comparand) {
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
    private static Boolean compareTextContainsAllWithin(Iterator<? extends CharSequence> valueIterator, List<String> comparand, int maxDistance) {
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
    private static Boolean compareTextContainsAny(Iterator<? extends CharSequence> valueIterator, List<String> comparand) {
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
    private static Boolean compareTextContainsAllPrefixes(Iterator<? extends CharSequence> valueIterator, List<String> comparand) {
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
    private static Boolean compareTextContainsAnyPrefix(Iterator<? extends CharSequence> valueIterator, List<String> comparand) {
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
    private static Boolean compareTextContainsPhrase(Iterator<? extends CharSequence> valueIterator, List<String> comparand) {
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
        SORT(false),
        @API(API.Status.EXPERIMENTAL)
        LIKE,
        IS_DISTINCT_FROM(false),
        NOT_DISTINCT_FROM(true),
        @API(API.Status.EXPERIMENTAL)
        DISTANCE_RANK_EQUALS(true),
        @API(API.Status.EXPERIMENTAL)
        DISTANCE_RANK_LESS_THAN,
        @API(API.Status.EXPERIMENTAL)
        DISTANCE_RANK_LESS_THAN_OR_EQUAL;

        private static final Supplier<BiMap<Type, PComparisonType>> protoEnumBiMapSupplier =
                Suppliers.memoize(() -> PlanSerialization.protoEnumBiMap(Type.class, PComparisonType.class));

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

        @SuppressWarnings("unused")
        public PComparisonType toProto(final PlanSerializationContext serializationContext) {
            return Objects.requireNonNull(getProtoEnumBiMap().get(this));
        }

        @SuppressWarnings("unused")
        public static Type fromProto(final PlanSerializationContext serializationContext,
                                     final PComparisonType physicalOperatorProto) {
            return Objects.requireNonNull(getProtoEnumBiMap().inverse().get(physicalOperatorProto));
        }

        private static BiMap<Type, PComparisonType> getProtoEnumBiMap() {
            return protoEnumBiMapSupplier.get();
        }
    }

    @Nullable
    public static Type invertComparisonType(final Type type) {
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
    public static Boolean evalComparison(Type type, @Nullable Object value, @Nullable Object comparand) {
        switch (type) {
            case STARTS_WITH:
                return compareStartsWith(value, comparand);
            case IN:
                return compareIn(value, comparand);
            case EQUALS:
                if (value == null || comparand == null) {
                    return null;
                }
                return compareEquals(value, comparand);
            case NOT_EQUALS:
                if (value == null || comparand == null) {
                    return null;
                }
                return !compareEquals(value, comparand);
            case IS_DISTINCT_FROM:
                return !compareNotDistinctFrom(value, comparand);
            case NOT_DISTINCT_FROM:
                return compareNotDistinctFrom(value, comparand);
            case LESS_THAN:
                if (value == null || comparand == null) {
                    return null;
                }
                return compare(value, comparand) < 0;
            case LESS_THAN_OR_EQUALS:
                if (value == null || comparand == null) {
                    return null;
                }
                return compare(value, comparand) <= 0;
            case GREATER_THAN:
                if (value == null || comparand == null) {
                    return null;
                }
                return compare(value, comparand) > 0;
            case GREATER_THAN_OR_EQUALS:
                if (value == null || comparand == null) {
                    return null;
                }
                return compare(value, comparand) >= 0;
            case LIKE:
                return compareLike(value, comparand);
            default:
                throw new RecordCoreException("Unsupported comparison type: " + type);
        }
    }

    @Nullable
    @SuppressWarnings("rawtypes")
    public static Boolean evalListComparison(Type type, @Nullable Object value, @Nullable List comparand) {
        if (value == null) {
            return null;
        }
        switch (type) {
            case EQUALS:
                return compareListEquals(value, Objects.requireNonNull(comparand));
            case NOT_EQUALS:
                return !compareListEquals(value, Objects.requireNonNull(comparand));
            case STARTS_WITH:
                return compareListStartsWith(value, Objects.requireNonNull(comparand));
            case IN:
                return compareIn(value, Objects.requireNonNull(comparand));
            default:
                throw new RecordCoreException("Only equals/not equals/starts with are supported for lists");
        }
    }

    /**
     * A comparison between a value associated with someplace in the record (such as a field) and a value associated
     * with the plan (such as a constant or a bound parameter).
     */
    public interface Comparison extends WithValue<Comparison>, PlanHashable, Correlated<Comparison>, UsesValueEquivalence<Comparison>, PlanSerializable {
        /**
         * Evaluate this comparison for the value taken from the target record.
         * @param store the record store for the query
         * @param context the evaluation context for getting the other comparison value
         * @param value the value taken from the record
         * @return the tri-valued logic result of the comparison
         */
        @Nullable
        Boolean eval(@Nullable FDBRecordStoreBase<?> store, EvaluationContext context, @Nullable Object value);

        /**
         * Validate that this comparison is compatible with a given record field.
         * @param descriptor the Protobuf descriptor for the proposed comparison field
         * @param fannedOut whether a repeated field fans out into multiple comparisons or is treated as a single list value
         */
        void validate(Descriptors.FieldDescriptor descriptor, boolean fannedOut);

        /**
         * Get the comparison type.
         * @return the comparison type
         */
        Type getType();

        Comparison withType(Type newType);

        @Nullable
        @Override
        default Value getValue() {
            return null;
        }

        @Override
        default Comparison withValue(Value value) {
            throw new RecordCoreException("withValue is not implemented");
        }

        Optional<Comparison> replaceValuesMaybe(Function<Value, Optional<Value>> replacementFunction);

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
         * If so, {@link #getComparand} will return a {@link Tuple}.
         * @return {@code true} if the comparand is for multiple key columns
         */
        default boolean hasMultiColumnComparand() {
            return false;
        }

        /**
         * Get the printed representation of the comparison less the comparison operator itself.
         * @return the typeless string
         */
        String typelessString();

        default Comparison withParameterRelationshipMap(ParameterRelationshipGraph parameterRelationshipGraph) {
            return this;
        }

        @Override
        default Set<CorrelationIdentifier> getCorrelatedTo() {
            return ImmutableSet.of();
        }

        @Override
        default Comparison rebase(AliasMap translationMap) {
            return translateCorrelations(TranslationMap.rebaseWithAliasMap(translationMap), false);
        }

        Comparison translateCorrelations(TranslationMap translationMap, boolean shouldSimplifyValues);

        @Override
        default boolean semanticEquals(@Nullable Object other, AliasMap aliasMap) {
            return semanticEquals(other, ValueEquivalence.fromAliasMap(aliasMap)).isTrue();
        }

        @Override
        @SuppressWarnings("unused")
        default ConstrainedBoolean semanticEqualsTyped(final Comparison other,
                                                       final ValueEquivalence valueEquivalence) {
            return this.equals(other) ? ConstrainedBoolean.alwaysTrue() : ConstrainedBoolean.falseValue();
        }

        @Override
        default int semanticHashCode() {
            return hashCode();
        }

        @SuppressWarnings("unused")
        PComparison toComparisonProto(PlanSerializationContext serializationContext);

        static Comparison fromComparisonProto(final PlanSerializationContext serializationContext,
                                              final PComparison comparisonProto) {
            return (Comparison)PlanSerialization.dispatchFromProtoContainer(serializationContext, comparisonProto);
        }

        ExplainTokensWithPrecedence explain();
    }

    public static String toPrintable(@Nullable Object value) {
        if (value instanceof ByteString) {
            return toPrintable(((ByteString)value).toByteArray());
        } else if (value instanceof byte[]) {
            // loggable() only returns null when given a null array; value is non-null here.
            return Objects.requireNonNull(ByteArrayUtil2.loggable((byte[])value));
        } else {
            return Objects.toString(value);
        }
    }

    /**
     * A comparison with a constant value.
     */
    public abstract static class SimpleComparisonBase implements Comparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Simple-Comparison");

        protected final Type type;
        protected final Object comparand;

        protected SimpleComparisonBase(Type type, Object comparand) {
            this.type = type;
            this.comparand = comparand;
        }

        @Override
        public void validate(Descriptors.FieldDescriptor fieldDescriptor, boolean fannedOut) {
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
        private boolean validForComparand(Descriptors.FieldDescriptor fieldDescriptor) {
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
                    if (comparand instanceof ProtocolMessageEnum) {
                        return fieldDescriptor.getEnumType().equals(((ProtocolMessageEnum)comparand).getDescriptorForType());
                    }
                    return comparand instanceof ProtoUtils.DynamicEnum; // returns false for descriptors
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

        @Override
        public Object getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            return comparand;
        }

        @Override
        public Type getType() {
            return type;
        }

        @Nullable
        @Override
        public Value getValue() {
            // Use the 2-arg overload directly: this class's override of it is statically known to be
            // non-null (it just returns the non-null comparand field), unlike the inherited no-arg
            // default which is declared @Nullable for implementors whose comparand can be absent.
            return LiteralValue.ofScalar(getComparand(null, null));
        }

        @Override
        public Comparison withValue(final Value value) {
            if (value instanceof LiteralValue<?>) {
                return new SimpleComparison(getType(),
                        Objects.requireNonNull(((LiteralValue<?>)value).getLiteralValue()));
            }
            return new ValueComparison(getType(), value);
        }

        @Override
        public Optional<Comparison> replaceValuesMaybe(final Function<Value, Optional<Value>> replacementFunction) {
            return Optional.of(this);
        }

        @Nullable
        @Override
        public Boolean eval(@Nullable FDBRecordStoreBase<?> store, EvaluationContext context, @Nullable Object value) {
            return evalComparison(type, value, getComparand(store, context));
        }

        @Override
        public String typelessString() {
            return toPrintable(comparand);
        }

        @Override
        public String toString() {
            return explain().getExplainTokens().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Override
        public ExplainTokensWithPrecedence explain() {
            return ExplainTokensWithPrecedence.of(new ExplainTokens().addKeyword(type.name())
                    .addWhitespace().addIdentifier(typelessString()));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SimpleComparisonBase that = (SimpleComparisonBase) o;
            return type == that.type &&
                    Objects.equals(toClassWithRealEquals(comparand), toClassWithRealEquals(that.comparand));
        }

        @Override
        public int hashCode() {
            return Objects.hash(type.name(), toClassWithRealEquals(comparand));
        }

        @Override
        public int planHash(final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return type.name().hashCode() + PlanHashable.objectPlanHash(mode, comparand);
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, type, comparand);
                default:
                    throw new UnsupportedOperationException("Hash Kind " + mode.name() + " is not supported");
            }
        }

        @Override
        public Comparison translateCorrelations(final TranslationMap translationMap,
                                                final boolean shouldSimplifyValues) {
            return this;
        }
    }

    /**
     * A comparison with a constant value.
     */
    public static class SimpleComparison extends SimpleComparisonBase {
        public SimpleComparison(Type type, Object comparand) {
            super(type, comparand);
        }

        @Override
        public Comparison withType(final Type newType) {
            if (type == newType) {
                return this;
            }
            return new SimpleComparison(newType, comparand);
        }

        @Override
        public PSimpleComparison toProto(final PlanSerializationContext serializationContext) {
            return PSimpleComparison.newBuilder()
                    .setType(type.toProto(serializationContext))
                    .setObject(PlanSerialization.valueObjectToProto(comparand))
                    .build();
        }

        @Override
        public PComparison toComparisonProto(final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setSimpleComparison(toProto(serializationContext)).build();
        }

        public static SimpleComparison fromProto(final PlanSerializationContext serializationContext,
                                                 final PSimpleComparison simpleComparisonProto) {
            return new SimpleComparison(Type.fromProto(serializationContext, Objects.requireNonNull(simpleComparisonProto.getType())),
                    Objects.requireNonNull(PlanSerialization.protoToValueObject(Objects.requireNonNull(simpleComparisonProto.getObject()))));
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PSimpleComparison, SimpleComparison> {
            @Override
            public Class<PSimpleComparison> getProtoMessageClass() {
                return PSimpleComparison.class;
            }

            @Override
            public SimpleComparison fromProto(final PlanSerializationContext serializationContext,
                                              final PSimpleComparison simpleComparisonProto) {
                return SimpleComparison.fromProto(serializationContext, simpleComparisonProto);
            }
        }
    }

    /**
     * Exception thrown when comparison evaluation needs the query context, but none was supplied.
     */
    @SuppressWarnings("serial")
    public static class EvaluationContextRequiredException extends RecordCoreException {
        private static final Supplier<EvaluationContextRequiredException> INSTANCE_SUPPLIER =
                Suppliers.memoize(() -> new EvaluationContextRequiredException("unable to evaluate comparison without context and/or store"));

        // RecordCoreException(String, Throwable, boolean, boolean) mirrors Throwable's own
        // constructor, which legitimately allows a null cause; that 4-arg constructor just
        // isn't annotated @Nullable yet (owned outside this change's scope).
        @SuppressWarnings("NullAway")
        private EvaluationContextRequiredException(String msg) {
            super(msg, null, false, false);
        }

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
        String getParameter();
    }

    /**
     * A comparison with a bound parameter, as opposed to a literal constant in the query.
     */
    public abstract static class ParameterComparisonBase implements ComparisonWithParameter {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Parameter-Comparison");

        protected final Type type;
        protected final String parameter;
        protected final @Nullable Internal internal;
        protected final ParameterRelationshipGraph parameterRelationshipGraph;
        @SuppressWarnings("this-escape")
        protected final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        protected ParameterComparisonBase(Type type, String parameter,
                                          @Nullable Internal internal,
                                          ParameterRelationshipGraph parameterRelationshipGraph) {
            checkInternalBinding(parameter, internal);
            this.type = type;
            this.parameter = parameter;
            this.internal = internal;
            if (type.isUnary()) {
                throw new RecordCoreException("Unary comparison type " + type + " cannot be bound to a parameter");
            }
            this.parameterRelationshipGraph = parameterRelationshipGraph;
        }

        @Override
        @SuppressWarnings("PMD.EmptyMethodInAbstractClassShouldBeAbstract")
        public void validate(Descriptors.FieldDescriptor descriptor, boolean fannedOut) {
            // No additional validation.
        }

        @Override
        public Type getType() {
            return type;
        }

        public boolean isCorrelation() {
            return internal == Bindings.Internal.CORRELATION;
        }

        @Override
        public boolean isCorrelatedTo(final CorrelationIdentifier alias) {
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

        @Override
        public Optional<Comparison> replaceValuesMaybe(final Function<Value, Optional<Value>> replacementFunction) {
            if (isCorrelation()) {
                return Optional.empty();
            }
            return Optional.of(this);
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Comparison translateCorrelations(final TranslationMap translationMap, final boolean shouldSimplifyValues) {
            if (isCorrelation()) {
                final var alias = CorrelationIdentifier.of(Bindings.Internal.CORRELATION.identifier(parameter));
                final var quantifiedObjectValue = QuantifiedObjectValue.of(alias,
                        com.apple.foundationdb.record.query.plan.cascades.typing.Type.any());

                //
                // Note that the following cast must work! If it does not we are in a bad spot and we should fail.
                //
                final var translatedQuantifiedObjectValue =
                        (QuantifiedObjectValue)quantifiedObjectValue
                                .translateCorrelations(translationMap, shouldSimplifyValues);
                if (quantifiedObjectValue == translatedQuantifiedObjectValue) {
                    return this;
                }
                return withTranslatedCorrelation(translatedQuantifiedObjectValue.getAlias());
            } else {
                return this;
            }
        }

        protected abstract ParameterComparisonBase withTranslatedCorrelation(CorrelationIdentifier translatedAlias);

        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            if (!isCorrelation()) {
                return ImmutableSet.of();
            }
            return ImmutableSet.of(getAlias());
        }

        @Override
        public ConstrainedBoolean semanticEqualsTyped(final Comparison other, final ValueEquivalence valueEquivalence) {
            ParameterComparisonBase that = (ParameterComparisonBase) other;
            if (type != that.type) {
                return ConstrainedBoolean.falseValue();
            }

            //
            // Either this parameter is a proper correlation in which case the alias map needs to be consulted,
            // or, if it is a non-correlation like an extracted literal we need to consult the parameter relationship
            // graph.
            //
            if (isCorrelation() && that.isCorrelation()) {
                if (getAlias().equals(that.getAlias())) {
                    return ConstrainedBoolean.alwaysTrue();
                }
                // This case should happen rather infrequently
                return valueEquivalence.isDefinedEqual(getAlias(), that.getAlias());
            }

            if (!getParameter().equals(that.getParameter())) {
                return ConstrainedBoolean.falseValue();
            }

            return Objects.equals(relatedByEquality(), that.relatedByEquality())
                   ? ConstrainedBoolean.alwaysTrue() : ConstrainedBoolean.falseValue();
        }

        @Nullable
        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Boolean eval(@Nullable FDBRecordStoreBase<?> store, EvaluationContext context, @Nullable Object value) {
            // this is at evaluation time --> always use the context binding
            final Object comparand = getComparand(store, context);
            if (comparand == COMPARISON_SKIPPED_BINDING) {
                return Boolean.TRUE;
            } else {
                return evalComparison(type, value, comparand);
            }
        }

        @Override
        public String typelessString() {
            return "$" + parameter;
        }

        @Override
        public String toString() {
            return explain().getExplainTokens().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Override
        public ExplainTokensWithPrecedence explain() {
            return ExplainTokensWithPrecedence.of(new ExplainTokens().addKeyword(type.name())
                    .addWhitespace().addIdentifier(typelessString()));
        }

        @Override
        public String getParameter() {
            return parameter;
        }

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
            return semanticEquals(o, AliasMap.emptyMap());
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        public int computeHashCode() {
            // Note: This hash must be stable across JVMs, as it is used in `Comparison.semanticHashCode()`.
            return Objects.hash(type.name(), relatedByEquality());
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
        public int planHash(final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return type.name().hashCode() + (isCorrelation() ? 0 : parameter.hashCode());
                case FOR_CONTINUATION:
                    if (isCorrelation()) {
                        return PlanHashable.objectsPlanHash(mode, BASE_HASH, type);
                    } else {
                        return PlanHashable.objectsPlanHash(mode, BASE_HASH, type, parameter);
                    }
                default:
                    throw new UnsupportedOperationException("Hash Kind " + mode.name() + " is not supported");
            }
        }

        private static String checkInternalBinding(String parameter, @Nullable Internal internal) {
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
    public static class ParameterComparison extends ParameterComparisonBase {
        protected ParameterComparison(Type type, String parameter,
                                      @Nullable Internal internal,
                                      ParameterRelationshipGraph parameterRelationshipGraph) {
            super(type, parameter, internal, parameterRelationshipGraph);
        }

        public ParameterComparison(Type type, String parameter) {
            this(type, parameter, null, ParameterRelationshipGraph.unbound());
        }

        public ParameterComparison(Type type, String parameter, @Nullable Internal internal) {
            this(type, parameter, internal, ParameterRelationshipGraph.unbound());
        }

        @Override
        public Comparison withType(final Type newType) {
            if (type == newType) {
                return this;
            }
            return new ParameterComparison(newType, parameter, internal, parameterRelationshipGraph);
        }

        @Override
        protected ParameterComparisonBase withTranslatedCorrelation(CorrelationIdentifier translatedAlias) {
            return new ParameterComparison(type,
                                           Bindings.Internal.CORRELATION.bindingName(translatedAlias.getId()),
                                           Bindings.Internal.CORRELATION,
                                           parameterRelationshipGraph);
        }

        @Override
        public Comparison withParameterRelationshipMap(final ParameterRelationshipGraph parameterRelationshipGraph) {
            Verify.verify(this.parameterRelationshipGraph.isUnbound());
            return new ParameterComparison(type, parameter, internal, parameterRelationshipGraph);
        }

        @Override
        public PParameterComparison toProto(final PlanSerializationContext serializationContext) {
            final PParameterComparison.Builder builder = PParameterComparison.newBuilder()
                    .setType(type.toProto(serializationContext))
                    .setParameter(parameter);

            if (internal != null) {
                builder.setInternal(internal.toProto(serializationContext));
            }
            return builder.build();
        }

        @Override
        public PComparison toComparisonProto(final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setParameterComparison(toProto(serializationContext)).build();
        }

        public static ParameterComparison fromProto(final PlanSerializationContext serializationContext,
                                                    final PParameterComparison parameterComparisonProto) {
            final Bindings.Internal internal;
            if (parameterComparisonProto.hasInternal()) {
                internal = Bindings.Internal.fromProto(serializationContext, Objects.requireNonNull(parameterComparisonProto.getInternal()));
            } else {
                internal = null;
            }
            return new ParameterComparison(Type.fromProto(serializationContext, Objects.requireNonNull(parameterComparisonProto.getType())),
                    Objects.requireNonNull(parameterComparisonProto.getParameter()),
                    internal);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PParameterComparison, ParameterComparison> {
            @Override
            public Class<PParameterComparison> getProtoMessageClass() {
                return PParameterComparison.class;
            }

            @Override
            public ParameterComparison fromProto(final PlanSerializationContext serializationContext,
                                                 final PParameterComparison parameterComparisonProto) {
                return ParameterComparison.fromProto(serializationContext, parameterComparisonProto);
            }
        }
    }

    /**
     * A comparison with a {@link Value}, as opposed to a literal constant in the query.
     */
    public static class ValueComparison implements Comparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Value-Comparison");
        private final Type type;
        private final Value comparandValue;
        protected final ParameterRelationshipGraph parameterRelationshipGraph;
        private final Supplier<Integer> hashCodeSupplier;

        protected ValueComparison(final PlanSerializationContext serializationContext,
                                  final PValueComparison valueComparisonProto) {
            this(Type.fromProto(serializationContext, Objects.requireNonNull(valueComparisonProto.getType())),
                    Value.fromValueProto(serializationContext, Objects.requireNonNull(valueComparisonProto.getComparandValue())));
        }

        public ValueComparison(final Type type,
                               final Value comparandValue) {
            this(type, comparandValue, ParameterRelationshipGraph.unbound());
        }

        @SuppressWarnings("this-escape")
        public ValueComparison(final Type type,
                               final Value comparandValue,
                               final ParameterRelationshipGraph parameterRelationshipGraph) {
            this.type = type;
            this.comparandValue = comparandValue;
            if (type.isUnary()) {
                throw new RecordCoreException("Unary comparison type " + type + " cannot be bound to a value");
            }
            this.parameterRelationshipGraph = parameterRelationshipGraph;
            this.hashCodeSupplier = Suppliers.memoize(this::computeHashCode);
        }

        @Override
        public void validate(Descriptors.FieldDescriptor descriptor, boolean fannedOut) {
            // No additional validation.
        }

        @Override
        public Type getType() {
            return type;
        }

        @Override
        public ValueComparison withType(final Type newType) {
            if (type == newType) {
                return this;
            }
            return new ValueComparison(newType, comparandValue, parameterRelationshipGraph);
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public ValueComparison withValue(final Value value) {
            if (comparandValue == value) {
                return this;
            }
            return new ValueComparison(getType(), value);
        }

        public Value getComparandValue() {
            return comparandValue;
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Optional<Comparison> replaceValuesMaybe(final Function<Value, Optional<Value>> replacementFunction) {
            return replacementFunction.apply(getValue())
                    .map(replacedComparandValue -> {
                        if (replacedComparandValue == getValue()) {
                            return this;
                        }
                        return withValue(replacedComparandValue);
                    });
        }

        @Nullable
        @Override
        public Object getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            if (context == null) {
                throw EvaluationContextRequiredException.instance();
            }
            return comparandValue.eval(store, context);
        }

        @Override
        public ValueComparison translateCorrelations(final TranslationMap translationMap,
                                                     final boolean shouldSimplifyValues) {
            if (comparandValue.getCorrelatedTo()
                    .stream()
                    .noneMatch(translationMap::containsSourceAlias)) {
                return this;
            }

            return new ValueComparison(type, comparandValue.translateCorrelations(translationMap, shouldSimplifyValues),
                    parameterRelationshipGraph);
        }

        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            return comparandValue.getCorrelatedTo();
        }

        @Override
        public Value getValue() {
            return getComparandValue();
        }

        @Override
        public ConstrainedBoolean semanticEqualsTyped(final Comparison other, final ValueEquivalence valueEquivalence) {
            final var that = (ValueComparison) other;
            if (type != that.type) {
                return ConstrainedBoolean.falseValue();
            }

            return comparandValue.semanticEquals(that.comparandValue, valueEquivalence);
        }

        @Nullable
        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Boolean eval(@Nullable FDBRecordStoreBase<?> store, EvaluationContext context, @Nullable Object v) {
            // this is at evaluation time --> always use the context binding
            final Object comparand = getComparand(store, context);
            if (comparand == COMPARISON_SKIPPED_BINDING) {
                return Boolean.TRUE;
            } else {
                return evalComparison(type, v, comparand);
            }
        }

        @Override
        public String typelessString() {
            return comparandValue.toString();
        }

        @Override
        public String toString() {
            return explain().getExplainTokens().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Override
        public ExplainTokensWithPrecedence explain() {
            return ExplainTokensWithPrecedence.of(new ExplainTokens().addKeyword(type.name())
                    .addWhitespace().addNested(comparandValue.explain().getExplainTokens()));
        }

        @Override
        @SpotBugsSuppressWarnings("EQ_UNUSUAL")
        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        public boolean equals(Object o) {
            return semanticEquals(o, AliasMap.emptyMap());
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        public int computeHashCode() {
            return Objects.hash(type.name(), relatedByEquality());
        }

        private Set<String> relatedByEquality() {
            return ImmutableSet.of();
        }

        @Override
        public int planHash(final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, type);
                default:
                    throw new UnsupportedOperationException("Hash Kind " + mode.name() + " is not supported");
            }
        }

        @Override
        public ValueComparison withParameterRelationshipMap(final ParameterRelationshipGraph parameterRelationshipGraph) {
            Verify.verify(this.parameterRelationshipGraph.isUnbound());
            return new ValueComparison(type, comparandValue, parameterRelationshipGraph);
        }

        @Override
        public Message toProto(final PlanSerializationContext serializationContext) {
            return toValueComparisonProto(serializationContext);
        }

        public PValueComparison toValueComparisonProto(final PlanSerializationContext serializationContext) {
            return PValueComparison.newBuilder()
                    .setType(type.toProto(serializationContext))
                    .setComparandValue(comparandValue.toValueProto(serializationContext))
                    .build();
        }

        @Override
        public PComparison toComparisonProto(final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setValueComparison(toValueComparisonProto(serializationContext)).build();
        }

        public static ValueComparison fromProto(final PlanSerializationContext serializationContext,
                                                final PValueComparison valueComparisonProto) {
            return new ValueComparison(serializationContext, valueComparisonProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PValueComparison, ValueComparison> {
            @Override
            public Class<PValueComparison> getProtoMessageClass() {
                return PValueComparison.class;
            }

            @Override
            public ValueComparison fromProto(final PlanSerializationContext serializationContext,
                                             final PValueComparison valueComparisonProto) {
                return ValueComparison.fromProto(serializationContext, valueComparisonProto);
            }
        }
    }

    @SpotBugsSuppressWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
    public static class DistanceRankValueComparison extends ValueComparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Distance-Rank-Value-Comparison");

        private final Value limitValue;

        @Nullable
        private final Integer efSearch;

        @Nullable
        private final Boolean isReturningVectors;

        protected DistanceRankValueComparison(PlanSerializationContext serializationContext,
                                              final PDistanceRankValueComparison distanceRankValueComparisonProto) {
            super(serializationContext, distanceRankValueComparisonProto.getSuper());
            this.limitValue = Value.fromValueProto(serializationContext,
                    Objects.requireNonNull(distanceRankValueComparisonProto.getLimitValue()));
            this.efSearch = distanceRankValueComparisonProto.hasEfSearch() ? distanceRankValueComparisonProto.getEfSearch() : null;
            this.isReturningVectors = distanceRankValueComparisonProto.hasIsReturningVectors() ? distanceRankValueComparisonProto.getIsReturningVectors() : null;
        }

        public DistanceRankValueComparison(final Type type, final Value comparandValue,
                                           final Value limitValue, @Nullable final Integer efSearch,
                                           @Nullable final Boolean isReturningVectors) {
            this(type, comparandValue, ParameterRelationshipGraph.unbound(), limitValue, efSearch, isReturningVectors);
        }

        public DistanceRankValueComparison(final Type type, final Value comparandValue,
                                           final ParameterRelationshipGraph parameterRelationshipGraph,
                                           final Value limitValue, @Nullable  final Integer efSearch,
                                           @Nullable final Boolean isReturningVectors) {
            super(type, comparandValue, parameterRelationshipGraph);
            Verify.verify(type == Type.DISTANCE_RANK_LESS_THAN ||
                    type == Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL);
            this.limitValue = limitValue;
            this.efSearch = efSearch;
            this.isReturningVectors = isReturningVectors;
        }

        public Value getLimitValue() {
            return limitValue;
        }

        @Override
        public DistanceRankValueComparison withType(final Type newType) {
            if (getType() == newType) {
                return this;
            }
            return new DistanceRankValueComparison(newType, getComparandValue(), parameterRelationshipGraph,
                    getLimitValue(), getEfSearch(), isReturningVectors());
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public DistanceRankValueComparison withValue(final Value value) {
            if (getComparandValue() == value) {
                return this;
            }
            return new DistanceRankValueComparison(getType(), value, parameterRelationshipGraph, getLimitValue(),
                    getEfSearch(), isReturningVectors());
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Optional<Comparison> replaceValuesMaybe(final Function<Value, Optional<Value>> replacementFunction) {
            final var replacedComparandValueMaybe = replacementFunction.apply(getComparandValue());
            if (replacedComparandValueMaybe.isEmpty()) {
                return Optional.empty();
            }
            final var replacedLimitValueMaybe = replacementFunction.apply(getLimitValue());
            if (replacedLimitValueMaybe.isEmpty()) {
                return Optional.empty();
            }

            if (replacedComparandValueMaybe.get() == getComparandValue() &&
                    replacedLimitValueMaybe.get() == getLimitValue()) {
                return Optional.of(this);
            }
            return Optional.of(new DistanceRankValueComparison(getType(), replacedComparandValueMaybe.get(),
                    parameterRelationshipGraph, replacedLimitValueMaybe.get(), getEfSearch(), isReturningVectors()));
        }

        @Override
        public DistanceRankValueComparison translateCorrelations(final TranslationMap translationMap,
                                                                 final boolean shouldSimplifyValues) {
            if (getComparandValue().getCorrelatedTo()
                    .stream()
                    .noneMatch(translationMap::containsSourceAlias) &&
                    getLimitValue().getCorrelatedTo()
                            .stream()
                            .noneMatch(translationMap::containsSourceAlias)) {
                return this;
            }

            return new DistanceRankValueComparison(getType(),
                    getComparandValue().translateCorrelations(translationMap, shouldSimplifyValues),
                    parameterRelationshipGraph,
                    getLimitValue().translateCorrelations(translationMap, shouldSimplifyValues),
                    getEfSearch(), isReturningVectors());
        }

        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            return ImmutableSet.<CorrelationIdentifier>builder()
                    .addAll(getComparandValue().getCorrelatedTo())
                    .addAll(getLimitValue().getCorrelatedTo())
                    .build();
        }

        @Override
        public ConstrainedBoolean semanticEqualsTyped(final Comparison other, final ValueEquivalence valueEquivalence) {
            if (!(other instanceof DistanceRankValueComparison)) {
                return ConstrainedBoolean.falseValue();
            }

            return super.semanticEqualsTyped(other, valueEquivalence)
                    .compose(ignored -> getLimitValue()
                            .semanticEquals(((DistanceRankValueComparison)other).getLimitValue(),
                                    valueEquivalence))
                    .compose(ignored -> ConstrainedBoolean.ofBoolean(Objects.equals(getEfSearch(), ((DistanceRankValueComparison)other).getEfSearch())))
                    .compose(ignored -> ConstrainedBoolean.ofBoolean(Objects.equals(isReturningVectors(), ((DistanceRankValueComparison)other).isReturningVectors())));
        }

        @Nullable
        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Boolean eval(@Nullable FDBRecordStoreBase<?> store, EvaluationContext context, @Nullable Object v) {
            throw new IllegalStateException("this comparison can only be evaluated using an index");
        }

        @Override
        public String typelessString() {
            return typelessExplain().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Override
        public String toString() {
            return explain().getExplainTokens().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Override
        public ExplainTokensWithPrecedence explain() {
            return ExplainTokensWithPrecedence.of(new ExplainTokens().addKeyword(getType().name())
                    .addWhitespace().addNested(typelessExplain()));
        }

        private ExplainTokens typelessExplain() {
            return new ExplainTokens().addNested(getComparandValue().explain().getExplainTokens())
                    .addKeyword(":").addWhitespace()
                    .addNested(getLimitValue().explain().getExplainTokens());
        }

        @Override
        public int planHash(final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, getType(), getComparandValue(), getLimitValue());
                default:
                    throw new UnsupportedOperationException("Hash Kind " + mode.name() + " is not supported");
            }
        }

        @Override
        public int computeHashCode() {
            return Objects.hash(super.computeHashCode(), getType().name(), getComparandValue(), getLimitValue());
        }

        @Override
        public DistanceRankValueComparison withParameterRelationshipMap(final ParameterRelationshipGraph parameterRelationshipGraph) {
            Verify.verify(this.parameterRelationshipGraph.isUnbound());
            return new DistanceRankValueComparison(getType(), getComparandValue(), parameterRelationshipGraph,
                    getLimitValue(), getEfSearch(), isReturningVectors());
        }

        @Override
        public PDistanceRankValueComparison toProto(final PlanSerializationContext serializationContext) {
            final var distanceRankValueComparisonProtoBuilder = PDistanceRankValueComparison.newBuilder()
                    .setSuper(super.toValueComparisonProto(serializationContext))
                    .setLimitValue(getLimitValue().toValueProto(serializationContext));
            if (getEfSearch() != null) {
                distanceRankValueComparisonProtoBuilder.setEfSearch(Verify.verifyNotNull(getEfSearch()));
            }
            if (isReturningVectors() != null) {
                distanceRankValueComparisonProtoBuilder.setIsReturningVectors(Verify.verifyNotNull(isReturningVectors()));
            }
            return distanceRankValueComparisonProtoBuilder.build();
        }

        @Override
        public PComparison toComparisonProto(final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setDistanceRankValueComparison(toProto(serializationContext)).build();
        }

        @Nullable
        public RealVector getVector(@Nullable final FDBRecordStoreBase<?> store, final @Nullable EvaluationContext context) {
            return (RealVector)getComparand(store, context);
        }

        public int getLimit(@Nullable final FDBRecordStoreBase<?> store, final @Nullable EvaluationContext context) {
            if (context == null) {
                throw EvaluationContextRequiredException.instance();
            }
            return (int)Objects.requireNonNull(getLimitValue().eval(store, context));
        }

        public static DistanceRankValueComparison fromProto(final PlanSerializationContext serializationContext,
                                                            final PDistanceRankValueComparison distanceRankValueComparisonProto) {
            return new DistanceRankValueComparison(serializationContext, distanceRankValueComparisonProto);
        }

        @Nullable
        public Integer getEfSearch() {
            return efSearch;
        }

        @Nullable
        public Boolean isReturningVectors() {
            return isReturningVectors;
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PDistanceRankValueComparison, DistanceRankValueComparison> {
            @Override
            public Class<PDistanceRankValueComparison> getProtoMessageClass() {
                return PDistanceRankValueComparison.class;
            }

            @Override
            public DistanceRankValueComparison fromProto(final PlanSerializationContext serializationContext,
                                                         final PDistanceRankValueComparison distanceRankValueComparisonProto) {
                return DistanceRankValueComparison.fromProto(serializationContext, distanceRankValueComparisonProto);
            }
        }
    }

    /**
     * A comparison with a list of values.
     */
    public static class ListComparison implements Comparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("List-Comparison");

        private final Type type;
        @SuppressWarnings("rawtypes")
        private final List comparand;
        @Nullable
        private final JavaType javaType;

        @SuppressWarnings("rawtypes")
        private final Supplier<List> comparandListWithEqualsSupplier;

        @SuppressWarnings({"rawtypes", "unchecked"})
        public ListComparison(Type type, List comparand) {
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
            if (this.type == Type.IN && comparand.stream().anyMatch(o -> o == null)) {
                throw new NullPointerException("List comparand contains null");
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
            this.comparandListWithEqualsSupplier = Suppliers.memoize(this::computeComparandListWithRealEquals);
        }

        // Guava's Function<F, T> is not nullness-aware, so T is inferred @NonNull from the raw
        // List target type even though a comparand element (and thus the transformed element) is
        // allowed to be null for non-IN comparisons; the transform itself is null-safe.
        @SuppressWarnings({"rawtypes", "unchecked", "NullAway"})
        private List computeComparandListWithRealEquals() {
            return Lists.transform(comparand, obj -> obj != null ? toClassWithRealEquals(obj) : null);
        }

        private static JavaType getJavaType(Object o) {
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
            } else if (o instanceof EnumLite) {
                return JavaType.ENUM;
            } else {
                throw new RecordCoreException(o.getClass() + " is an invalid type for a comparand");
            }
        }

        @Override
        public void validate(Descriptors.FieldDescriptor fieldDescriptor, boolean fannedOut) {
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

        @Override
        @SuppressWarnings("rawtypes")
        public List getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            return comparand;
        }

        @SuppressWarnings("rawtypes")
        public List getComparandWithRealEquals() {
            return comparandListWithEqualsSupplier.get();
        }

        @Override
        public Optional<Comparison> replaceValuesMaybe(final Function<Value, Optional<Value>> replacementFunction) {
            return Optional.of(this);
        }

        @Override
        public Comparison translateCorrelations(final TranslationMap translationMap,
                                                final boolean shouldSimplifyValues) {
            return this;
        }

        @Override
        public Type getType() {
            return type;
        }

        @Override
        public Comparison withType(final Type newType) {
            if (type == newType) {
                return this;
            }
            return new ListComparison(newType, comparand);
        }

        @Nullable
        @Override
        public Boolean eval(@Nullable FDBRecordStoreBase<?> store, EvaluationContext context, @Nullable Object value) {
            return evalListComparison(type, value, getComparand(store, context));
        }

        @Override
        public String typelessString() {
            return comparand.toString();
        }

        @Override
        public String toString() {
            return explain().getExplainTokens().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Override
        public ExplainTokensWithPrecedence explain() {
            return ExplainTokensWithPrecedence.of(new ExplainTokens().addKeyword(type.name())
                    .addWhitespace().addIdentifier(typelessString()));
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
                    Objects.equals(getComparandWithRealEquals(), that.getComparandWithRealEquals()) &&
                    javaType == that.javaType;
        }

        @Override
        public int hashCode() {
            // Note: This hash must be stable across JVMs, as it is used in `Comparison.semanticHashCode()`.
            final Object javaTypeName = javaType == null ? null : javaType.name();
            return Objects.hash(type.name(), getComparandWithRealEquals(), javaTypeName);
        }

        @Override
        @SuppressWarnings("NullAway") // PlanHashable.objectsPlanHash's varargs aren't annotated @Nullable, but each
                                       // element is hashed via objectPlanHash, which is explicitly null-safe.
        public int planHash(final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return type.name().hashCode() + PlanHashable.iterablePlanHash(mode, comparand) + PlanHashable.objectPlanHash(mode, javaType);
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, type, comparand, javaType);
                default:
                    throw new UnsupportedOperationException("Hash Kind " + mode.name() + " is not supported");
            }
        }

        @Override
        public PListComparison toProto(final PlanSerializationContext serializationContext) {
            final var builder = PListComparison.newBuilder()
                    .setType(type.toProto(serializationContext));
            for (final Object element : comparand) {
                builder.addComparand(PlanSerialization.valueObjectToProto(element));
            }
            return builder.build();
        }

        @Override
        public PComparison toComparisonProto(final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setListComparison(toProto(serializationContext)).build();
        }

        public static ListComparison fromProto(final PlanSerializationContext serializationContext,
                                               final PListComparison listComparisonProto) {
            List<Object> comparand = Lists.newArrayList();
            for (int i = 0; i < listComparisonProto.getComparandCount(); i ++) {
                comparand.add(PlanSerialization.protoToValueObject(listComparisonProto.getComparand(i)));
            }
            return new ListComparison(Type.fromProto(serializationContext, Objects.requireNonNull(listComparisonProto.getType())),
                    comparand);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PListComparison, ListComparison> {
            @Override
            public Class<PListComparison> getProtoMessageClass() {
                return PListComparison.class;
            }

            @Override
            public ListComparison fromProto(final PlanSerializationContext serializationContext,
                                            final PListComparison listComparisonProto) {
                return ListComparison.fromProto(serializationContext, listComparisonProto);
            }
        }
    }

    /**
     * A unary predicate for special nullity checks, such as {@code NULL} and {@code NOT NULL}.
     */
    public static class NullComparison implements Comparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Null-Comparison");

        private final Type type;

        public NullComparison(Type type) {
            this.type = type;
        }

        @Nullable
        @Override
        public Boolean eval(@Nullable FDBRecordStoreBase<?> store, EvaluationContext context, @Nullable Object value) {
            if (type == Type.IS_NULL) {
                return value == null;
            } else {
                return value != null;
            }
        }

        @Override
        public void validate(Descriptors.FieldDescriptor descriptor, boolean fannedOut) {
            if (!fannedOut && descriptor.isRepeated()) {
                throw new RecordCoreException("Nullability comparison on repeated field " + descriptor.getName());
            }
        }

        @Override
        public Type getType() {
            return type;
        }

        @Override
        public Comparison withType(final Type newType) {
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

        @Override
        public Optional<Comparison> replaceValuesMaybe(final Function<Value, Optional<Value>> replacementFunction) {
            return Optional.of(this);
        }

        @Override
        public Comparison translateCorrelations(final TranslationMap translationMap,
                                                final boolean shouldSimplifyValues) {
            return this;
        }

        @Override
        public String typelessString() {
            return "NULL";
        }

        @Override
        public String toString() {
            return explain().getExplainTokens().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Override
        public ExplainTokensWithPrecedence explain() {
            return ExplainTokensWithPrecedence.of(new ExplainTokens().addKeyword(type.name()));
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
            return Objects.hash(type.name());
        }

        @Override
        public int planHash(final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return type.name().hashCode();
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, type);
                default:
                    throw new UnsupportedOperationException("Hash Kind " + mode.name() + " is not supported");
            }
        }

        @Override
        public PNullComparison toProto(final PlanSerializationContext serializationContext) {
            return PNullComparison.newBuilder().setType(type.toProto(serializationContext)).build();
        }

        @Override
        public PComparison toComparisonProto(final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setNullComparison(toProto(serializationContext)).build();
        }

        public static NullComparison fromProto(final PlanSerializationContext serializationContext,
                                               final PNullComparison nullComparisonProto) {
            return new NullComparison(Type.fromProto(serializationContext, Objects.requireNonNull(nullComparisonProto.getType())));
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PNullComparison, NullComparison> {
            @Override
            public Class<PNullComparison> getProtoMessageClass() {
                return PNullComparison.class;
            }

            @Override
            public NullComparison fromProto(final PlanSerializationContext serializationContext,
                                            final PNullComparison nullComparisonProto) {
                return NullComparison.fromProto(serializationContext, nullComparisonProto);
            }
        }
    }

    /**
     * A predicate for comparisons to things unknown or opaque to the planner. We only know it is equal to some value.
     */
    public static class OpaqueEqualityComparison implements Comparison {
        @Nullable
        @Override
        public Boolean eval(@Nullable FDBRecordStoreBase<?> store, EvaluationContext context, @Nullable Object value) {
            return false;
        }

        @Override
        public void validate(final Descriptors.FieldDescriptor descriptor, final boolean fannedOut) {
            throw new UnsupportedOperationException("comparison should not be used in a plan");
        }

        @Override
        public Type getType() {
            return Type.EQUALS;
        }

        @Override
        public Comparison withType(final Type newType) {
            return this;
        }

        @Nullable
        @Override
        public Object getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            return null;
        }

        @Override
        public Optional<Comparison> replaceValuesMaybe(final Function<Value, Optional<Value>> replacementFunction) {
            return Optional.of(this);
        }

        @Override
        public Comparison translateCorrelations(final TranslationMap translationMap,
                                                final boolean shouldSimplifyValues) {
            return this;
        }

        @Override
        public String typelessString() {
            return ":?:";
        }

        @Override
        public String toString() {
            return explain().getExplainTokens().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Override
        public ExplainTokensWithPrecedence explain() {
            return ExplainTokensWithPrecedence.of(new ExplainTokens().addKeyword(Type.EQUALS.name())
                    .addWhitespace().addIdentifier(typelessString()));
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
        public int planHash(final PlanHashMode mode) {
            throw new UnsupportedOperationException("Hash Kind " + mode.name() + " is not supported");
        }

        @Override
        public POpaqueEqualityComparison toProto(final PlanSerializationContext serializationContext) {
            return POpaqueEqualityComparison.newBuilder().build();
        }

        @Override
        public PComparison toComparisonProto(final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setOpaqueEqualityComparison(toProto(serializationContext)).build();
        }

        @SuppressWarnings("unused")
        public static OpaqueEqualityComparison fromProto(final PlanSerializationContext serializationContext,
                                                         final POpaqueEqualityComparison opaqueEqualityComparisonProto) {
            return new OpaqueEqualityComparison();
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<POpaqueEqualityComparison, OpaqueEqualityComparison> {
            @Override
            public Class<POpaqueEqualityComparison> getProtoMessageClass() {
                return POpaqueEqualityComparison.class;
            }

            @Override
            public OpaqueEqualityComparison fromProto(final PlanSerializationContext serializationContext,
                                                      final POpaqueEqualityComparison opaqueEqualityComparisonProto) {
                return OpaqueEqualityComparison.fromProto(serializationContext, opaqueEqualityComparisonProto);
            }
        }
    }

    /**
     * A text-style comparison, such as containing a given set of tokens.
     */
    public static class TextComparison implements Comparison {
        private static final TextTokenizerRegistry TOKENIZER_REGISTRY = TextTokenizerRegistryImpl.instance();
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Text-Comparison");

        private final Type type;
        @Nullable
        private final List<String> tokenList;
        @Nullable
        private final String tokenStr;
        @Nullable
        private final String tokenizerName;
        private final String fallbackTokenizerName;

        public TextComparison(Type type, String tokens, @Nullable String tokenizerName, String fallbackTokenizerName) {
            this.type = type;
            this.tokenList = null;
            this.tokenStr = tokens;
            this.tokenizerName = tokenizerName;
            this.fallbackTokenizerName = fallbackTokenizerName;
        }

        public TextComparison(Type type, List<String> tokens, @Nullable String tokenizerName, String fallbackTokenizerName) {
            this.type = type;
            this.tokenList = tokens;
            this.tokenStr = null;
            this.tokenizerName = tokenizerName;
            this.fallbackTokenizerName = fallbackTokenizerName;
        }

        @Override
        public Optional<Comparison> replaceValuesMaybe(final Function<Value, Optional<Value>> replacementFunction) {
            return Optional.of(this);
        }

        @Override
        public Comparison translateCorrelations(final TranslationMap translationMap,
                                                final boolean shouldSimplifyValues) {
            return this;
        }

        private Iterator<? extends CharSequence> tokenize(String text, TextTokenizer.TokenizerMode tokenizerMode) {
            final TextTokenizer tokenizer = TOKENIZER_REGISTRY.getTokenizer(tokenizerName == null ? fallbackTokenizerName : tokenizerName);
            return tokenizer.tokenize(text, tokenizer.getMaxVersion(), tokenizerMode);
        }

        @Nullable
        Boolean evalComparison(Iterator<? extends CharSequence> textIterator, List<String> comparand) {
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
        public Boolean eval(@Nullable FDBRecordStoreBase<?> store, EvaluationContext context, @Nullable Object value) {
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
        public void validate(Descriptors.FieldDescriptor descriptor, boolean fannedOut) {
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

        public String getFallbackTokenizerName() {
            return fallbackTokenizerName;
        }

        @Override
        public Type getType() {
            return type;
        }

        @Override
        public Comparison withType(final Type newType) {
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

        @Override
        public String typelessString() {
            final Object comparand = getComparand(null, EvaluationContext.EMPTY);
            if (comparand == null) {
                return "null";
            } else {
                return comparand.toString();
            }
        }

        @Override
        public String toString() {
            return explain().getExplainTokens().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Override
        public ExplainTokensWithPrecedence explain() {
            return ExplainTokensWithPrecedence.of(new ExplainTokens().addKeyword(type.name())
                    .addWhitespace().addIdentifier(typelessString()));
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
        @SuppressWarnings("NullAway") // PlanHashable.objectsPlanHash's varargs aren't annotated @Nullable, but each
                                       // element is hashed via objectPlanHash, which is explicitly null-safe.
        public int planHash(final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return PlanHashable.objectsPlanHash(mode, type, getComparand(), tokenizerName, fallbackTokenizerName);
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, type, getComparand(), tokenizerName, fallbackTokenizerName);
                default:
                    throw new UnsupportedOperationException("Hash Kind " + mode.name() + " is not supported");
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(type.name(), getComparand(), tokenizerName, fallbackTokenizerName);
        }

        @Override
        public Message toProto(final PlanSerializationContext serializationContext) {
            throw new RecordCoreException("serialization of comparison of this kind is not supported");
        }

        @Override
        public PComparison toComparisonProto(final PlanSerializationContext serializationContext) {
            throw new RecordCoreException("serialization of comparison of this kind is not supported");
        }
    }

    /**
     * A {@link TextComparison} that must be satisfied within a certain number of text tokens.
     */
    public static class TextWithMaxDistanceComparison extends TextComparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Text-With-Max-Distance-Comparison");

        private final int maxDistance;

        public TextWithMaxDistanceComparison(String tokens, int maxDistance, @Nullable String tokenizerName, String fallbackTokenizerName) {
            super(Type.TEXT_CONTAINS_ALL_WITHIN, tokens, tokenizerName, fallbackTokenizerName);
            this.maxDistance = maxDistance;
        }

        public TextWithMaxDistanceComparison(List<String> tokens, int maxDistance, @Nullable String tokenizerName, String fallbackTokenizerName) {
            super(Type.TEXT_CONTAINS_ALL_WITHIN, tokens, tokenizerName, fallbackTokenizerName);
            this.maxDistance = maxDistance;
        }

        @Nullable
        @Override
        Boolean evalComparison(Iterator<? extends CharSequence> textIterator, List<String> comparand) {
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
        public int planHash(final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return super.planHash(mode) * 31 + maxDistance;
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, super.planHash(mode), maxDistance);
                default:
                    throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
            }
        }

        @Override
        public int hashCode() {
            return super.hashCode() * 31 + maxDistance;
        }

        @Override
        public ExplainTokensWithPrecedence explain() {
            return ExplainTokensWithPrecedence.of(new ExplainTokens().addKeyword(getType().name())
                    .addOptionalWhitespace().addOpeningParen().addOptionalWhitespace().addToString(maxDistance)
                    .addOptionalWhitespace().addClosingParen().addWhitespace().addIdentifier(typelessString()));
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

        public TextContainsAllPrefixesComparison(String tokenPrefixes, boolean strict, @Nullable String tokenizerName, String fallbackTokenizerName) {
            this(tokenPrefixes, strict, ProbableIntersectionCursor.DEFAULT_EXPECTED_RESULTS, ProbableIntersectionCursor.DEFAULT_FALSE_POSITIVE_PERCENTAGE, tokenizerName, fallbackTokenizerName);
        }

        public TextContainsAllPrefixesComparison(String tokenPrefixes, boolean strict, long expectedRecords, double falsePositivePercentage,
                                                 @Nullable String tokenizerName, String fallbackTokenizerName) {
            super(Type.TEXT_CONTAINS_ALL_PREFIXES, tokenPrefixes, tokenizerName, fallbackTokenizerName);
            this.strict = strict;
            this.expectedRecords = expectedRecords;
            this.falsePositivePercentage = falsePositivePercentage;
        }

        public TextContainsAllPrefixesComparison(List<String> tokenPrefixes, boolean strict, @Nullable String tokenizerName, String fallbackTokenizerName) {
            this(tokenPrefixes, strict, ProbableIntersectionCursor.DEFAULT_EXPECTED_RESULTS, ProbableIntersectionCursor.DEFAULT_FALSE_POSITIVE_PERCENTAGE,
                    tokenizerName, fallbackTokenizerName);
        }

        public TextContainsAllPrefixesComparison(List<String> tokenPrefixes, boolean strict, long expectedRecords, double falsePositivePercentage,
                                                 @Nullable String tokenizerName, String fallbackTokenizerName) {
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
        public int planHash(final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return super.planHash(mode) * (strict ? -1 : 1);
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, super.planHash(mode), strict);
                default:
                    throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
            }
        }

        @Override
        public int hashCode() {
            return super.hashCode() * (strict ? -1 : 1);
        }

        @Override
        public ExplainTokensWithPrecedence explain() {
            final var resultExplainTokens =
                    new ExplainTokens().addKeyword(getType().name())
                            .addOptionalWhitespace().addOpeningParen().addOpeningParen();
            if (strict) {
                resultExplainTokens.addKeyword("STRICTLY");
            } else {
                resultExplainTokens.addKeyword("APPROXIMATELY");
            }
            resultExplainTokens.addOptionalWhitespace().addClosingParen().addWhitespace()
                    .addIdentifier(typelessString());
            return ExplainTokensWithPrecedence.of(resultExplainTokens);
        }
    }

    /**
     * Comparison wrapping another one and answering {@code true} to {@link #hasMultiColumnComparand}.
     */
    public static class MultiColumnComparison implements Comparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Multi-Column-Comparison");

        private final Comparison inner;

        public MultiColumnComparison(final Comparison inner) {
            this.inner = inner;
        }

        @Nullable
        @Override
        public Boolean eval(@Nullable final FDBRecordStoreBase<?> store, final EvaluationContext context, @Nullable final Object value) {
            return inner.eval(store, context, value);
        }

        @Override
        public void validate(final Descriptors.FieldDescriptor descriptor, final boolean fannedOut) {
            inner.validate(descriptor, fannedOut);
        }

        @Override
        public Type getType() {
            return inner.getType();
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Comparison withType(final Type newType) {
            final var newInner = inner.withType(newType);
            if (newInner == inner) {
                return this;
            }
            return new MultiColumnComparison(newInner);
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Comparison withValue(final Value value) {
            final var newInner = inner.withValue(value);
            if (newInner == inner) {
                return this;
            }
            return new MultiColumnComparison(newInner);
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Optional<Comparison> replaceValuesMaybe(final Function<Value, Optional<Value>> replacementFunction) {
            return inner.replaceValuesMaybe(replacementFunction)
                    .map(replacedInner -> {
                        if (replacedInner == inner) {
                            return this;
                        }
                        return new MultiColumnComparison(replacedInner);
                    });
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Comparison translateCorrelations(final TranslationMap translationMap,
                                                final boolean shouldSimplifyValues) {
            final var translatedInner = inner.translateCorrelations(translationMap, shouldSimplifyValues);
            if (inner == translatedInner) {
                return this;
            } else {
                return new MultiColumnComparison(translatedInner);
            }
        }

        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            return inner.getCorrelatedTo();
        }

        @Override
        public ConstrainedBoolean semanticEqualsTyped(final Comparison other, final ValueEquivalence valueEquivalence) {
            MultiColumnComparison that = (MultiColumnComparison)other;
            return this.inner.semanticEquals(that.inner, valueEquivalence);
        }

        @Override
        public int planHash(final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return inner.planHash(mode);
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, inner);
                default:
                    throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
            }
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
            return semanticEquals(o, AliasMap.emptyMap());
        }

        @Override
        public String toString() {
            return explain().getExplainTokens().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Override
        public ExplainTokensWithPrecedence explain() {
            return inner.explain();
        }

        @Override
        public PMultiColumnComparison toProto(final PlanSerializationContext serializationContext) {
            return PMultiColumnComparison.newBuilder()
                    .setInner(inner.toComparisonProto(serializationContext))
                    .build();
        }

        @Override
        public PComparison toComparisonProto(final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setMultiColumnComparison(toProto(serializationContext)).build();
        }

        public static MultiColumnComparison fromProto(final PlanSerializationContext serializationContext,
                                                      final PMultiColumnComparison multiColumnComparisonProto) {
            return new MultiColumnComparison(Comparison.fromComparisonProto(serializationContext,
                    Objects.requireNonNull(multiColumnComparisonProto.getInner())));
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PMultiColumnComparison, MultiColumnComparison> {
            @Override
            public Class<PMultiColumnComparison> getProtoMessageClass() {
                return PMultiColumnComparison.class;
            }

            @Override
            public MultiColumnComparison fromProto(final PlanSerializationContext serializationContext,
                                                   final PMultiColumnComparison multiColumnComparisonProto) {
                return MultiColumnComparison.fromProto(serializationContext, multiColumnComparisonProto);
            }
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
        private final InvertibleFunctionKeyExpression function;
        private final Comparison originalComparison;
        private final Type type;

        private InvertedFunctionComparison(InvertibleFunctionKeyExpression function,
                                           Comparison originalComparison,
                                           Type type) {
            this.function = function;
            this.originalComparison = originalComparison;
            this.type = type;
        }

        @Override
        public int planHash(final PlanHashMode mode) {
            return PlanHashable.planHash(mode, function, originalComparison);
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
        public Boolean eval(@Nullable final FDBRecordStoreBase<?> store, final EvaluationContext context, @Nullable final Object value) {
            Object comparand = getComparand(store, context);
            return evalComparison(type, value, comparand);
        }

        @Override
        public void validate(final Descriptors.FieldDescriptor descriptor, final boolean fannedOut) {
            originalComparison.validate(descriptor, fannedOut);
        }

        @Override
        public Type getType() {
            return type;
        }

        @Override
        public Comparison withType(final Type newType) {
            return from(function, originalComparison.withType(newType));
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public Comparison withValue(final Value value) {
            final var newComparison = originalComparison.withValue(value);
            if (newComparison == originalComparison) {
                return this;
            }
            return from(function, newComparison);
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
                    // getSingletonPreImage legitimately returns @Nullable, but java.util.function.Function
                    // (the target type of the method-reference here) isn't nullness-aware, so its R is
                    // inferred @NonNull regardless; this Stream.map cannot be annotated around that.
                    @SuppressWarnings("NullAway")
                    final List<Object> mapped = inverse.stream()
                            .map(this::getSingletonPreImage)
                            .collect(Collectors.toList());
                    finalValues.addAll(mapped);
                }
                return finalValues;
            } else {
                Key.Evaluated evaluated = Key.Evaluated.scalar(originalComparandValue);
                List<Key.Evaluated> inverse = function.evaluateInverse(evaluated);
                if (getType() == Type.IN) {
                    // See the @SuppressWarnings comment above: same Function<T, R> nullability-inference
                    // limitation applies to this method reference.
                    @SuppressWarnings("NullAway")
                    final List<Object> mapped = inverse.stream()
                            .map(this::getSingletonPreImage)
                            .collect(Collectors.toList());
                    return mapped;
                } else {
                    Key.Evaluated preImage = inverse.get(0);
                    return getSingletonPreImage(preImage);
                }
            }
        }

        @Nullable
        private Object getSingletonPreImage(Key.Evaluated preImage) {
            if (preImage.size() != 1) {
                throw new RecordCoreException("unable to get singleton pre-image for function")
                        .addLogInfo(LogMessageKeys.FUNCTION, function.getName());
            }
            return preImage.getObject(0);
        }

        @Override
        public String typelessString() {
            return function.getName() + "^-1(" + originalComparison.typelessString() + ")";
        }

        @Override
        public String toString() {
            return explain().getExplainTokens().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Override
        public ExplainTokensWithPrecedence explain() {
            return ExplainTokensWithPrecedence.of(new ExplainTokens().addKeyword(type.name())
                    .addWhitespace().addIdentifier(typelessString()));
        }

        @Override
        @SuppressWarnings({"PMD.CompareObjectsWithEquals"}) // used here for referential equality
        public Optional<Comparison> replaceValuesMaybe(final Function<Value, Optional<Value>> replacementFunction) {
            return originalComparison.replaceValuesMaybe(replacementFunction)
                    .map(translatedOriginalComparison -> {
                        if (translatedOriginalComparison == originalComparison) {
                            return this;
                        }
                        return new InvertedFunctionComparison(function, translatedOriginalComparison, type);
                    });
        }

        @Override
        @SuppressWarnings({"PMD.CompareObjectsWithEquals"}) // used here for referential equality
        public Comparison translateCorrelations(final TranslationMap translationMap,
                                                final boolean shouldSimplifyValues) {
            Comparison translated = originalComparison.translateCorrelations(translationMap, shouldSimplifyValues);
            if (translated == originalComparison) {
                return this;
            } else {
                return new InvertedFunctionComparison(function, translated, type);
            }
        }

        @Override
        public PInvertedFunctionComparison toProto(final PlanSerializationContext serializationContext) {
            return PInvertedFunctionComparison.newBuilder()
                    .setFunction(function.toProto())
                    .setOriginalComparison(originalComparison.toComparisonProto(serializationContext))
                    .setType(type.toProto(serializationContext))
                    .build();
        }

        @Override
        public PComparison toComparisonProto(final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setInvertedFunctionComparison(toProto(serializationContext)).build();
        }

        public static InvertedFunctionComparison fromProto(final PlanSerializationContext serializationContext,
                                                           final PInvertedFunctionComparison invertedFunctionComparisonProto) {
            return new InvertedFunctionComparison((InvertibleFunctionKeyExpression)InvertibleFunctionKeyExpression.fromProto(Objects.requireNonNull(invertedFunctionComparisonProto.getFunction())),
                    Comparison.fromComparisonProto(serializationContext, Objects.requireNonNull(invertedFunctionComparisonProto.getOriginalComparison())),
                    Type.fromProto(serializationContext, Objects.requireNonNull(invertedFunctionComparisonProto.getType())));
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
        public static InvertedFunctionComparison from(InvertibleFunctionKeyExpression function,
                                                      Comparison originalComparison) {
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

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PInvertedFunctionComparison, InvertedFunctionComparison> {
            @Override
            public Class<PInvertedFunctionComparison> getProtoMessageClass() {
                return PInvertedFunctionComparison.class;
            }

            @Override
            public InvertedFunctionComparison fromProto(final PlanSerializationContext serializationContext,
                                                        final PInvertedFunctionComparison invertedFunctionComparisonProto) {
                return InvertedFunctionComparison.fromProto(serializationContext, invertedFunctionComparisonProto);
            }
        }
    }
}
