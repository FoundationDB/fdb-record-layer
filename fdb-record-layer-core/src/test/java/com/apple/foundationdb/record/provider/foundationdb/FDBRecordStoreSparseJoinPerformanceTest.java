/*
 * FDBRecordStoreSparseJoinPerformanceTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MappedKeyValue;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestSparseJoinPerfProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.Tags;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Performance tests of a "sparse join", that is, a nested loop join where many entries returned by the
 * first half of the join require looking up another record in a second index which may or may not have
 * any entries.
 *
 * <p>
 * To run, do the following:
 * </p>
 *
 * <ol>
 *     <li>Run the {@link #repopulateForTest()} test to insert records.</li>
 *     <li>The other tests can then be run to investigate certain scenarios, querying from that dataset.</li>
 * </ol>
 *
 * <p>
 * Certain testing parameters, like the number of {@code OuterRecord}s, can be adjusted, though the dataset
 * will need to be repopulated in order to pick up those changes.
 * </p>
 */
@Tag(Tags.RequiresFDB)
@Tag(Tags.Performance)
public class FDBRecordStoreSparseJoinPerformanceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBRecordStoreSparseJoinPerformanceTest.class);

    private static final Object[] PATH = { "record-test", "performance", "join"};

    private static final String OUTER_RECORD = TestSparseJoinPerfProto.OuterRecord.getDescriptor().getName();
    private static final String INNER_RECORD = TestSparseJoinPerfProto.InnerRecord.getDescriptor().getName();

    private static final String TEXT_INDEX_NAME = "textIndex";
    private static final Object TEXT_INDEX_SUBSPACE_KEY = 1L;
    private static final String GROUP_OUTER_VALUE_INDEX = "groupOuterValueIndex";
    private static final Object GROUP_OUTER_VALUE_SUBSPACE_KEY = 2L;

    private static final String TEXT_FIELD = "text";
    private static final String GROUP_FIELD = "group";
    private static final String VAL_FIELD = "val";
    private static final String OUTER_ID_FIELD = "outer_id";
    private static final String REC_NO_FIELD = "rec_no";

    private static final String TEXT_PARAM = "text_param";
    private static final String GROUP_PARAM = "group_param";
    private static final String OUTER_PARAM = "outer_param";

    private static final PipelineOperation JOIN = new PipelineOperation("NESTED_LOOP_JOIN");

    private static final List<String> TEXTS = List.of("a", "b", "c", "d", "e", "f", "g");
    private static final int OUTER_RECORD_COUNT = 100_000;
    private static final int GROUP_COUNT = 10;

    private FDBDatabase database;

    private static RecordMetaData createMetaData() {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder()
                .setRecords(TestSparseJoinPerfProto.getDescriptor());

        Index textIndex = new Index(TEXT_INDEX_NAME, Key.Expressions.field(TEXT_FIELD));
        textIndex.setSubspaceKey(TEXT_INDEX_SUBSPACE_KEY);
        builder.addIndex(OUTER_RECORD, textIndex);

        Index groupOuterValIndex = new Index(GROUP_OUTER_VALUE_INDEX, Key.Expressions.concatenateFields(GROUP_FIELD, OUTER_ID_FIELD, VAL_FIELD));
        groupOuterValIndex.setSubspaceKey(GROUP_OUTER_VALUE_SUBSPACE_KEY);
        builder.addIndex(INNER_RECORD, groupOuterValIndex);

        return builder.build();
    }

    private static RecordQuery outerQuery() {
        // Roughly:
        //   SELECT rec_no FROM OuterRecord WHERE text = $text_param
        return RecordQuery.newBuilder()
                .setRecordType(OUTER_RECORD)
                .setFilter(Query.field(TEXT_FIELD).equalsParameter(TEXT_PARAM))
                .setRequiredResults(List.of(Key.Expressions.field(REC_NO_FIELD)))
                .build();
    }

    private static RecordQuery innerQuery() {
        // Roughly:
        //   SELECT val FROM InnerRecord WHERE group = $group_param AND outer_id = $outer_param
        return RecordQuery.newBuilder()
                .setRecordType(INNER_RECORD)
                .setFilter(Query.and(Query.field(GROUP_FIELD).equalsParameter(GROUP_PARAM), Query.field(OUTER_ID_FIELD).equalsParameter(OUTER_PARAM)))
                .setRequiredResults(List.of(Key.Expressions.field(VAL_FIELD)))
                .build();
    }

    private Map<Long, List<TestSparseJoinPerfProto.InnerRecord>> getInnerByOuterForGroup(int group) {
        Map<Long, List<TestSparseJoinPerfProto.InnerRecord>> innerRecordsByOuter = new HashMap<>();
        RecordCursorContinuation continuation = RecordCursorStartContinuation.START;

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType(INNER_RECORD)
                .setFilter(Query.field(GROUP_FIELD).equalsValue((long)group))
                .build();

        do {
            try (FDBRecordContext context = database.openContext()) {
                ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                        .setTimeLimit(3000L)
                        .build();
                FDBRecordStore recordStore = openStore(context);
                try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(query, continuation.toBytes(), executeProperties)) {
                    RecordCursorResult<FDBQueriedRecord<Message>> result;
                    while ((result = cursor.getNext()).hasNext()) {
                        TestSparseJoinPerfProto.InnerRecord innerRecord = TestSparseJoinPerfProto.InnerRecord.newBuilder()
                                .mergeFrom(result.get().getRecord())
                                .build();
                        long outerId = innerRecord.getOuterId();
                        innerRecordsByOuter.compute(outerId, (ignore, existing) -> {
                            List<TestSparseJoinPerfProto.InnerRecord> newList = existing == null ? new ArrayList<>() : existing;
                            newList.add(innerRecord);
                            return newList;
                        });
                    }
                    continuation = result.getContinuation();
                }
            }
        } while (!continuation.isEnd());

        return innerRecordsByOuter;
    }

    private Set<Long> getOuterRecordIdsByGroup(int group) {
        return getInnerByOuterForGroup(group).keySet();
    }

    private BitSet getOuterRecordBitSetByGroup(int group) {
        Set<Long> outerIdsByGroup = getOuterRecordIdsByGroup(group);
        BitSet bitSet = new BitSet();
        for (Long outerId : outerIdsByGroup) {
            bitSet.set(outerId.intValue());
        }
        return bitSet;
    }

    private BloomFilter<Long> getOuterRecordBloomFilterByGroup(int group) {
        Set<Long> outerIdsByGroup = getOuterRecordIdsByGroup(group);
        BloomFilter<Long> filter = BloomFilter.create(Funnels.longFunnel(), outerIdsByGroup.size());
        for (Long outerId : outerIdsByGroup) {
            filter.put(outerId);
        }
        return filter;
    }

    public enum JoinTechniqueType {
        STANDARD,
        PREFETCH,
        OVERSCAN,
        SCAN_WIDER,
        LOAD_AND_CACHE,
        IN_MEMORY_HASH_JOIN,
        SET_FILTER,
        BIT_SET_FILTER,
        BLOOM_FILTER,
        REMOTE_FETCH,
    }

    public JoinTechnique getJoinTechnique(JoinTechniqueType type, int group) {
        switch (type) {
            case STANDARD:
                return new StandardIndexProbeJoinTechnique();
            case PREFETCH:
                return new PreFetchedIndexProbeJoinTechnique(group, 100);
            case OVERSCAN:
                return new OverScanIndexProbeJoinTechnique();
            case SCAN_WIDER:
                return new ScanWiderIndexRangeJoinTechnique();
            case LOAD_AND_CACHE:
                return new LoadAndCachePageJoinTechnique();
            case IN_MEMORY_HASH_JOIN:
                return new InMemoryHashJoinTechnique(getInnerByOuterForGroup(group));
            case SET_FILTER:
                return new IdSetHashJoinTechnique(getOuterRecordIdsByGroup(group));
            case BIT_SET_FILTER:
                return new BitSetHashJoinTechnique(getOuterRecordBitSetByGroup(group));
            case BLOOM_FILTER:
                return new BloomFilterHashJoinTechnique(getOuterRecordBloomFilterByGroup(group));
            case REMOTE_FETCH:
                return new DummyRemoteFetchJoinTechnique();
            default:
                throw new RecordCoreArgumentException("Unrecognized join technique type", "type", type);
        }
    }

    public interface JoinTechnique {
        JoinTechniqueType getType();
        Joiner createJoiner(FDBRecordStore store);
    }

    public interface Joiner {
        RecordCursor<QueryResult> executeInner(EvaluationContext innerContext, @Nullable byte[] continuation, ExecuteProperties executeProperties);
    }

    /**
     * Join technique that executes the inner loop of a nested loop join via an index probe. This is the "standard" way
     * of attempting to do the operation.
     */
    private static class StandardIndexProbeJoinTechnique implements JoinTechnique {
        @Override
        public JoinTechniqueType getType() {
            return JoinTechniqueType.STANDARD;
        }

        @Override
        public Joiner createJoiner(final FDBRecordStore store) {
            final RecordQuery inner = innerQuery();
            final RecordQueryPlan innerPlan = store.planQuery(inner);
            assertThat(innerPlan, coveringIndexScan(indexScan(allOf(indexName(GROUP_OUTER_VALUE_INDEX), bounds(hasTupleString("[EQUALS $" + GROUP_PARAM + ", EQUALS $" + OUTER_PARAM + "]"))))));
            return new StandardIndexProbeJoiner(store, innerPlan);
        }

        public static class StandardIndexProbeJoiner implements Joiner {
            private final FDBRecordStore store;
            private final RecordQueryPlan plan;

            public StandardIndexProbeJoiner(FDBRecordStore store, RecordQueryPlan plan) {
                this.store = store;
                this.plan = plan;
            }

            @Override
            public RecordCursor<QueryResult> executeInner(EvaluationContext innerContext, @Nullable byte[] continuation, ExecuteProperties executeProperties) {
                return store.executeQuery(plan, continuation, innerContext, executeProperties);
            }
        }
    }

    private static class PreFetchedIndexProbeJoinTechnique implements JoinTechnique {
        private final int group;
        private final int prefetchLimit;

        public PreFetchedIndexProbeJoinTechnique(int group, int prefetchLimit) {
            this.group = group;
            this.prefetchLimit = prefetchLimit;
        }

        @Override
        public JoinTechniqueType getType() {
            return JoinTechniqueType.PREFETCH;
        }

        @Override
        public Joiner createJoiner(final FDBRecordStore store) {
            // Fire-and-forget a limited index scan of the group to try and get the values for the group into
            // the RYW cache
            Index index = store.getRecordMetaData().getIndex(GROUP_OUTER_VALUE_INDEX);
            ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                    .setReturnedRowLimit(prefetchLimit)
                    .setFailOnScanLimitReached(false)
                    .build();
            try (RecordCursor<IndexEntry> indexPrefetchCursor = store.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from(group)), null, executeProperties.asScanProperties(false))) {
                AsyncUtil.whileTrue(() -> {
                    if (store.getRecordContext().isClosed()) {
                        return AsyncUtil.READY_FALSE;
                    }
                    // Consume results until we run out
                    return indexPrefetchCursor.onNext().thenApply(RecordCursorResult::hasNext);
                }, store.getExecutor()).join();
            }

            // Return the same join strategy as if we didn't have prefetch enabled.
            final RecordQueryPlan innerPlan = store.planQuery(innerQuery());
            return new StandardIndexProbeJoinTechnique.StandardIndexProbeJoiner(store, innerPlan);
        }
    }

    private static class OverScanIndexProbeJoinTechnique implements JoinTechnique {
        private static final StandardIndexProbeJoinTechnique standardJoinTechnique = new StandardIndexProbeJoinTechnique();

        @Override
        public JoinTechniqueType getType() {
            return JoinTechniqueType.OVERSCAN;
        }

        @Override
        public Joiner createJoiner(final FDBRecordStore store) {
            return new OverScanIndexProbeJoiner(standardJoinTechnique.createJoiner(store));
        }

        public static class OverScanIndexProbeJoiner implements Joiner {
            private final Joiner standardJoiner;

            public OverScanIndexProbeJoiner(Joiner standardJoiner) {
                this.standardJoiner = standardJoiner;
            }

            @Override
            public RecordCursor<QueryResult> executeInner(final EvaluationContext innerContext, @Nullable final byte[] continuation, final ExecuteProperties executeProperties) {
                ExecuteProperties execPropsWithOverScan = executeProperties.setOverScanForCache(true);
                return standardJoiner.executeInner(innerContext, continuation, execPropsWithOverScan);
            }
        }
    }

    private static class ScanWiderIndexRangeJoinTechnique implements JoinTechnique {
        @Override
        public JoinTechniqueType getType() {
            return JoinTechniqueType.SCAN_WIDER;
        }

        @Override
        public Joiner createJoiner(final FDBRecordStore store) {
            return new ScanWiderIndexRangeJoiner(store);
        }

        private static class ScanWiderIndexRangeJoiner implements Joiner {
            private final FDBRecordStore store;
            private final Index index;

            public ScanWiderIndexRangeJoiner(FDBRecordStore store) {
                this.store = store;
                this.index = store.getRecordMetaData().getIndex(GROUP_OUTER_VALUE_INDEX);
            }

            @Override
            public RecordCursor<QueryResult> executeInner(final EvaluationContext innerContext, @Nullable final byte[] continuation, final ExecuteProperties executeProperties) {
                Object group = innerContext.getBinding(GROUP_PARAM);
                Object outerId = innerContext.getBinding(OUTER_PARAM);

                TupleRange range = new TupleRange(Tuple.from(group, outerId), Tuple.from(group), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE);
                return store.scanIndex(index, IndexScanType.BY_VALUE, range, continuation, executeProperties.asScanProperties(false))
                        .mapResult(result -> {
                            if (result.hasNext()) {
                                IndexEntry entry = result.get();
                                if (!Objects.equals(entry.getKey().get(1), outerId)) {
                                    return RecordCursorResult.exhausted();
                                }
                            }
                            return result;
                        })
                        .map(indexEntry -> {
                            Tuple key = indexEntry.getKey();
                            TestSparseJoinPerfProto.InnerRecord msg = TestSparseJoinPerfProto.InnerRecord.newBuilder()
                                    .setGroup(key.getLong(0))
                                    .setOuterId(key.getLong(1))
                                    .setVal((int) key.getLong(2))
                                    .setRecNo(key.getLong(3))
                                    .build();
                            return QueryResult.ofComputed(msg);
                        });
            }
        }
    }

    private static class LoadAndCachePageJoinTechnique implements JoinTechnique {

        @Override
        public JoinTechniqueType getType() {
            return JoinTechniqueType.LOAD_AND_CACHE;
        }

        @Override
        public Joiner createJoiner(final FDBRecordStore store) {
            return new LoadAndCacheJoiner(store);
        }

        private static class LoadAndCacheJoiner implements Joiner {
            private final NavigableMap<Tuple, Tuple> cachedValues;
            private final RangeSet<Tuple> cachedRanges;

            private final FDBRecordStore store;
            private final Index index;

            public LoadAndCacheJoiner(FDBRecordStore store) {
                this.store = store;
                this.index = store.getRecordMetaData().getIndex(GROUP_OUTER_VALUE_INDEX);
                this.cachedValues = new ConcurrentSkipListMap<>();
                this.cachedRanges = TreeRangeSet.create();
            }

            @Override
            public RecordCursor<QueryResult> executeInner(final EvaluationContext innerContext, @Nullable final byte[] continuation, final ExecuteProperties executeProperties) {
                Object groupId = innerContext.getBinding(GROUP_PARAM);
                long outerId = (long) innerContext.getBinding(OUTER_PARAM);
                Tuple prefix = Tuple.from(groupId, outerId);
                Tuple outside = Tuple.from(groupId, outerId + 1);
                if (cachedRanges.rangeContaining(prefix) != null) {
                    return RecordCursor.fromIterator(store.getExecutor(), cachedValues.subMap(prefix, outside).entrySet().iterator())
                            .map(entry -> toQueryResult(entry.getKey()));
                } else {
                    return store.scanIndex(index, IndexScanType.BY_VALUE, new TupleRange(prefix, Tuple.from(groupId), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation, executeProperties.asScanProperties(false))
                            .mapResult(result -> {
                                if (result.hasNext()) {
                                    IndexEntry indexEntry = result.get();
                                    cachedValues.put(indexEntry.getKey(), indexEntry.getValue());
                                    if (!TupleHelpers.isPrefix(prefix, indexEntry.getKey())) {
                                        cachedRanges.add(Range.closed(prefix, indexEntry.getKey()));
                                        return RecordCursorResult.exhausted();
                                    }
                                    return RecordCursorResult.withNextValue(toQueryResult(indexEntry.getKey()), result.getContinuation());
                                } else if (result.getNoNextReason().isSourceExhausted()) {
                                    cachedRanges.add(Range.greaterThan(prefix));
                                }
                                return RecordCursorResult.withoutNextValue(result);
                            });
                }
            }

            private QueryResult toQueryResult(@Nonnull Tuple key) {
                TestSparseJoinPerfProto.InnerRecord innerRecord = TestSparseJoinPerfProto.InnerRecord.newBuilder()
                        .setGroup(key.getLong(0))
                        .setOuterId(key.getLong(1))
                        .setVal((int) key.getLong(2))
                        .setRecNo(key.getLong(3))
                        .build();
                return QueryResult.ofComputed(innerRecord);
            }
        }
    }

    /**
     * Simulate an entirely in-memory hash join. This is in some sense for getting a baseline measurement, as this
     * assumes we'd be able to get the full
     */
    private static class InMemoryHashJoinTechnique implements JoinTechnique {
        private final Map<Long, List<TestSparseJoinPerfProto.InnerRecord>> innerRecordsByOuter;

        public InMemoryHashJoinTechnique(Map<Long, List<TestSparseJoinPerfProto.InnerRecord>> innerRecordsByOuter) {
            this.innerRecordsByOuter = innerRecordsByOuter;
        }

        @Override
        public JoinTechniqueType getType() {
            return JoinTechniqueType.IN_MEMORY_HASH_JOIN;
        }

        @Override
        public Joiner createJoiner(final FDBRecordStore store) {
            return new InMemoryHashJoiner(store.getExecutor());
        }

        private class InMemoryHashJoiner implements Joiner {
            private final Executor executor;

            private InMemoryHashJoiner(Executor executor) {
                this.executor = executor;
            }

            @Override
            public RecordCursor<QueryResult> executeInner(final EvaluationContext innerContext, @Nullable final byte[] continuation, final ExecuteProperties executeProperties) {
                Long outerId = (Long) innerContext.getBinding(OUTER_PARAM);
                List<TestSparseJoinPerfProto.InnerRecord> innerRecords = innerRecordsByOuter.get(outerId);
                if (innerRecords == null) {
                    return RecordCursor.empty();
                } else {
                    return RecordCursor.fromList(executor, innerRecords, continuation)
                            .map(QueryResult::ofComputed);
                }
            }
        }
    }

    /**
     * Abstract join technique for join strategies that attempt to optimize execution by filtering out some
     * of the values before executing the
     */
    private static abstract class AbstractPreFilterHashJoinTechnique implements JoinTechnique {
        abstract Predicate<EvaluationContext> getFilter();

        @Override
        public Joiner createJoiner(final FDBRecordStore store) {
            return new AbstractPreFilterHashJoiner(store, store.planQuery(innerQuery()), getFilter());
        }

        private static class AbstractPreFilterHashJoiner extends StandardIndexProbeJoinTechnique.StandardIndexProbeJoiner {
            private final Predicate<EvaluationContext> filter;

            public AbstractPreFilterHashJoiner(final FDBRecordStore store, final RecordQueryPlan plan, Predicate<EvaluationContext> filter) {
                super(store, plan);
                this.filter = filter;
            }

            @Override
            public RecordCursor<QueryResult> executeInner(final EvaluationContext innerContext, @Nullable final byte[] continuation, final ExecuteProperties executeProperties) {
                if (filter.test(innerContext)) {
                    return super.executeInner(innerContext, continuation, executeProperties);
                } else {
                    return RecordCursor.empty();
                }
            }
        }
    }

    private static class IdSetHashJoinTechnique extends AbstractPreFilterHashJoinTechnique {
        private final Set<Long> outerIdsInGroup;

        public IdSetHashJoinTechnique(Set<Long> outerIdsInGroup) {
            this.outerIdsInGroup = outerIdsInGroup;
        }

        @Override
        public JoinTechniqueType getType() {
            return JoinTechniqueType.SET_FILTER;
        }

        @Override
        Predicate<EvaluationContext> getFilter() {
            return evaluationContext -> {
                Long outerId = (Long) evaluationContext.getBinding(OUTER_PARAM);
                return outerIdsInGroup.contains(outerId);
            };
        }
    }

    private static class BitSetHashJoinTechnique extends AbstractPreFilterHashJoinTechnique {
        private final BitSet outerIdsInGroup;

        public BitSetHashJoinTechnique(BitSet outerIdsInGroup) {
            this.outerIdsInGroup = outerIdsInGroup;
        }

        @Override
        public JoinTechniqueType getType() {
            return JoinTechniqueType.BIT_SET_FILTER;
        }

        @Override
        Predicate<EvaluationContext> getFilter() {
            return evaluationContext -> {
                Long outerId = (Long) evaluationContext.getBinding(OUTER_PARAM);
                return outerIdsInGroup.get(outerId.intValue());
            };
        }
    }

    private static class BloomFilterHashJoinTechnique extends AbstractPreFilterHashJoinTechnique {
        private final BloomFilter<Long> bloomFilter;

        public BloomFilterHashJoinTechnique(BloomFilter<Long> bloomFilter) {
            this.bloomFilter = bloomFilter;
        }

        @Override
        public JoinTechniqueType getType() {
            return JoinTechniqueType.BLOOM_FILTER;
        }

        @Override
        Predicate<EvaluationContext> getFilter() {
            return evaluationContext -> {
                Long outerId = (Long) evaluationContext.getBinding(OUTER_PARAM);
                return bloomFilter.mightContain(outerId);
            };
        }
    }

    private static class DummyRemoteFetchJoinTechnique implements JoinTechnique {
        @Override
        public JoinTechniqueType getType() {
            return JoinTechniqueType.REMOTE_FETCH;
        }

        @Override
        public Joiner createJoiner(final FDBRecordStore store) {
            return null;
        }

        public RecordCursor<Integer> executeRemoteFetch(FDBRecordStore store, int group, String text, @Nullable byte[] continuation, ExecuteProperties executeProperties) {
            final Transaction tr = store.getRecordContext().ensureActive();

            Index textIndex = store.getRecordMetaData().getIndex(TEXT_INDEX_NAME);
            final Subspace scanSubspace = store.indexSubspace(textIndex).subspace(Tuple.from(text));
            final com.apple.foundationdb.Range range = scanSubspace.range();
            int scanSubspaceSize = Tuple.fromBytes(scanSubspace.pack()).size();

            Index groupIndex = store.getRecordMetaData().getIndex(GROUP_OUTER_VALUE_INDEX);
            final Subspace mapSubspace = store.indexSubspace(groupIndex).subspace(Tuple.from(group));
            final byte[] mapper = mapSubspace.pack(Tuple.from("{K[" + scanSubspaceSize + "]}", "{...}"));
            final int scanLimit = executeProperties.getScannedRecordsLimit();
            final int configuredScanLimit = 100; // scanLimit == 0 || scanLimit == Integer.MAX_VALUE ? 100 : scanLimit;
            final AsyncIterable<MappedKeyValue> iterable = tr.getMappedRange(
                    continuation == null ? KeySelector.firstGreaterOrEqual(range.begin) : KeySelector.firstGreaterThan(continuation),
                    KeySelector.firstGreaterOrEqual(range.end),
                    mapper,
                    configuredScanLimit,
                    false,
                    StreamingMode.WANT_ALL);
            final AsyncIterator<MappedKeyValue> iterator = iterable.iterator();
            CursorLimitManager cursorLimitManager = new CursorLimitManager(executeProperties.asScanProperties(false));

            return new RecordCursor<Integer>() {
                private RecordCursorResult<Integer> lastResult;
                private byte[] lastKey;
                private int scanned;

                @Nonnull
                @Override
                public CompletableFuture<RecordCursorResult<Integer>> onNext() {
                    if (lastResult != null) {
                        return CompletableFuture.completedFuture(lastResult);
                    }
                    return iterator.onHasNext().thenApply(hasNext -> {
                        if (hasNext) {
                            MappedKeyValue mappedKeyValue = iterator.next();
                            cursorLimitManager.reportScannedBytes(mappedKeyValue.getKey().length + mappedKeyValue.getValue().length + mappedKeyValue.getRangeResult().stream().mapToInt(kv -> kv.getKey().length + kv.getValue().length).sum());
                            RecordCursorContinuation nextContinuation = ByteArrayContinuation.fromNullable(mappedKeyValue.getKey());
                            if (cursorLimitManager.tryRecordScan() || lastKey == null) {
                                List<KeyValue> results = mappedKeyValue.getRangeResult();
                                lastKey = mappedKeyValue.getKey();
                                scanned++;

                                if (!results.isEmpty()) {
                                    KeyValue childKey = results.get(0);
                                    int result = (int) mapSubspace.unpack(childKey.getKey()).getLong(1);
                                    return RecordCursorResult.withNextValue(result, nextContinuation);
                                } else {
                                    return RecordCursorResult.withNextValue(null, nextContinuation);
                                }
                            } else {
                                Optional<NoNextReason> noNextReason = cursorLimitManager.getStoppedReason();
                                return RecordCursorResult.withoutNextValue(ByteArrayContinuation.fromNullable(lastKey), noNextReason.get());
                            }
                        } else {
                            if (scanned < configuredScanLimit || lastKey == null) {
                                lastResult = RecordCursorResult.exhausted();
                            } else {
                                lastResult = RecordCursorResult.withoutNextValue(ByteArrayContinuation.fromNullable(lastKey), NoNextReason.RETURN_LIMIT_REACHED);
                            }
                            return lastResult;
                        }
                    });
                }

                @Override
                public void close() {
                }

                @Nonnull
                @Override
                public Executor getExecutor() {
                    return store.getExecutor();
                }

                @Override
                public boolean accept(@Nonnull final RecordCursorVisitor visitor) {
                    return visitor.visitEnter(this) && visitor.visitLeave(this);
                }
            }.filter(Objects::nonNull).skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimitOrMax());
        }
    }


    private RecordCursor<Integer> executeJoin(FDBRecordStore store, int group, String text, @Nullable byte[] continuation, ExecuteProperties executeProperties, JoinTechnique joinTechnique) {
        // Combine the outer and inner queries to produce roughly this join:
        //   SELECT InnerRecord.val FROM OuterRecord JOIN InnerRecord
        //   WHERE OuterRecord.text = $text AND InnerRecord.group = $group AND InnerRecord.outer_id = OuterRecord.rec_no
        if (joinTechnique.getType() == JoinTechniqueType.REMOTE_FETCH) {
            DummyRemoteFetchJoinTechnique remoteFetchJoinTechnique = (DummyRemoteFetchJoinTechnique) joinTechnique;
            return remoteFetchJoinTechnique.executeRemoteFetch(store, group, text, continuation, executeProperties);
        }

        final RecordQuery outer = outerQuery();
        final RecordQueryPlan outerPlan = store.planQuery(outer);
        assertThat(outerPlan, coveringIndexScan(indexScan(allOf(indexName(TEXT_INDEX_NAME), bounds(hasTupleString("[EQUALS $" + TEXT_PARAM + "]"))))));
        Joiner joiner = joinTechnique.createJoiner(store);

        final EvaluationContext evalContext = EvaluationContext.newBuilder()
                .setBinding(GROUP_PARAM, group)
                .setBinding(TEXT_PARAM, text)
                .build(TypeRepository.EMPTY_SCHEMA);

        Descriptors.FieldDescriptor recNoDescriptor = TestSparseJoinPerfProto.OuterRecord.getDescriptor().findFieldByName(REC_NO_FIELD);
        Descriptors.FieldDescriptor valDescriptor = TestSparseJoinPerfProto.InnerRecord.getDescriptor().findFieldByName(VAL_FIELD);
        return RecordCursor.flatMapPipelined(
                outerContinuation -> store.executeQuery(outerPlan, outerContinuation, evalContext, executeProperties.clearSkipAndLimit()),
                (outerRecord, innerContinuation) -> {
                    Message outerMessage = outerRecord.getMessage();
                    final EvaluationContext innerEvalContext = evalContext.withBinding(OUTER_PARAM, outerMessage.getField(recNoDescriptor));
                    return joiner.executeInner(innerEvalContext, innerContinuation, executeProperties.clearSkipAndLimit())
                            .map(innerRecord -> (Integer) innerRecord.getMessage().getField(valDescriptor));
                },
                outerRecord -> outerRecord.getQueriedRecord().getPrimaryKey().pack(),
                continuation,
                store.getPipelineSize(JOIN)
        ).skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    private List<Integer> executeMultiTransactionJoin(FDBRecordContextConfig.Builder contextConfigBuilder, int group, String text, JoinTechnique joinTechnique) {
        long startNanos = System.nanoTime();
        List<Integer> values = new ArrayList<>();
        RecordCursorContinuation continuation = RecordCursorStartContinuation.START;
        int trCount = 0;

        do {
            final ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                    .setReturnedRowLimit(100)
                    .setScannedBytesLimit(50_000)
                    .setScannedRecordsLimit(1000)
                    .setTimeLimit(3_000L)
                    .build();
            final FDBRecordContextConfig contextConfig;
            if (contextConfigBuilder.getTransactionId() != null) {
                contextConfig = contextConfigBuilder.copyBuilder()
                        .setTransactionId(contextConfigBuilder.getTransactionId() + "_" + trCount)
                        .build();
            } else {
                contextConfig = contextConfigBuilder.build();
            }
            try (FDBRecordContext context = database.openContext(contextConfig)) {
                FDBRecordStore store = openStore(context);
                try (RecordCursor<Integer> cursor = executeJoin(store, group, text, continuation.toBytes(), executeProperties, joinTechnique)) {
                    RecordCursorResult<Integer> result;
                    while ((result = cursor.getNext()).hasNext()) {
                        values.add(result.get());
                    }
                    continuation = result.getContinuation();
                }
            }
            trCount++;
        } while (!continuation.isEnd());
        long endNanos = System.nanoTime();

        if (LOGGER.isInfoEnabled()) {
            KeyValueLogMessage msg = KeyValueLogMessage.build("multi-transaction join executed",
                    "group", group,
                    "text", text,
                    "result_count", values.size(),
                    "join_technique_type", joinTechnique.getType(),
                    "execute_micros", TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));
            FDBStoreTimer timer = contextConfigBuilder.getTimer();
            if (timer != null) {
                msg.addKeysAndValues(timer.getKeysAndValues());
            }
            LOGGER.info(msg.toString());
        }

        return values;
    }

    private static void addStats(Map<String, Object> stats, List<Long> data, String prefix) {
        data.sort(Comparator.naturalOrder());
        stats.put(prefix + "_min", data.get(0));
        stats.put(prefix + "_max", data.get(data.size() - 1));
        stats.put(prefix + "_median", data.get(data.size() / 2));
        stats.put(prefix + "_avg", data.stream().mapToLong(Long::longValue).sum() / data.size());
    }

    private Map<String, Object> profileQuery(int group, String text, JoinTechniqueType joinTechniqueType) {
        return profileQuery(group, text, joinTechniqueType, 50, 50);
    }

    private Map<String, Object> profileQuery(int group, String text, JoinTechniqueType joinTechniqueType, int untrackedRepetitions, int trackedRepititions) {
        JoinTechnique joinTechnique = getJoinTechnique(joinTechniqueType, group);

        final String transactionIdBase = String.format("perf_%s_%d_%s_", joinTechniqueType, group, text);

        // Run some initial tests without profiling to warm up the JVM
        for (int i = 0; i < untrackedRepetitions; i++) {
            executeMultiTransactionJoin(FDBRecordContextConfig.newBuilder().setTimer(new FDBStoreTimer()), group, text, joinTechnique);
        }

        List<Long> durations = new ArrayList<>();
        int resultCount = 0;
        long readsCount = 0;
        int bytesReadCount = 0;
        int transactionCount = 0;
        for (int i = 0; i < trackedRepititions; i++) {
            long startNanos = System.nanoTime();
            final FDBStoreTimer timer = new FDBStoreTimer();
            FDBRecordContextConfig.Builder configBuilder = FDBRecordContextConfig.newBuilder()
                    .setTimer(timer)
                    .setLogTransaction(false)
                    .setTransactionId(transactionIdBase);
            resultCount = executeMultiTransactionJoin(configBuilder, group, text, joinTechnique).size();
            readsCount = timer.getCount(FDBStoreTimer.Counts.READS);
            bytesReadCount = timer.getCount(FDBStoreTimer.Counts.BYTES_READ);
            transactionCount = timer.getCount(FDBStoreTimer.Counts.OPEN_CONTEXT);
            long endNanos = System.nanoTime();
            durations.add(TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));
        }

        Map<String, Object> stats = new TreeMap<>();
        stats.put("result_count", resultCount);
        stats.put("reads_count", readsCount);
        stats.put("bytes_read_count", bytesReadCount);
        stats.put("selectivity", resultCount * TEXTS.size() * 1.0 / OUTER_RECORD_COUNT);
        stats.put("transaction_count", transactionCount);
        addStats(stats, durations, "latency_micros");

        if (LOGGER.isInfoEnabled()) {

            KeyValueLogMessage logMessage = KeyValueLogMessage.build("join query profiled")
                    .addKeyAndValue("group", group)
                    .addKeyAndValue("text", text)
                    .addKeyAndValue("join_technique_type", joinTechnique.getType());
            logMessage.addKeysAndValues(stats);

            LOGGER.info(logMessage.toString());
        }

        return stats;
    }

    private FDBRecordStore openStore(FDBRecordContext context) {
        return FDBRecordStore.newBuilder()
                .setContext(context)
                .setMetaDataProvider(createMetaData())
                .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION)
                .setKeySpacePath(TestKeySpace.getKeyspacePath(PATH))
                // .setPipelineSizer(operation -> 100)
                .createOrOpen();
    }

    private void populate(Random r) {
        long startNanos = System.nanoTime();
        Map<Integer, Integer> countsByGroup = new TreeMap<>();

        FDBRecordContext context = database.openContext();
        try {
            FDBRecordStore store = openStore(context);

            for (int i = 0; i < OUTER_RECORD_COUNT; i++) {
                TestSparseJoinPerfProto.OuterRecord outerRecord = TestSparseJoinPerfProto.OuterRecord.newBuilder()
                        .setRecNo(r.nextInt(50_000_000))
                        .setText(TEXTS.get(i % TEXTS.size()))
                        .build();
                store.saveRecord(outerRecord);

                // Create an inner record pointing to OuterRecord, choosing a group with a normal distribution
                // centered around the middle group so that there are 3 z-values around the center
                int group = (int)(r.nextGaussian() * (GROUP_COUNT / 6) + (GROUP_COUNT / 2));
                group = Math.min(group, GROUP_COUNT - 1);
                group = Math.max(group, 0);
                TestSparseJoinPerfProto.InnerRecord innerRecord = TestSparseJoinPerfProto.InnerRecord.newBuilder()
                        .setRecNo(r.nextLong())
                        .setGroup(group)
                        .setOuterId(outerRecord.getRecNo())
                        .setVal(i)
                        .build();
                store.saveRecord(innerRecord);
                countsByGroup.compute(group, (ignore, current) -> current == null ? 1 : current + 1);

                if (context.ensureActive().getApproximateSize().join() > 100_000) {
                    context.commit();
                    context.close();

                    context = database.openContext();
                    store = openStore(context);
                }
            }

            context.commit();

        } finally {
            context.close();

            long endNanos = System.nanoTime();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(KeyValueLogMessage.of("data populated for sparse join test",
                        "outer_record_count", OUTER_RECORD_COUNT,
                        "inner_records_by_group", countsByGroup,
                        "populate_time_micros", TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos)));
            }
        }
    }

    private void repopulate() {
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath path = TestKeySpace.getKeyspacePath(PATH);
            path.deleteAllData(context);
            context.commit();
        }

        Random r = new Random();
        populate(r);
    }

    @BeforeEach
    void setUp() {
        FDBDatabaseFactory databaseFactory = FDBDatabaseFactory.instance();
        databaseFactory.setAPIVersion(APIVersion.API_VERSION_7_1);
        database = databaseFactory.getDatabase();
    }

    @Test
    void repopulateForTest() {
        repopulate();
    }

    @Test
    void emptyJoin() {
        try (FDBRecordContext context = database.openContext()) {
            FDBRecordStore store = openStore(context);
            RecordCursor<Integer> results = executeJoin(store, 42, "foo", null, ExecuteProperties.SERIAL_EXECUTE, new StandardIndexProbeJoinTechnique());
            assertEquals(0, results.getCount().join());
            context.commit();
        }
    }

    @ParameterizedTest(name = "middleGroupJoin[joinTechniqueType={0}]")
    @EnumSource(value = JoinTechniqueType.class)
    void middleGroupJoin(JoinTechniqueType joinTechniqueType) {
        profileQuery(GROUP_COUNT / 2, "d", joinTechniqueType);
    }

    @ParameterizedTest(name = "zeroGroupJoin[joinTechniqueType={0}]")
    @EnumSource(value = JoinTechniqueType.class)
    void zeroGroupJoin(JoinTechniqueType joinTechniqueType) {
        profileQuery(0, "d", joinTechniqueType);
    }

    @ParameterizedTest(name = "compareEachGroup[joinTechniqueType={0}]")
    @EnumSource(value = JoinTechniqueType.class)
    @Timeout(value = 30L, unit = TimeUnit.MINUTES)
    void compareEachGroup(JoinTechniqueType joinTechniqueType) {
        Map<Integer, Map<String, Object>> statsByGroup = new TreeMap<>();
        for (int i = 0; i < GROUP_COUNT; i++) {
            Map<String, Object> stats = profileQuery(i, "b", joinTechniqueType, 10, 10);
            statsByGroup.put(i, stats);
        }
        printTable(statsByGroup, "group");
    }

    private static <K> void printTable(Map<K, Map<String, Object>> statsByKey, String key) {
        Map<String, Object> first = statsByKey.values().iterator().next();
        Set<String> keySet = first.keySet();

        System.out.print(key);
        for (String column : keySet) {
            System.out.print('\t');
            System.out.print(column);
        }
        System.out.println();

        for (Map.Entry<K, Map<String, Object>> entry : statsByKey.entrySet()) {
            System.out.print(entry.getKey());
            for (String column : keySet) {
                System.out.print('\t');
                System.out.print(entry.getValue().get(column));
            }
            System.out.println();
        }
    }
}
