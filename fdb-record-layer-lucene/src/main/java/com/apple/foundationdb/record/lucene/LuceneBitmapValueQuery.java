/*
 * LuceneBitmapValueQuery.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunctionCall;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.BaseField;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexAggregate;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.planning.FilterSatisfiedMask;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.DocIdSetBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Use BitmapValue indexes to get composite bitmaps to turn back into doc ids.
 * Do this for any leftover predicates after pushing down into Lucene, that is, for fields not included in Lucene documents.
 * Probably only performs acceptably when the record keys and document ids have mostly congruent segment distributions.
 */
@API(API.Status.EXPERIMENTAL)
public class LuceneBitmapValueQuery extends LuceneQueryClause {
    @Nonnull
    private final RecordQueryPlan plan;

    public LuceneBitmapValueQuery(@Nonnull RecordQueryPlan plan) {
        super(LuceneQueryType.QUERY);
        this.plan = plan;
    }

    @Nullable
    public static LuceneBitmapValueQuery tryBuild(@Nonnull RecordQueryPlanner planner,
                                                  @Nonnull RecordType recordType,
                                                  @Nonnull FilterSatisfiedMask filterMask,
                                                  @Nonnull KeyExpression groupingKey) {
        final KeyExpression primaryKey = recordType.getPrimaryKey();
        if (!(primaryKey instanceof FieldKeyExpression)) {
            return null;
        }
        final List<QueryComponent> filters = new ArrayList<>();
        final List<FilterSatisfiedMask> candidates = new ArrayList<>();
        for (FilterSatisfiedMask child : filterMask.getChildren()) {
            if (!child.isSatisfied()) {
                filters.add(child.getUnsatisfiedFilter());
                candidates.add(child);
            } else if (child.getFilter() instanceof BaseField && groupingKey instanceof FieldKeyExpression
                    && ((BaseField)child.getFilter()).getFieldName().equals(((FieldKeyExpression)groupingKey).getFieldName())) {
                filters.add(child.getFilter()); // Share group predicate.
            }
        }
        if (filters.isEmpty()) {
            return null;
        }
        KeyExpression predicateField = null;
        for (QueryComponent filter : filters) {
            if (filter instanceof BaseField) {
                predicateField = Key.Expressions.field(((BaseField)filter).getFieldName());
                break;
            }
        }
        if (predicateField == null) {
            return null;
        }
        final IndexAggregateFunctionCall aggregate = new IndexAggregateFunctionCall(FunctionNames.BITMAP_VALUE,
                                                                                    Key.Expressions.concat(groupingKey, primaryKey).group(1));
        final QueryComponent filter = filters.size() > 1 ? com.apple.foundationdb.record.query.expressions.Query.and(filters) : filters.get(0);
        // TODO: Add primary key range filters here; these will limit size (and so relevancy) of returned bitmaps.
        final RecordQuery recordQuery = RecordQuery.newBuilder()
                .setRecordType(recordType.getName())
                .setFilter(filter)
                .setRequiredResults(Collections.singletonList(primaryKey))
                .build();
        final Optional<RecordQueryPlan> recordQueryPlan = ComposedBitmapIndexAggregate.tryPlan(planner, recordQuery, aggregate, IndexQueryabilityFilter.DEFAULT);
        if (recordQueryPlan.isEmpty()) {
            return null;
        }
        for (FilterSatisfiedMask candidate : candidates) {
            candidate.setSatisfied(true);
        }
        return new LuceneBitmapValueQuery(recordQueryPlan.get());
    }

    @Override
    public BoundQuery bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
        final Query luceneQuery = new BitmapValueQuery(store, plan, context);
        return BoundQuery.ofLuceneQueryWithQueryType(luceneQuery, getQueryType());
    }

    @Override
    public void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder, @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        detailsBuilder.add("bitmap: {{plan}}");
        attributeMapBuilder.put("plan", Attribute.gml(plan.toString()));
    }

    @Override
    public int planHash(@Nonnull PlanHashMode hashMode) {
        return plan.planHash(hashMode);
    }

    /**
     * The actual Lucene {@link Query}.
     */
    static class BitmapValueQuery extends Query {
        @Nonnull
        private final FDBRecordStoreBase<?> store;
        @Nonnull
        private final RecordQueryPlan plan;
        @Nonnull
        private final EvaluationContext context;

        BitmapValueQuery(@Nonnull FDBRecordStoreBase<?> store, @Nonnull RecordQueryPlan plan, @Nonnull EvaluationContext context) {
            this.store = store;
            this.plan = plan;
            this.context = context;
        }

        @Override
        public Weight createWeight(final IndexSearcher searcher, final ScoreMode scoreMode, final float boost) throws IOException {
            return new ConstantScoreWeight(this, boost) {
                @SuppressWarnings("PMD.CloseResource")
                @Override
                public ScorerSupplier scorerSupplier(final LeafReaderContext context) throws IOException {
                    final LeafReader reader = context.reader();
                    final SegmentInfo segmentInfo = ((SegmentReader)reader).getSegmentInfo().info;
                    final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc());
                    final Weight weight = this;

                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            runQuery(segmentInfo, result);
                            DocIdSetIterator iterator = result.build().iterator();
                            return new ConstantScoreScorer(weight, score(), scoreMode, iterator);
                        }

                        @Override
                        public long cost() {
                            // TODO: Could run the query to get just popcounts and not do the actually docid mapping.
                            return -1;
                        }
                    };
                }

                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    return scorerSupplier(context).get(Long.MAX_VALUE);
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false;
                }
            };
        }

        private void runQuery(@Nonnull SegmentInfo segmentInfo, @Nonnull DocIdSetBuilder docIdSetBuilder) {
            final FDBDirectory directory = (FDBDirectory)segmentInfo.dir;
            final LucenePrimaryKeySegmentIndex primaryKeySegmentIndex = Objects.requireNonNull(directory.getPrimaryKeySegmentIndex());
            final String segmentName = segmentInfo.name;
            final DirectoryReader directoryReader;
            final long segmentId;
            try {
                segmentId = directory.primaryKeySegmentId(segmentName, false);
                directoryReader = DirectoryReader.open(directory);
            } catch (IOException ex) {
                throw LuceneExceptions.toRecordCoreException("segment info error", ex);
            }
            // TODO: Bind primary key range parameters from max per segment, as maintained by LucenePrimaryKeySegmentIndex.
            try (RecordCursor<QueryResult> cursor = plan.executePlan(store, context, null, ExecuteProperties.SERIAL_EXECUTE)) {
                while (true) {
                    final RecordCursorResult<QueryResult> nextResult = store.getContext().asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_FIND_PRIMARY_KEY, cursor.onNext());
                    if (!nextResult.hasNext()) {
                        break;
                    }
                    final IndexEntry indexEntry = nextResult.get().getIndexEntry();
                    final long offset = indexEntry.getKey().getLong(indexEntry.getKeySize() - 1);
                    final byte[] bits = indexEntry.getValue().getBytes(0);
                    int nrecs = 0;
                    for (int i = 0; i < bits.length; i++) {
                        nrecs += Integer.bitCount(bits[i] & 0xFF);
                    }
                    if (nrecs > 0) {
                        // TODO: Bulk version of these loops and mapping lookup. Read multiple pkey entries or store a different index.
                        final DocIdSetBuilder.BulkAdder adder = docIdSetBuilder.grow(nrecs);
                        for (int i = 0; i < bits.length; i++) {
                            int b = bits[i] & 0xFF;
                            for (int j = 0; j < 8; j++) {
                                if ((b & (1 << j)) != 0) {
                                    Tuple primaryKey = Tuple.from(offset + i * 8L + j);
                                    final LucenePrimaryKeySegmentIndex.DocumentIndexEntry entry;
                                    try {
                                        entry = primaryKeySegmentIndex.findDocument(directoryReader, primaryKey);
                                    } catch (IOException ex) {
                                        throw LuceneExceptions.toRecordCoreException("segment docid mapping error", ex);
                                    }
                                    if (entry != null) {
                                        adder.add(entry.docId);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        @Override
        public String toString(final String field) {
            return "BITMAP-VALUE(" + plan + ")";
        }

        @Override
        @SpotBugsSuppressWarnings("EQ_UNUSUAL")
        public boolean equals(final Object obj) {
            return sameClassAs(obj) &&
                equalsTo(getClass().cast(obj));
        }

        private boolean equalsTo(BitmapValueQuery query) {
            return Objects.equals(plan, query.plan);
        }

        @Override
        public int hashCode() {
            int hash = classHash();
            return 31 * hash + plan.hashCode();
        }

    }
}
