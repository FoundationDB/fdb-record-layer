/*
 * BitSetQuery.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.query;

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
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
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.Objects;

/**
 * This version assumes longs--want to switch based on Point type (either long or int) as future work.
 */
public class BitSetQuery extends Query {

    private final String field;
    private final long bitMask;

    public BitSetQuery(final String field, long bitMask) {
        this.field = field;
        this.bitMask = bitMask;
    }

    @Override
    public String toString(final String field) {
        return "BITSET-MASK(" + Long.toBinaryString(bitMask) + ")";
    }

    @Override
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    public boolean equals(final Object obj) {
        return sameClassAs(obj) &&
               equalsTo(getClass().cast(obj));
    }

    private boolean equalsTo(BitSetQuery query) {
        return Objects.equals(bitMask, query.bitMask) && Objects.equals(field, query.field);
    }

    @Override
    public int hashCode() {
        int hash = classHash();
        hash = 31 * hash + field.hashCode();
        return 31 * hash + Long.hashCode(bitMask);
    }

    @Override
    public Weight createWeight(final IndexSearcher searcher, final ScoreMode scoreMode, final float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {

            private boolean matches(byte[] bitSet) {
                return (NumericUtils.sortableBytesToLong(bitSet, 0) & bitMask) == bitMask;
            }

            private PointValues.IntersectVisitor getIntersectVisitor(DocIdSetBuilder result) {
                return new PointValues.IntersectVisitor() {

                    DocIdSetBuilder.BulkAdder adder;

                    @Override
                    public void grow(int count) {
                        adder = result.grow(count);
                    }

                    @Override
                    public void visit(int docID) {
                        adder.add(docID);
                    }

                    @Override
                    public void visit(int docID, byte[] packedValue) {
                        if (matches(packedValue)) {
                            visit(docID);
                        }
                    }

                    @SuppressWarnings("PMD.AssignmentInOperand")
                    @Override
                    public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                        if (matches(packedValue)) {
                            int docID;
                            while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                                visit(docID);
                            }
                        }
                    }

                    @Override
                    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                        return PointValues.Relation.CELL_CROSSES_QUERY;
                    }
                };
            }

            private PointValues.IntersectVisitor getInverseIntersectVisitor(FixedBitSet result, int[] cost) {
                return new PointValues.IntersectVisitor() {

                    @Override
                    public void visit(int docID) {
                        result.clear(docID);
                        cost[0]--;
                    }

                    @Override
                    public void visit(int docID, byte[] packedValue) {
                        if (!matches(packedValue)) {
                            visit(docID);
                        }
                    }

                    @Override
                    @SuppressWarnings("PMD.AssignmentInOperand")
                    public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                        if (!matches(packedValue)) {
                            int docID;
                            while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                                visit(docID);
                            }
                        }
                    }

                    @Override
                    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                        return PointValues.Relation.CELL_CROSSES_QUERY;
                    }
                };
            }

            @SuppressWarnings("PMD.CloseResource")
            @Override
            public ScorerSupplier scorerSupplier(final LeafReaderContext context) throws IOException {
                LeafReader reader = context.reader();
                PointValues values = reader.getPointValues(field);
                if (values == null) {
                    // no docs in this segment
                    return null;
                }
                Weight weight = this;

                // TODO(Bfines) decide if we need to check index dimensions here
                // if (values.getNumIndexDimensions() != numDims) {
                //     throw new IllegalArgumentException("field=\"" + field + "\" was indexed with numIndexDimensions=" + values.getNumIndexDimensions() + " but this query has numDims=" + numDims);
                // }
                // if (bytesPerDim != values.getBytesPerDimension()) {
                //     throw new IllegalArgumentException("field=\"" + field + "\" was indexed with bytesPerDim=" + values.getBytesPerDimension() + " but this query has bytesPerDim=" + bytesPerDim);
                // }

                return new ScorerSupplier() {

                    final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);
                    final PointValues.IntersectVisitor visitor = getIntersectVisitor(result);
                    long cost = -1;

                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        values.intersect(visitor);
                        DocIdSetIterator iterator = result.build().iterator();
                        return new ConstantScoreScorer(weight, score(), scoreMode, iterator);
                    }

                    @Override
                    public long cost() {
                        if (cost == -1) {
                            // Computing the cost may be expensive, so only do it if necessary
                            cost = values.estimateDocCount(visitor);
                            assert cost >= 0;
                        }
                        return cost;
                    }
                };
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                ScorerSupplier scorerSupplier = scorerSupplier(context);
                if (scorerSupplier == null) {
                    return null;
                }
                return scorerSupplier.get(Long.MAX_VALUE);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }
}
