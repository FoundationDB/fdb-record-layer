/*
 * LuceneQueryFieldComparisonClause.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Query clause using a {@link Comparisons.Comparison} against a document field.
 */
@API(API.Status.UNSTABLE)
public abstract class LuceneQueryFieldComparisonClause extends LuceneQueryClause {
    @Nonnull
    protected final String field;
    @Nonnull
    protected final LuceneIndexExpressions.DocumentFieldType fieldType;
    @Nonnull
    protected final Comparisons.Comparison comparison;

    protected LuceneQueryFieldComparisonClause(@Nonnull String field, @Nonnull LuceneIndexExpressions.DocumentFieldType fieldType, @Nonnull Comparisons.Comparison comparison) {
        this.field = field;
        this.fieldType = fieldType;
        this.comparison = comparison;
    }

    @Nonnull
    public String getField() {
        return field;
    }

    @Nonnull
    public LuceneIndexExpressions.DocumentFieldType getFieldType() {
        return fieldType;
    }

    @Nonnull
    public Comparisons.Comparison getComparison() {
        return comparison;
    }

    @Override
    public void getPlannerGraphDetails(@Nonnull final ImmutableList.Builder<String> detailsBuilder, @Nonnull final ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        detailsBuilder.add("field: {{field}}");
        attributeMapBuilder.put("field", Attribute.gml(field));
        detailsBuilder.add("fieldType: {{fieldType}}");
        attributeMapBuilder.put("fieldType", Attribute.gml(fieldType));
        detailsBuilder.add("comparisonType: {{comparisonType}}");
        attributeMapBuilder.put("comparisonType", Attribute.gml(comparison.getType()));
        detailsBuilder.add("comparison: {{comparison}}");
        attributeMapBuilder.put("comparison", Attribute.gml(comparison.typelessString()));
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, field, fieldType, comparison);
    }

    @Override
    public String toString() {
        return field + ":" + fieldType + " " + comparison;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final LuceneQueryFieldComparisonClause that = (LuceneQueryFieldComparisonClause)o;

        if (!field.equals(that.field)) {
            return false;
        }
        if (fieldType != that.fieldType) {
            return false;
        }
        return comparison.equals(that.comparison);
    }

    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + fieldType.hashCode();
        result = 31 * result + comparison.hashCode();
        return result;
    }

    @Nonnull
    @SuppressWarnings("fallthrough")
    public static LuceneQueryFieldComparisonClause create(@Nonnull String field, @Nonnull LuceneIndexExpressions.DocumentFieldType fieldType, @Nonnull Comparisons.Comparison comparison) {
        switch (comparison.getType()) {
            case NOT_NULL:
            case IS_NULL:
                return new NullQuery(field, fieldType, comparison);
            case EQUALS:
            case NOT_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
            case IN:
                break;
            case STARTS_WITH:
                if (fieldType == LuceneIndexExpressions.DocumentFieldType.STRING) {
                    break;
                }
                /* else falls through */
            case TEXT_CONTAINS_ALL:
            case TEXT_CONTAINS_ANY:
            case TEXT_CONTAINS_PHRASE:
            case TEXT_CONTAINS_PREFIX:
            case TEXT_CONTAINS_ALL_PREFIXES:
            case TEXT_CONTAINS_ANY_PREFIX:
                if (fieldType == LuceneIndexExpressions.DocumentFieldType.TEXT) {
                    break;
                }
                /* else falls through */
            default:
                throw new RecordCoreException("comparison type not supported for Lucene: " + comparison.getType());
        }
        switch (fieldType) {
            case STRING:
            case TEXT:
                return new StringQuery(field, fieldType, comparison);
            case INT:
                return new IntQuery(field, fieldType, comparison);
            case LONG:
                return new LongQuery(field, fieldType, comparison);
            case DOUBLE:
                return new DoubleQuery(field, fieldType, comparison);
            default:
                throw new RecordCoreException("unsupported Lucene index field type: " + fieldType);
        }
    }

    protected static Query negate(@Nonnull Query query) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        builder.add(query, BooleanClause.Occur.MUST_NOT);
        return builder.build();
    }

    static class NullQuery extends LuceneQueryFieldComparisonClause {
        public NullQuery(@Nonnull String field, @Nonnull LuceneIndexExpressions.DocumentFieldType fieldType, @Nonnull Comparisons.Comparison comparison) {
            super(field, fieldType, comparison);
        }

        @Override
        public Query bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
            Query allValues = new TermRangeQuery(field, null, null, true, true);
            if (comparison.getType() == Comparisons.Type.NOT_NULL) {
                return allValues;
            } else {
                // *:* -f[* TO *]
                return negate(allValues);
            }
        }
    }

    static class StringQuery extends LuceneQueryFieldComparisonClause {
        public StringQuery(@Nonnull String field, @Nonnull LuceneIndexExpressions.DocumentFieldType fieldType, @Nonnull Comparisons.Comparison comparison) {
            super(field, fieldType, comparison);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Query bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
            Object comparand = comparison.getComparand(store, context);
            if (comparand == null) {
                return new MatchNoDocsQuery();
            }
            switch (comparison.getType()) {
                case EQUALS:
                    return new TermQuery(new Term(field, (String)comparand));
                case NOT_EQUALS: {
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    builder.add(TermRangeQuery.newStringRange(field, null, (String)comparand, true, false), BooleanClause.Occur.SHOULD);
                    builder.add(TermRangeQuery.newStringRange(field, (String)comparand, null, false, true), BooleanClause.Occur.SHOULD);
                    return builder.build();
                }
                case LESS_THAN:
                    return TermRangeQuery.newStringRange(field, null, (String)comparand, true, false);
                case LESS_THAN_OR_EQUALS:
                    return TermRangeQuery.newStringRange(field, null, (String)comparand, true, true);
                case GREATER_THAN:
                    return TermRangeQuery.newStringRange(field, (String)comparand, null, false, true);
                case GREATER_THAN_OR_EQUALS:
                    return TermRangeQuery.newStringRange(field, (String)comparand, null, true, true);
                case STARTS_WITH:
                case TEXT_CONTAINS_PREFIX:
                    return new PrefixQuery(new Term(field, (String)comparand));
                case TEXT_CONTAINS_PHRASE:
                    // PhraseQuery will require tokenizing, so may as well just use parser.
                    try {
                        final Pair<Analyzer, Analyzer> analyzerPair = LuceneAnalyzerRegistryImpl.instance().getLuceneAnalyzerPair(index);
                        final QueryParser parser = new QueryParser(field, analyzerPair.getRight());
                        return parser.parse("\"" + comparand + "\"");
                    } catch (Exception ex) {
                        throw new RecordCoreArgumentException("Unable to parse phrase for query", ex);
                    }
                case IN:
                case TEXT_CONTAINS_ANY: {
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    for (String value : ((List<String>)comparand)) {
                        builder.add(new TermQuery(new Term(field, value)), BooleanClause.Occur.SHOULD);
                    }
                    return builder.build();
                }
                case TEXT_CONTAINS_ALL: {
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    for (String value : ((List<String>)comparand)) {
                        builder.add(new TermQuery(new Term(field, value)), BooleanClause.Occur.MUST);
                    }
                    return builder.build();
                }
                case TEXT_CONTAINS_ALL_PREFIXES: {
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    for (String value : ((List<String>)comparand)) {
                        builder.add(new PrefixQuery(new Term(field, value)), BooleanClause.Occur.MUST);
                    }
                    return builder.build();
                }
                case TEXT_CONTAINS_ANY_PREFIX: {
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    for (String value : ((List<String>)comparand)) {
                        builder.add(new PrefixQuery(new Term(field, value)), BooleanClause.Occur.SHOULD);
                    }
                    return builder.build();
                }
                default:
                    throw new RecordCoreException("comparison type not supported for String: " + comparison.getType());
            }
        }
    }

    static class IntQuery extends LuceneQueryFieldComparisonClause {
        public IntQuery(@Nonnull String field, @Nonnull LuceneIndexExpressions.DocumentFieldType fieldType, @Nonnull Comparisons.Comparison comparison) {
            super(field, fieldType, comparison);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Query bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
            Object comparand = comparison.getComparand(store, context);
            if (comparand == null) {
                return new MatchNoDocsQuery();
            }
            switch (comparison.getType()) {
                case EQUALS:
                    return IntPoint.newExactQuery(field, (Integer)comparand);
                case NOT_EQUALS: {
                    int value = (Integer)comparand;
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    builder.add(IntPoint.newRangeQuery(field, Integer.MIN_VALUE, value - 1), BooleanClause.Occur.SHOULD);
                    builder.add(IntPoint.newRangeQuery(field, value + 1, Integer.MAX_VALUE), BooleanClause.Occur.SHOULD);
                    return builder.build();
                }
                case LESS_THAN:
                    return IntPoint.newRangeQuery(field, Integer.MIN_VALUE, (Integer)comparand - 1);
                case LESS_THAN_OR_EQUALS:
                    return IntPoint.newRangeQuery(field, Integer.MIN_VALUE, (Integer)comparand);
                case GREATER_THAN:
                    return IntPoint.newRangeQuery(field, (Integer)comparand + 1, Integer.MAX_VALUE);
                case GREATER_THAN_OR_EQUALS:
                    return IntPoint.newRangeQuery(field, (Integer)comparand, Integer.MAX_VALUE);
                case IN:
                    return IntPoint.newSetQuery(field, ((List<Integer>)comparand));
                default:
                    throw new RecordCoreException("comparison type not supported for Integer: " + comparison.getType());
            }
        }
    }

    static class LongQuery extends LuceneQueryFieldComparisonClause {
        public LongQuery(@Nonnull String field, @Nonnull LuceneIndexExpressions.DocumentFieldType fieldType, @Nonnull Comparisons.Comparison comparison) {
            super(field, fieldType, comparison);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Query bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
            Object comparand = comparison.getComparand(store, context);
            if (comparand == null) {
                return new MatchNoDocsQuery();
            }
            switch (comparison.getType()) {
                case EQUALS:
                    return LongPoint.newExactQuery(field, (Long)comparand);
                case NOT_EQUALS: {
                    long value = (Long)comparand;
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    builder.add(LongPoint.newRangeQuery(field, Long.MIN_VALUE, value - 1), BooleanClause.Occur.SHOULD);
                    builder.add(LongPoint.newRangeQuery(field, value + 1, Long.MAX_VALUE), BooleanClause.Occur.SHOULD);
                    return builder.build();
                }
                case LESS_THAN:
                    return LongPoint.newRangeQuery(field, Long.MIN_VALUE, (Long)comparand - 1);
                case LESS_THAN_OR_EQUALS:
                    return LongPoint.newRangeQuery(field, Long.MIN_VALUE, (Long)comparand);
                case GREATER_THAN:
                    return LongPoint.newRangeQuery(field, (Long)comparand + 1, Long.MAX_VALUE);
                case GREATER_THAN_OR_EQUALS:
                    return LongPoint.newRangeQuery(field, (Long)comparand, Long.MAX_VALUE);
                case IN:
                    return LongPoint.newSetQuery(field, ((List<Long>)comparand));
                default:
                    throw new RecordCoreException("comparison type not supported for Long: " + comparison.getType());
            }
        }
    }

    static class DoubleQuery extends LuceneQueryFieldComparisonClause {
        public DoubleQuery(@Nonnull String field, @Nonnull LuceneIndexExpressions.DocumentFieldType fieldType, @Nonnull Comparisons.Comparison comparison) {
            super(field, fieldType, comparison);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Query bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
            Object comparand = comparison.getComparand(store, context);
            if (comparand == null) {
                return new MatchNoDocsQuery();
            }
            switch (comparison.getType()) {
                case EQUALS:
                    return DoublePoint.newExactQuery(field, (Double)comparand);
                case NOT_EQUALS: {
                    double value = (Double)comparand;
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    builder.add(DoublePoint.newRangeQuery(field, Double.MIN_VALUE, value - 1), BooleanClause.Occur.SHOULD);
                    builder.add(DoublePoint.newRangeQuery(field, value + 1, Double.MAX_VALUE), BooleanClause.Occur.SHOULD);
                    return builder.build();
                }
                case LESS_THAN:
                    return DoublePoint.newRangeQuery(field, Double.MIN_VALUE, (Double)comparand - 1);
                case LESS_THAN_OR_EQUALS:
                    return DoublePoint.newRangeQuery(field, Double.MIN_VALUE, (Double)comparand);
                case GREATER_THAN:
                    return DoublePoint.newRangeQuery(field, (Double)comparand + 1, Double.MAX_VALUE);
                case GREATER_THAN_OR_EQUALS:
                    return DoublePoint.newRangeQuery(field, (Double)comparand, Double.MAX_VALUE);
                case IN:
                    return DoublePoint.newSetQuery(field, ((List<Double>)comparand));
                default:
                    throw new RecordCoreException("comparison type not supported for Double: " + comparison.getType());
            }
        }
    }
}
