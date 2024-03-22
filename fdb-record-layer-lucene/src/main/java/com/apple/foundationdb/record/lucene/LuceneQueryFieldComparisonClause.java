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
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import javax.annotation.Nullable;
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

    protected LuceneQueryFieldComparisonClause(@Nonnull LuceneQueryType querType, @Nonnull String field, @Nonnull LuceneIndexExpressions.DocumentFieldType fieldType, @Nonnull Comparisons.Comparison comparison) {
        super(querType);
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
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, field, fieldType, comparison);
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
    public static LuceneQueryFieldComparisonClause create(@Nonnull final LuceneQueryType queryType,
                                                          @Nonnull final String field,
                                                          @Nonnull final LuceneIndexExpressions.DocumentFieldType fieldType,
                                                          final boolean fieldNameOverride, @Nullable final String namedFieldSuffix,
                                                          @Nonnull final Comparisons.Comparison comparison) {
        switch (comparison.getType()) {
            case NOT_NULL:
            case IS_NULL:
                return new NullQuery(queryType, field, fieldType, comparison);
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

        // At this point we assume that if fieldNameOverride is TRUE, then the query is modifying the original field name
        // (adding a value for map support).
        // The resulting Lucene query will use the comparand against the field name, with "any" value for the field's value.
        // TODO: In the future this would need to be changed to support comparing both field name and value.
        switch (fieldType) {
            case STRING:
            case TEXT:
                return new StringQuery(queryType, field, fieldType, comparison);
            case INT:
                return new IntQuery(queryType, field, fieldType, comparison, fieldNameOverride, namedFieldSuffix);
            case LONG:
                return new LongQuery(queryType, field, fieldType, comparison, fieldNameOverride, namedFieldSuffix);
            case DOUBLE:
                return new DoubleQuery(queryType, field, fieldType, comparison, fieldNameOverride, namedFieldSuffix);
            default:
                throw new RecordCoreException("unsupported Lucene index field type: " + fieldType);
        }
    }

    protected String applyFieldNameConversion(final boolean fieldNameOverride, final String field, final String namedFieldSuffix, final Object comparand) {
        if ( ! fieldNameOverride) {
            return field;
        }
        int location = field.lastIndexOf(namedFieldSuffix);
        if (location == -1) {
            throw new RecordCoreArgumentException("Cannot find the replacement suffix in Lucene field name")
                    .addLogInfo("fieldName", field)
                    .addLogInfo("suffix", namedFieldSuffix);
        }
        if (! (comparand instanceof String)) {
            throw new RecordCoreArgumentException("Comparand type for Lucene field comparison must be a String")
                    .addLogInfo("fieldName", field)
                    .addLogInfo("comparandType", comparand.getClass().getName());
        }
        return field.substring(0, location) + comparand;
    }

    protected Comparisons.Comparison applyComparisonConversion(final boolean fieldNameOverride, Comparisons.Comparison comparison) {
        if (fieldNameOverride) {
            // When comparing against the field name the query effectively becomes field existance
            return new Comparisons.NullComparison(Comparisons.Type.NOT_NULL);
        } else {
            return comparison;
        }
    }

    protected static Query negate(@Nonnull Query query) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        builder.add(query, BooleanClause.Occur.MUST_NOT);
        return builder.build();
    }

    static class NullQuery extends LuceneQueryFieldComparisonClause {
        public NullQuery(@Nonnull final LuceneQueryType queryType, @Nonnull String field, @Nonnull LuceneIndexExpressions.DocumentFieldType fieldType, @Nonnull Comparisons.Comparison comparison) {
            super(queryType, field, fieldType, comparison);
        }

        @Override
        public BoundQuery bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
            Query allValues = new TermRangeQuery(field, null, null, true, true);
            if (comparison.getType() == Comparisons.Type.NOT_NULL) {
                return toBoundQuery(allValues);
            } else {
                // *:* -f[* TO *]
                final Query negatedQuery = negate(allValues);
                return toBoundQuery(negatedQuery);
            }
        }
    }

    static class StringQuery extends LuceneQueryFieldComparisonClause {
        public StringQuery(@Nonnull final LuceneQueryType queryType, @Nonnull String field, @Nonnull LuceneIndexExpressions.DocumentFieldType fieldType, @Nonnull Comparisons.Comparison comparison) {
            super(queryType, field, fieldType, comparison);
        }

        @Override
        @SuppressWarnings("unchecked")
        public BoundQuery bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
            Object comparand = comparison.getComparand(store, context);
            if (comparand == null) {
                return toBoundQuery(new MatchNoDocsQuery());
            }
            switch (comparison.getType()) {
                case EQUALS:
                    return toBoundQuery(new TermQuery(new Term(field, (String)comparand)));
                case NOT_EQUALS: {
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    builder.add(TermRangeQuery.newStringRange(field, null, (String)comparand, true, false), BooleanClause.Occur.SHOULD);
                    builder.add(TermRangeQuery.newStringRange(field, (String)comparand, null, false, true), BooleanClause.Occur.SHOULD);
                    return toBoundQuery(builder.build());
                }
                case LESS_THAN:
                    return toBoundQuery(TermRangeQuery.newStringRange(field, null, (String)comparand, true, false));
                case LESS_THAN_OR_EQUALS:
                    return toBoundQuery(TermRangeQuery.newStringRange(field, null, (String)comparand, true, true));
                case GREATER_THAN:
                    return toBoundQuery(TermRangeQuery.newStringRange(field, (String)comparand, null, false, true));
                case GREATER_THAN_OR_EQUALS:
                    return toBoundQuery(TermRangeQuery.newStringRange(field, (String)comparand, null, true, true));
                case STARTS_WITH:
                case TEXT_CONTAINS_PREFIX:
                    return toBoundQuery(new PrefixQuery(new Term(field, (String)comparand)));
                case TEXT_CONTAINS_PHRASE:
                    // PhraseQuery will require tokenizing, so may as well just use parser.
                    try {
                        final var fieldInfos = LuceneIndexExpressions.getDocumentFieldDerivations(index, store.getRecordMetaData());
                        final LuceneAnalyzerCombinationProvider analyzerSelector = LuceneAnalyzerRegistryImpl.instance().getLuceneAnalyzerCombinationProvider(index, LuceneAnalyzerType.FULL_TEXT, fieldInfos);
                        final QueryParser parser = new QueryParser(field, analyzerSelector.provideQueryAnalyzer((String) comparand).getAnalyzer());
                        return toBoundQuery(parser.parse("\"" + comparand + "\""));
                    } catch (Exception ex) {
                        throw new RecordCoreArgumentException("Unable to parse phrase for query", ex);
                    }
                case IN:
                case TEXT_CONTAINS_ANY: {
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    for (String value : ((List<String>)comparand)) {
                        builder.add(new TermQuery(new Term(field, value)), BooleanClause.Occur.SHOULD);
                    }
                    return toBoundQuery(builder.build());
                }
                case TEXT_CONTAINS_ALL: {
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    for (String value : ((List<String>)comparand)) {
                        builder.add(new TermQuery(new Term(field, value)), BooleanClause.Occur.MUST);
                    }
                    return toBoundQuery(builder.build());
                }
                case TEXT_CONTAINS_ALL_PREFIXES: {
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    for (String value : ((List<String>)comparand)) {
                        builder.add(new PrefixQuery(new Term(field, value)), BooleanClause.Occur.MUST);
                    }
                    return toBoundQuery(builder.build());
                }
                case TEXT_CONTAINS_ANY_PREFIX: {
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    for (String value : ((List<String>)comparand)) {
                        builder.add(new PrefixQuery(new Term(field, value)), BooleanClause.Occur.SHOULD);
                    }
                    return toBoundQuery(builder.build());
                }
                default:
                    throw new RecordCoreException("comparison type not supported for String: " + comparison.getType());
            }
        }
    }

    static class IntQuery extends LuceneQueryFieldComparisonClause {
        private final boolean fieldNameOverride;
        private final String namedFieldSuffix;

        public IntQuery(@Nonnull final LuceneQueryType queryType, @Nonnull String field, @Nonnull LuceneIndexExpressions.DocumentFieldType fieldType, @Nonnull Comparisons.Comparison comparison,
                        final boolean fieldNameOverride, final String namedFieldSuffix) {
            super(queryType, field, fieldType, comparison);
            this.fieldNameOverride = fieldNameOverride;
            this.namedFieldSuffix = namedFieldSuffix;
        }

        @Override
        @SuppressWarnings("unchecked")
        public BoundQuery bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
            Object comparand = comparison.getComparand(store, context);
            if (comparand == null) {
                return toBoundQuery(new MatchNoDocsQuery());
            }
            String appliedFieldName = applyFieldNameConversion(fieldNameOverride, field, namedFieldSuffix, comparand);
            Comparisons.Comparison appliedComparison = applyComparisonConversion(fieldNameOverride, comparison);
            switch (appliedComparison.getType()) {
                case EQUALS:
                    return toBoundQuery(IntPoint.newExactQuery(appliedFieldName, (Integer)comparand));
                case NOT_EQUALS: {
                    int value = (Integer)comparand;
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    builder.add(IntPoint.newRangeQuery(appliedFieldName, Integer.MIN_VALUE, value - 1), BooleanClause.Occur.SHOULD);
                    builder.add(IntPoint.newRangeQuery(appliedFieldName, value + 1, Integer.MAX_VALUE), BooleanClause.Occur.SHOULD);
                    return toBoundQuery(builder.build());
                }
                case LESS_THAN:
                    return toBoundQuery(IntPoint.newRangeQuery(appliedFieldName, Integer.MIN_VALUE, (Integer)comparand - 1));
                case LESS_THAN_OR_EQUALS:
                    return toBoundQuery(IntPoint.newRangeQuery(appliedFieldName, Integer.MIN_VALUE, (Integer)comparand));
                case GREATER_THAN:
                    return toBoundQuery(IntPoint.newRangeQuery(appliedFieldName, (Integer)comparand + 1, Integer.MAX_VALUE));
                case GREATER_THAN_OR_EQUALS:
                    return toBoundQuery(IntPoint.newRangeQuery(appliedFieldName, (Integer)comparand, Integer.MAX_VALUE));
                case IN:
                    return toBoundQuery(IntPoint.newSetQuery(appliedFieldName, ((List<Integer>)comparand)));
                case NOT_NULL:
                    return toBoundQuery(IntPoint.newRangeQuery(appliedFieldName, Integer.MIN_VALUE, Integer.MAX_VALUE));
                default:
                    throw new RecordCoreException("comparison type not supported for Integer: " + comparison.getType());
            }
        }
    }

    static class LongQuery extends LuceneQueryFieldComparisonClause {
        private final boolean fieldNameOverride;
        private final String namedFieldSuffix;

        public LongQuery(@Nonnull final LuceneQueryType queryType, @Nonnull String field, @Nonnull LuceneIndexExpressions.DocumentFieldType fieldType, @Nonnull Comparisons.Comparison comparison,
                         final boolean fieldNameOverride, final String namedFieldSuffix) {
            super(queryType, field, fieldType, comparison);
            this.fieldNameOverride = fieldNameOverride;
            this.namedFieldSuffix = namedFieldSuffix;
        }

        @Override
        @SuppressWarnings("unchecked")
        public BoundQuery bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
            Object comparand = comparison.getComparand(store, context);
            if (comparand == null) {
                return toBoundQuery(new MatchNoDocsQuery());
            }
            String appliedFieldName = applyFieldNameConversion(fieldNameOverride, field, namedFieldSuffix, comparand);
            Comparisons.Comparison appliedComparison = applyComparisonConversion(fieldNameOverride, comparison);
            switch (appliedComparison.getType()) {
                case EQUALS:
                    return toBoundQuery(new LuceneComparisonQuery(LongPoint.newExactQuery(appliedFieldName, (Long)comparand),
                            appliedFieldName,
                            comparison.getType(),
                            comparand));
                case NOT_EQUALS: {
                    long value = (Long)comparand;
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    builder.add(
                            new LuceneComparisonQuery(LongPoint.newRangeQuery(appliedFieldName, Long.MIN_VALUE, value - 1),
                                    appliedFieldName,
                                    comparison.getType(),
                                    comparand),
                            BooleanClause.Occur.SHOULD);
                    builder.add(new LuceneComparisonQuery(LongPoint.newRangeQuery(appliedFieldName, value + 1, Long.MAX_VALUE),
                                    appliedFieldName,
                                    comparison.getType(),
                                    comparand),
                            BooleanClause.Occur.SHOULD);
                    return toBoundQuery(builder.build());
                }
                case LESS_THAN:
                    return toBoundQuery(new LuceneComparisonQuery(LongPoint.newRangeQuery(appliedFieldName, Long.MIN_VALUE, (Long)comparand - 1),
                            appliedFieldName,
                            comparison.getType(),
                            comparand));
                case LESS_THAN_OR_EQUALS:
                    return toBoundQuery(new LuceneComparisonQuery(LongPoint.newRangeQuery(appliedFieldName, Long.MIN_VALUE, (Long)comparand),
                            appliedFieldName,
                            comparison.getType(),
                            comparand));
                case GREATER_THAN:
                    return toBoundQuery(new LuceneComparisonQuery(LongPoint.newRangeQuery(appliedFieldName, (Long)comparand + 1, Long.MAX_VALUE),
                            appliedFieldName,
                            comparison.getType(),
                            comparand));
                case GREATER_THAN_OR_EQUALS:
                    return toBoundQuery(new LuceneComparisonQuery(LongPoint.newRangeQuery(appliedFieldName, (Long)comparand, Long.MAX_VALUE),
                            appliedFieldName,
                            comparison.getType(),
                            comparand));
                case IN:
                    return toBoundQuery(LongPoint.newSetQuery(appliedFieldName, ((List<Long>)comparand)));
                case NOT_NULL:
                    return toBoundQuery(LongPoint.newRangeQuery(appliedFieldName, Long.MIN_VALUE, Long.MAX_VALUE));
                default:
                    throw new RecordCoreException("comparison type not supported for Long: " + comparison.getType());
            }
        }
    }

    static class DoubleQuery extends LuceneQueryFieldComparisonClause {
        private final boolean fieldNameOverride;
        private final String namedFieldSuffix;

        public DoubleQuery(@Nonnull final LuceneQueryType queryType, @Nonnull String field, @Nonnull LuceneIndexExpressions.DocumentFieldType fieldType, @Nonnull Comparisons.Comparison comparison,
                           final boolean fieldNameOverride, final String namedFieldSuffix) {
            super(queryType, field, fieldType, comparison);
            this.fieldNameOverride = fieldNameOverride;
            this.namedFieldSuffix = namedFieldSuffix;
        }

        @Override
        @SuppressWarnings("unchecked")
        public BoundQuery bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
            Object comparand = comparison.getComparand(store, context);
            if (comparand == null) {
                return toBoundQuery(new MatchNoDocsQuery());
            }
            String appliedFieldName = applyFieldNameConversion(fieldNameOverride, field, namedFieldSuffix, comparand);
            Comparisons.Comparison appliedComparison = applyComparisonConversion(fieldNameOverride, comparison);
            switch (appliedComparison.getType()) {
                case EQUALS:
                    return toBoundQuery(DoublePoint.newExactQuery(appliedFieldName, (Double)comparand));
                case NOT_EQUALS: {
                    double value = (Double)comparand;
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    builder.add(DoublePoint.newRangeQuery(appliedFieldName, Double.MIN_VALUE, value - 1), BooleanClause.Occur.SHOULD);
                    builder.add(DoublePoint.newRangeQuery(appliedFieldName, value + 1, Double.MAX_VALUE), BooleanClause.Occur.SHOULD);
                    return toBoundQuery(builder.build());
                }
                case LESS_THAN:
                    return toBoundQuery(DoublePoint.newRangeQuery(appliedFieldName, Double.MIN_VALUE, (Double)comparand - 1));
                case LESS_THAN_OR_EQUALS:
                    return toBoundQuery(DoublePoint.newRangeQuery(appliedFieldName, Double.MIN_VALUE, (Double)comparand));
                case GREATER_THAN:
                    return toBoundQuery(DoublePoint.newRangeQuery(appliedFieldName, (Double)comparand + 1, Double.MAX_VALUE));
                case GREATER_THAN_OR_EQUALS:
                    return toBoundQuery(DoublePoint.newRangeQuery(appliedFieldName, (Double)comparand, Double.MAX_VALUE));
                case IN:
                    return toBoundQuery(DoublePoint.newSetQuery(appliedFieldName, ((List<Double>)comparand)));
                case NOT_NULL:
                    return toBoundQuery(DoublePoint.newRangeQuery(appliedFieldName, Double.MIN_VALUE, Double.MAX_VALUE));
                default:
                    throw new RecordCoreException("comparison type not supported for Double: " + comparison.getType());
            }
        }
    }
}
