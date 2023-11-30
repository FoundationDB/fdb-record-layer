/*
 * LuceneAutoCompleteQueryClause.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Auto complete query clause from string using Lucene search syntax.
 */
@API(API.Status.EXPERIMENTAL)
public class LuceneAutoCompleteQueryClause extends LuceneQueryClause {
    public static final Logger LOGGER = LoggerFactory.getLogger(LuceneAutoCompleteQueryClause.class);

    @Nonnull
    private final String search;
    private final boolean isParameter;
    @Nonnull
    private final Set<String> fields;

    public LuceneAutoCompleteQueryClause(@Nonnull final String search, final boolean isParameter,
                                         @Nonnull final Iterable<String> fields) {
        super(LuceneQueryType.AUTO_COMPLETE);
        this.search = search;
        this.isParameter = isParameter;
        this.fields = ImmutableSet.copyOf(fields);
    }

    @Nonnull
    public String getSearch() {
        return search;
    }

    public boolean isParameter() {
        return isParameter;
    }

    @Override
    public BoundQuery bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
        final String searchArgument =
                isParameter
                ? Verify.verifyNotNull((String)context.getBinding(search))
                : search;

        final boolean phraseQueryNeeded = LuceneAutoCompleteHelpers.isPhraseSearch(searchArgument);
        final String searchKey = LuceneAutoCompleteHelpers.searchKeyFromSearchArgument(searchArgument, phraseQueryNeeded);

        final var fieldDerivationMap = LuceneIndexExpressions.getDocumentFieldDerivations(index, store.getRecordMetaData());
        final var analyzerSelector =
                LuceneAnalyzerRegistryImpl.instance()
                        .getLuceneAnalyzerCombinationProvider(index, LuceneAnalyzerType.AUTO_COMPLETE, fieldDerivationMap);
        final var queryAnalyzer = analyzerSelector.provideQueryAnalyzer(searchKey).getAnalyzer();

        // Determine the tokens from the query key
        final var tokens = new ArrayList<String>();
        final var prefixToken = getQueryTokens(queryAnalyzer, searchKey, tokens);
        final Set<String> tokenSet = Sets.newHashSet(tokens);

        final var finalQuery = phraseQueryNeeded
                               ? buildQueryForPhraseMatching(fields, tokens, prefixToken)
                               : buildQueryForTermsMatching(fields, tokenSet, prefixToken);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(KeyValueLogMessage.build("query for auto-complete")
                    .addKeyAndValue(LogMessageKeys.INDEX_NAME, index.getName())
                    .addKeyAndValue(LogMessageKeys.QUERY, search.replace("\"", "\\\""))
                    .addKeyAndValue("lucene_query", finalQuery)
                    .toString());
        }

        return new BoundQuery(finalQuery);
    }

    @Override
    public void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder, @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        if (isParameter) {
            detailsBuilder.add("param: {{param}}");
            attributeMapBuilder.put("param", Attribute.gml(search));
        } else {
            detailsBuilder.add("search: {{search}}");
            attributeMapBuilder.put("search", Attribute.gml(search));
        }
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, search, isParameter);
    }

    @Override
    public String toString() {
        return "AUTO COMPLETE " + (isParameter ? ("$" + search) : search);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final LuceneAutoCompleteQueryClause that = (LuceneAutoCompleteQueryClause)o;

        if (isParameter != that.isParameter) {
            return false;
        }
        return search.equals(that.search);
    }

    @Override
    public int hashCode() {
        int result = search.hashCode();
        result = 31 * result + (isParameter ? 1 : 0);
        return result;
    }

    /**
     * Extract the query tokens from a string. All the tokens (except the last one) will be added to the
     * {@code tokens} list. The last token is special. If the there is no whitespace following that token,
     * this indicates that this is an incomplete prefix of a token that will be completed by the query.
     * If there is whitespace following that token, then it is assumed that token is complete and is added
     * to the {@code tokens} list. The final token will be returned by this method if and only if we are in
     * the former case.
     *
     * @param searchKey the phrase to find completions of
     * @param tokens the list to insert all complete tokens extracted from the query phrase
     * @return the final token if it needs to be added as a "prefix" component to the final query
     */
    @Nullable
    @VisibleForTesting
    static String getQueryTokens(Analyzer queryAnalyzer, String searchKey, @Nonnull List<String> tokens) {
        String prefixToken = null;
        try (TokenStream ts = queryAnalyzer.tokenStream("", new StringReader(searchKey))) {
            ts.reset();
            final CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            final OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
            String lastToken = null;
            int maxEndOffset = -1;
            while (ts.incrementToken()) {
                if (lastToken != null) {
                    tokens.add(lastToken);
                }
                lastToken = termAtt.toString();
                if (lastToken != null) {
                    maxEndOffset = Math.max(maxEndOffset, offsetAtt.endOffset());
                }
            }
            ts.end();

            if (lastToken != null) {
                if (maxEndOffset == offsetAtt.endOffset()) {
                    // Use PrefixQuery (or the ngram equivalent) when
                    // there was no trailing discarded chars in the
                    // string (e.g. whitespace), so that if query does
                    // not end with a space we show prefix matches for
                    // that token:
                    prefixToken = lastToken;
                } else {
                    // Use TermQuery for an exact match if there were
                    // trailing discarded chars (e.g. whitespace), so
                    // that if query ends with a space we only show
                    // exact matches for that term:
                    tokens.add(lastToken);
                }
            }
        } catch (IOException iOE) {
            // This cannot happen has the token stream is created on an in-memory reader.
            throw new RecordCoreException("in-memory tokenization threw an IOException", iOE);
        }
        return prefixToken;
    }

    @Nonnull
    public static Query buildQueryForPhraseMatching(@Nonnull Collection<String> fieldNames,
                                                    @Nonnull List<String> matchedTokens,
                                                    @Nullable String prefixToken) {
        // Construct a query that is essentially:
        //  - in any field,
        //  - the phrase must occur (with possibly the last token in the phrase as a prefix)
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();

        for (String field : fieldNames) {
            PhraseQuery.Builder phraseQueryBuilder = new PhraseQuery.Builder();
            for (String token : matchedTokens) {
                phraseQueryBuilder.add(new Term(field, token));
            }
            Query fieldQuery;
            if (prefixToken == null) {
                fieldQuery = phraseQueryBuilder.build();
            } else {
                fieldQuery = getPhrasePrefixQuery(field, phraseQueryBuilder.build(), prefixToken);
            }
            queryBuilder.add(fieldQuery, BooleanClause.Occur.SHOULD);
        }

        queryBuilder.setMinimumNumberShouldMatch(1);
        return queryBuilder.build();
    }

    @Nonnull
    private static Query buildQueryForTermsMatching(@Nonnull Collection<String> fieldNames,
                                                    @Nonnull Set<String> tokenSet,
                                                    @Nullable String prefixToken) {
        // Construct a query that is essentially:
        //  - in any field,
        //  - all of the tokens must occur (with the last one as a prefix)
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();

        for (String field : fieldNames) {
            BooleanQuery.Builder fieldQuery = new BooleanQuery.Builder();
            for (String token : tokenSet) {
                fieldQuery.add(new TermQuery(new Term(field, token)), BooleanClause.Occur.MUST);
            }
            if (prefixToken != null) {
                fieldQuery.add(new PrefixQuery(new Term(field, prefixToken)), BooleanClause.Occur.MUST);
            }
            queryBuilder.add(fieldQuery.build(), BooleanClause.Occur.SHOULD);
        }
        queryBuilder.setMinimumNumberShouldMatch(1);
        return queryBuilder.build();
    }

    @Nonnull
    private static Query getPhrasePrefixQuery(@Nonnull String fieldName, @Nonnull PhraseQuery phraseQuery, @Nonnull String lastToken) {
        Term[] terms = phraseQuery.getTerms();
        SpanNearQuery.Builder spanQuery = new SpanNearQuery.Builder(fieldName, true); // field
        for (Term term : terms) {
            spanQuery.addClause(new SpanTermQuery(term));
        }
        SpanQuery lastTokenQuery = new SpanMultiTermQueryWrapper<>(new PrefixQuery(new Term(fieldName, lastToken)));
        FieldMaskingSpanQuery fieldMask = new FieldMaskingSpanQuery(lastTokenQuery, fieldName);
        spanQuery.addClause(fieldMask);

        return spanQuery.build();
    }
}
