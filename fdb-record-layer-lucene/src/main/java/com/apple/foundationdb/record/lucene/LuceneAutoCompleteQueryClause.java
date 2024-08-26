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
import com.apple.foundationdb.record.lucene.search.LuceneQueryParserFactory;
import com.apple.foundationdb.record.lucene.search.LuceneQueryParserFactoryProvider;
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
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiPhraseQuery;
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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Auto complete query clause from string using Lucene search syntax.
 */
@API(API.Status.EXPERIMENTAL)
public class LuceneAutoCompleteQueryClause extends LuceneQueryClause {
    public static final Logger LOGGER = LoggerFactory.getLogger(LuceneAutoCompleteQueryClause.class);

    // Used as a marker at the end to capture the stop-word gaps between the initial phrase and the end prefix when parsing the search key
    private static final String NONSTOPWORD = "$nonstopword";

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
        long startNanos = System.nanoTime();
        final String searchArgument =
                isParameter
                ? Verify.verifyNotNull((String)context.getBinding(search))
                : search;

        long searchArgumentNanos = System.nanoTime();
        final boolean phraseQueryNeeded = LuceneAutoCompleteHelpers.isPhraseSearch(searchArgument);
        long isPhraseSearchNanos = System.nanoTime();
        final String searchKey = LuceneAutoCompleteHelpers.searchKeyFromSearchArgument(searchArgument, phraseQueryNeeded);
        long searchKeyNanos = System.nanoTime();
        final var fieldDerivationMap = LuceneIndexExpressions.getDocumentFieldDerivations(index, store.getRecordMetaData());
        long fieldDerivationMapNanos = System.nanoTime();
        // The analyzer used to construct the Lucene query should be the FULL_TEXT-index one in order to match how the text was indexed
        final var analyzerSelector =
                LuceneAnalyzerRegistryImpl.instance()
                        .getLuceneAnalyzerCombinationProvider(index, LuceneAnalyzerType.FULL_TEXT, fieldDerivationMap);
        long analyzerSelectorNanos = System.nanoTime();
        final Map<String, PointsConfig> pointsConfigMap = LuceneIndexExpressions.constructPointConfigMap(store, index);
        long pointConfigMapNanos = System.nanoTime();
        LuceneQueryParserFactory parserFactory = LuceneQueryParserFactoryProvider.instance().getParserFactory();
        long parserFactoryNanos = System.nanoTime();
        final QueryParser parser = parserFactory.createMultiFieldQueryParser(fields.toArray(new String[0]),
                analyzerSelector.provideIndexAnalyzer(searchKey).getAnalyzer(), pointsConfigMap);
        long createParserNanos = System.nanoTime();


        final var finalQuery = phraseQueryNeeded
                               ? buildQueryForPhraseMatching(parser, fields, searchKey)
                               : buildQueryForTermsMatching(analyzerSelector.provideIndexAnalyzer(searchKey).getAnalyzer(), fields, searchKey);
        if (LOGGER.isDebugEnabled()) {
            long finalQueryNanos = System.nanoTime();
            LOGGER.debug(KeyValueLogMessage.build("query for auto-complete")
                    .addKeyAndValue(LogMessageKeys.INDEX_NAME, index.getName())
                    .addKeyAndValue(LogMessageKeys.QUERY, search.replace("\"", "\\\""))
                    .addKeyAndValue("lucene_query", finalQuery)
                    // Adding a bunch of timing here because we believe this code sometimes takes a long time
                    // (more than 5 seconds) to initialize, but haven't been able to reproduce, hopefully this will
                    // help limit the scope of the investigation. These metrics shouldn't need to stick around longterm.
                    .addKeyAndValue("searchArgumentUsec", TimeUnit.NANOSECONDS.toMicros(searchArgumentNanos - startNanos))
                    .addKeyAndValue("isPhraseSearchUsec", TimeUnit.NANOSECONDS.toMicros(isPhraseSearchNanos - searchArgumentNanos))
                    .addKeyAndValue("searchKeyUsec", TimeUnit.NANOSECONDS.toMicros(searchKeyNanos - isPhraseSearchNanos))
                    .addKeyAndValue("fieldDerivationMapUsec", TimeUnit.NANOSECONDS.toMicros(fieldDerivationMapNanos - searchKeyNanos))
                    .addKeyAndValue("analyzerSelectorUsec", TimeUnit.NANOSECONDS.toMicros(analyzerSelectorNanos - fieldDerivationMapNanos))
                    .addKeyAndValue("pointConfigMapUsec", TimeUnit.NANOSECONDS.toMicros(pointConfigMapNanos - analyzerSelectorNanos))
                    .addKeyAndValue("parserFactoryUsec", TimeUnit.NANOSECONDS.toMicros(parserFactoryNanos - pointConfigMapNanos))
                    .addKeyAndValue("createParserUsec", TimeUnit.NANOSECONDS.toMicros(createParserNanos - parserFactoryNanos))
                    .addKeyAndValue("finalQueryUsec", TimeUnit.NANOSECONDS.toMicros(finalQueryNanos - createParserNanos))
                    .addKeyAndValue("totalUsec", TimeUnit.NANOSECONDS.toMicros(finalQueryNanos - startNanos))
                    .toString());
        }

        return toBoundQuery(finalQuery);
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

    /**
     * Constructs a query to match a phrase search, with the last token treated as a prefix.
     * This is to match "united states of" against "United States of America"
     *
     * @param parser Lucene parser with given fields and analyzer
     * @param fieldNames the fields to match against
     * @param phrase the phrase part of the search key
     * @param prefix the prefix (last token) of the search key
     * @param useGapForPrefix option to represent the last token as a gap (if it's a stopword)
     * @return a Lucene Query that matches phrase using the last token as a prefix
     */
    @Nonnull
    public static Query buildPhraseQueryWithPrefix(@Nonnull QueryParser parser,
                                                   @Nonnull Collection<String> fieldNames,
                                                   @Nonnull String phrase,
                                                   @Nullable String prefix,
                                                   final boolean useGapForPrefix) {

        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for (String field : fieldNames) {
            Query phraseQuery = parser.createPhraseQuery(field, phrase + " " + NONSTOPWORD);
            if (!(phraseQuery instanceof TermQuery || phraseQuery instanceof PhraseQuery || phraseQuery instanceof MultiPhraseQuery)) {
                throw new RecordCoreException("Unsupported phrase type in auto-complete: " + phraseQuery.getClass().getName());
            }

            SpanNearQuery.Builder spanQuery = new SpanNearQuery.Builder(field, true).setSlop(0);

            if (phraseQuery instanceof PhraseQuery) {
                PhraseQuery pq = (PhraseQuery) phraseQuery;
                Term[] terms = pq.getTerms();
                buildSpanQuery(spanQuery, terms, pq.getPositions(), useGapForPrefix);
            } else if (phraseQuery instanceof MultiPhraseQuery) {
                MultiPhraseQuery mpq = (MultiPhraseQuery) phraseQuery;
                final Term[][] termArrays = mpq.getTermArrays();
                final List<Term> terms = new ArrayList<>();
                /*
                flatten the arrays by using the last term in each position.
                example: In the case of WordDelimiterFilter with PRESERVE_ORIGINAL, bob@cat.com would result in:
                          {bob@cat.com, bob}, {cat}, {com}
                         Using "bob cat com" in the query would be able to match the indexed text
                 */
                for (Term[] t : termArrays) {
                    terms.add(t[t.length - 1]);
                }
                buildSpanQuery(spanQuery, terms.toArray(Term[]::new), mpq.getPositions(), useGapForPrefix);
            } else {
                // if NONSTOPWORD is the only term, then the rest of the phrase must be stop words
                spanQuery.addGap(1);
            }

            if (!useGapForPrefix && prefix != null) {
                SpanQuery lastTokenQuery = new SpanMultiTermQueryWrapper<>(new PrefixQuery(new Term(field, prefix)));
                FieldMaskingSpanQuery fieldMask = new FieldMaskingSpanQuery(lastTokenQuery, field);
                spanQuery.addClause(fieldMask);
            }

            queryBuilder.add(spanQuery.build(), BooleanClause.Occur.SHOULD);
        }
        queryBuilder.setMinimumNumberShouldMatch(1);
        return queryBuilder.build();
    }

    private static void buildSpanQuery(final SpanNearQuery.Builder spanQuery,
                                       final Term[] terms,
                                       final int[] positions,
                                       final boolean useGapForPrefix) {
        int prevPosition = -1;
        boolean endsWithGap = false;
        for (int i = 0; i < positions.length; i++) {
            if (positions[i] - 1 > prevPosition) {
                spanQuery.addGap(positions[i] - 1 - prevPosition);
                endsWithGap = true;
            }

            if (i < positions.length - 1) {
                spanQuery.addClause(new SpanTermQuery(terms[i]));
                endsWithGap = false;
            }

            prevPosition = positions[i];
        }
        if (useGapForPrefix && !endsWithGap) {
            spanQuery.addGap(1);
        }
    }

    @Nonnull
    public static Query buildQueryForPhraseMatching(@Nonnull QueryParser parser,
                                                    @Nonnull Collection<String> fieldNames,
                                                    @Nonnull String searchKey) {
        // Construct a query that is essentially:
        //  - in any field,
        //  - the phrase must occur (with possibly the last token in the phrase as a prefix)
        final String lowercasedSearchKey = searchKey.toLowerCase(Locale.ROOT);
        List<String> tokens = new ArrayList<>();
        String phrase = null;
        String prefix = getQueryTokens(new AutoCompleteAnalyzer(), lowercasedSearchKey, tokens);

        if (!tokens.isEmpty()) {
            final String lastToken = tokens.get(tokens.size() - 1);
            final int phraseEnd = lowercasedSearchKey.lastIndexOf(lastToken.toLowerCase(Locale.ROOT)) + lastToken.length();
            phrase = lowercasedSearchKey.substring(0, phraseEnd);
        }

        if (phrase == null && prefix == null) {
            throw new RecordCoreException("Invalid auto-complete input: empty key");
        }

        if (phrase != null) {
            BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
            booleanQueryBuilder.add(buildPhraseQueryWithPrefix(parser, fieldNames, phrase, prefix, false), BooleanClause.Occur.SHOULD);
            if (prefix != null && isStopWord(parser, prefix)) {
                booleanQueryBuilder.add(buildPhraseQueryWithPrefix(parser, fieldNames, phrase, prefix, true), BooleanClause.Occur.SHOULD);
            }
            booleanQueryBuilder.setMinimumNumberShouldMatch(1);
            return booleanQueryBuilder.build();
        } else {
            try {
                return parser.parse(prefix + "*");
            } catch (final Exception ioe) {
                throw new RecordCoreException("Unable to parse search given for query", ioe);
            }
        }
    }

    private static boolean isStopWord(@Nonnull QueryParser queryParser, @Nonnull String prefix) {
        try {
            final Query query = queryParser.parse(prefix);
            if (query instanceof BooleanQuery) {
                return ((BooleanQuery) query).clauses().isEmpty();
            } else {
                return false;
            }
        } catch (final Exception ioe) {
            return false;
        }
    }

    @Nonnull
    private static Query buildQueryForTermsMatching(@Nonnull Analyzer queryAnalyzer,
                                                    @Nonnull Collection<String> fieldNames,
                                                    @Nonnull String searchKey) {
        // Construct a query that is essentially:
        //  - in any field,
        //  - all of the tokens must occur (with the last one as a prefix)
        final var tokens = new ArrayList<String>();
        final var prefixToken = getQueryTokens(queryAnalyzer, searchKey, tokens);
        final Set<String> tokenSet = Sets.newHashSet(tokens);

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
}
