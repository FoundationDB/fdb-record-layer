/*
 * LuceneAutoCompleteHelpers.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import org.jspecify.annotations.Nullable;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * This class provides some helpers for auto-complete functionality using Lucene auto complete suggestion lookup.
 */
public class LuceneAutoCompleteHelpers {
    /**
     * Extract the query tokens from a string. All tokens (except the last one) are added to the
     * {@code tokens} list. The last token is special. If the there is no whitespace following that token,
     * this indicates that this is an incomplete prefix of a token that will be completed by the query.
     * If there is whitespace following that token, then it is assumed that token is complete and is added
     * to the {@code tokens} list. The final token will be returned by this method if and only if we are in
     * the former case.
     *
     * @param queryAnalyzer a query analyzer
     * @param searchKey the phrase to find completions of
     * @return the token info comprised of query tokens and a final token if it needs to be added as a "prefix" component
     *         to the final query
     */
    public static AutoCompleteTokens getQueryTokens(Analyzer queryAnalyzer, String searchKey) {
        final ImmutableList.Builder<String> tokensBuilder = ImmutableList.builder();
        String prefixToken = null;
        try (TokenStream ts = queryAnalyzer.tokenStream("", new StringReader(searchKey))) {
            ts.reset();
            final CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            final OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
            String lastToken = null;
            int maxEndOffset = -1;
            while (ts.incrementToken()) {
                if (lastToken != null) {
                    tokensBuilder.add(lastToken);
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
                    tokensBuilder.add(lastToken);
                }
            }
        } catch (final IOException ioException) {
            throw LuceneExceptions.toRecordCoreException("string reader throw IOException", ioException);
        }
        return new AutoCompleteTokens(tokensBuilder.build(), prefixToken == null ? ImmutableSet.of() : ImmutableSet.of(prefixToken));
    }

    public static List<String> computeAllMatches(String fieldName, Analyzer queryAnalyzer,
                                                 String text, AutoCompleteTokens tokens,
                                                 final int numAdditionalTokens) {
        final var resultBuilder = ImmutableList.<TermAcceptor>builder();
        final var acceptors = Lists.<TermAcceptor>newArrayList();
        final var queryTokens = tokens.getQueryTokensAsSet();
        final var seenQueryTokens = Sets.newHashSetWithExpectedSize(queryTokens.size());
        final var prefixTokens = tokens.getPrefixTokens();
        final var seenPrefixTokens = Sets.newHashSetWithExpectedSize(prefixTokens.size());
        try (TokenStream ts = queryAnalyzer.tokenStream(fieldName, new StringReader(text))) {
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
            ts.reset();
            int upto = 0;
            while (ts.incrementToken()) {
                String token = termAtt.toString();
                int startOffset = offsetAtt.startOffset();
                int endOffset = offsetAtt.endOffset();
                if (upto < startOffset) {
                    upto = startOffset;
                } else if (upto > startOffset) {
                    continue;
                }
                
                acceptors.removeIf(acceptor -> !acceptor.acceptAdditionalToken(endOffset));

                if (queryTokens.contains(token)) {
                    seenQueryTokens.add(token);
                    final var termAcceptor = new TermAcceptor(startOffset, endOffset, numAdditionalTokens);
                    resultBuilder.add(termAcceptor);
                    if (numAdditionalTokens > 0) {
                        acceptors.add(termAcceptor);
                    }
                    // Token matches.
                    upto = endOffset;
                } else {
                    for (String prefixToken : prefixTokens) {
                        if (token.startsWith(prefixToken)) {
                            upto = endOffset;
                            seenPrefixTokens.add(prefixToken);
                            final var termAcceptor = new TermAcceptor(startOffset, endOffset, numAdditionalTokens);
                            resultBuilder.add(termAcceptor);
                            if (numAdditionalTokens > 0) {
                                acceptors.add(termAcceptor);
                            }
                            break;
                        }
                    }
                }
            }
            ts.end();

            if (!seenQueryTokens.isEmpty() || !seenPrefixTokens.isEmpty()) {
                return resultBuilder.build().stream().map(acceptor -> acceptor.getAcceptedString(text)).collect(ImmutableList.toImmutableList());
            } else {
                return ImmutableList.of();
            }

        } catch (IOException ioException) {
            throw LuceneExceptions.toRecordCoreException("token stream threw an io exception", ioException);
        }
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static List<String> computeAllMatchesForPhrase(final String fieldName, final Analyzer queryAnalyzer,
                                                          final String text, final AutoCompleteTokens tokens,
                                                          final int numAdditionalTokens) {
        final var queryTokens = tokens.getQueryTokens();
        final var prefixTokens = tokens.getPrefixTokens();
        Preconditions.checkArgument(prefixTokens.size() <= 1);
        final var prefixToken = prefixTokens.isEmpty() ? null : Iterables.getOnlyElement(prefixTokens);
        final var activeAcceptors = Lists.<PhraseAcceptor>newArrayList();

        try (TokenStream ts = queryAnalyzer.tokenStream(fieldName, new StringReader(text))) {
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
            ts.reset();
            int upto = 0;

            while (ts.incrementToken()) {
                String token = termAtt.toString();
                int startOffset = offsetAtt.startOffset();
                int endOffset = offsetAtt.endOffset();
                if (upto > startOffset) {
                    continue;
                }

                final var matchedSubstring = text.substring(startOffset, endOffset);
                activeAcceptors.removeIf(currentAcceptor -> !currentAcceptor.accept(token, matchedSubstring, endOffset));

                // let's see if there is another partial match
                final var phraseAcceptor = PhraseAcceptor.acceptFirstToken(token, matchedSubstring, queryTokens, prefixToken, numAdditionalTokens, startOffset, endOffset);
                if (phraseAcceptor != null) {
                    activeAcceptors.add(phraseAcceptor);
                }

                upto = offsetAtt.endOffset();
            }
            ts.end();

            return activeAcceptors.stream()
                    .filter(PhraseAcceptor::isEndState)
                    .map(phraseAcceptor -> phraseAcceptor.getAcceptedPhrase(text))
                    .collect(ImmutableList.toImmutableList());
        } catch (IOException ioException) {
            throw LuceneExceptions.toRecordCoreException("token stream threw an io exception", ioException);
        }
    }

    private static class TermAcceptor {
        private final int startOffset;
        private int endOffset;
        private int additionalTokenCount;

        public TermAcceptor(final int startOffset, final int endOffset, final int additionalTokenCount) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.additionalTokenCount = additionalTokenCount;
        }

        public boolean acceptAdditionalToken(final int currentEndOffset) {
            if (additionalTokenCount > 0) {
                additionalTokenCount --;
                endOffset = currentEndOffset;
            }
            return additionalTokenCount > 0;
        }

        public String getAcceptedString(final String originalString) {
            return originalString.substring(startOffset, endOffset);
        }
    }

    private static class PhraseAcceptor {

        private enum AcceptState {
            NOT_ACCEPTED,
            ACCEPTED_QUERY_TOKEN,
            ACCEPTED_PREFIX_TOKEN;
        }

        private final List<String> acceptedTokens;
        private final PeekingIterator<String> queryTokensRemainingIterator;
        @Nullable
        private final String prefixToken;
        private final int startOffset;
        private int endOffset;

        private int additionalTokenCount;

        private boolean isEndState;

        private PhraseAcceptor(final Iterator<String> queryTokensRemainingIterator, @Nullable final String prefixToken,
                               final List<String> acceptedTokens, final boolean isEndState, final int startOffset,
                               final int endOffset, final int additionalTokenCount) {
            this.queryTokensRemainingIterator = Iterators.peekingIterator(queryTokensRemainingIterator);
            this.prefixToken = prefixToken;
            this.acceptedTokens = acceptedTokens;
            this.isEndState = isEndState;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.additionalTokenCount = additionalTokenCount;
        }

        public Iterator<String> getQueryTokensRemainingIterator() {
            return queryTokensRemainingIterator;
        }

        @Nullable
        public String getPrefixToken() {
            return prefixToken;
        }

        public List<String> getAcceptedTokens() {
            return acceptedTokens;
        }

        public String getAcceptedPhrase(final String originalString) {
            return originalString.substring(startOffset, endOffset);
        }

        public boolean isEndState() {
            return isEndState;
        }

        public boolean accept(final String currentToken, final String currentMatchedString, final int currentEndOffset) {
            if (isEndState) {
                if (additionalTokenCount > 0) {
                    additionalTokenCount --;
                    acceptedTokens.add(currentMatchedString);
                    endOffset = currentEndOffset;
                }
                return true;
            }

            final var currentQueryToken =
                    queryTokensRemainingIterator.hasNext()
                    ? queryTokensRemainingIterator.peek()
                    : null;
            final var acceptState = acceptToken(currentToken, currentQueryToken, prefixToken);

            switch (acceptState) {
                case NOT_ACCEPTED:
                    return false;
                case ACCEPTED_QUERY_TOKEN:
                    acceptedTokens.add(currentMatchedString);
                    queryTokensRemainingIterator.next();
                    if (!queryTokensRemainingIterator.hasNext() && prefixToken == null) {
                        isEndState = true;
                        endOffset = currentEndOffset;
                    }
                    return true;
                case ACCEPTED_PREFIX_TOKEN:
                    acceptedTokens.add(currentMatchedString);
                    isEndState = true;
                    endOffset = currentEndOffset;
                    return true;
                default:
                    throw new RecordCoreException("unexpected accept state");
            }
        }

        @Nullable
        public static PhraseAcceptor acceptFirstToken(final String currentToken, final String currentMatchedString,
                                                      final List<String> queryTokens, @Nullable final String prefixToken,
                                                      final int additionalTokenCount, final int startOffset, final int currentEndOffset) {
            final var firstQueryToken = queryTokens.isEmpty() ? null : queryTokens.get(0);
            final var acceptState = acceptToken(currentToken, firstQueryToken, prefixToken);

            switch (acceptState) {
                case NOT_ACCEPTED:
                    return null;
                case ACCEPTED_QUERY_TOKEN:
                    final var queryTokensIterator = queryTokens.iterator();
                    queryTokensIterator.next(); // skip the first item
                    final var isEndState = !queryTokensIterator.hasNext() && prefixToken == null;
                    return new PhraseAcceptor(queryTokensIterator, prefixToken, Lists.newArrayList(currentMatchedString), isEndState, startOffset, isEndState ? currentEndOffset : -1, additionalTokenCount);
                case ACCEPTED_PREFIX_TOKEN:
                    return new PhraseAcceptor(queryTokens.iterator(), prefixToken, Lists.newArrayList(currentMatchedString), true, startOffset, currentEndOffset, additionalTokenCount);
                default:
                    throw new RecordCoreException("unexpected accept state");
            }
        }

        private static AcceptState acceptToken(final String currentToken, @Nullable final String currentQueryToken, @Nullable final String prefixToken) {
            Preconditions.checkArgument(currentQueryToken != null || prefixToken != null);
            if (currentQueryToken != null) {
                return currentToken.equals(currentQueryToken) ? AcceptState.ACCEPTED_QUERY_TOKEN : AcceptState.NOT_ACCEPTED;
            }
            return currentToken.startsWith(prefixToken) ? AcceptState.ACCEPTED_PREFIX_TOKEN : AcceptState.NOT_ACCEPTED;
        }
    }

    public static boolean isPhraseSearch(final String search) {
        return search.startsWith("\"") && search.endsWith("\"");
    }

    public static String searchKeyFromSearchArgument(final String search) {
        return searchKeyFromSearchArgument(search, isPhraseSearch(search));
    }

    public static String searchKeyFromSearchArgument(final String search, final boolean isPhraseSearch) {
        return isPhraseSearch ? search.substring(1, search.length() - 1) : search;
    }

    /*
     * For auto-complete matches, use the index analyzer selector
     * Returns an analyzerSelector for full text
     */
    @Nullable
    public static <M extends Message> LuceneAnalyzerCombinationProvider getAutoCompletedMatchesAnalyzerSelector(@Nullable FDBQueriedRecord<M> queriedRecord) {
        if (queriedRecord == null) {
            return null;
        }
        final var indexEntry = queriedRecord.getIndexEntry();
        if (!(indexEntry instanceof LuceneRecordCursor.ScoreDocIndexEntry)) {
            return null;
        }
        final var docIndexEntry = (LuceneRecordCursor.ScoreDocIndexEntry)indexEntry;
        return docIndexEntry.getAnalyzerSelector();
    }

    @Nullable
    public static <M extends Message> LuceneAnalyzerCombinationProvider getAutoCompleteAnalyzerSelector(@Nullable FDBQueriedRecord<M> queriedRecord) {
        if (queriedRecord == null) {
            return null;
        }
        final var indexEntry = queriedRecord.getIndexEntry();
        if (!(indexEntry instanceof LuceneRecordCursor.ScoreDocIndexEntry)) {
            return null;
        }
        final var docIndexEntry = (LuceneRecordCursor.ScoreDocIndexEntry)indexEntry;
        return docIndexEntry.getAutoCompleteAnalyzerSelector();
    }

    /**
     * Helper class to capture token information synthesized from a search key.
     */
    public static class AutoCompleteTokens {
        private final List<String> queryTokens;
        private final Set<String> prefixTokens;

        private final Supplier<Set<String>> queryTokensAsSetSupplier;

        public AutoCompleteTokens(final Collection<String> queryTokens, final Set<String> prefixTokens) {
            this.queryTokens = ImmutableList.copyOf(queryTokens);
            this.prefixTokens = ImmutableSet.copyOf(prefixTokens);
            this.queryTokensAsSetSupplier = Suppliers.memoize(() -> ImmutableSet.copyOf(queryTokens));
        }

        public List<String> getQueryTokens() {
            return queryTokens;
        }

        public Set<String> getQueryTokensAsSet() {
            return queryTokensAsSetSupplier.get();
        }

        public Set<String> getPrefixTokens() {
            return prefixTokens;
        }

        @Nullable
        public String getPrefixTokenOrNull() {
            return prefixTokens.isEmpty() ? null : Iterables.getOnlyElement(prefixTokens);
        }
    }
}
