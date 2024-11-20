/*
 * LuceneHighlighting.java
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

package com.apple.foundationdb.record.lucene.highlight;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerCombinationProvider;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerWrapper;
import com.apple.foundationdb.record.lucene.LuceneDocumentFromRecord;
import com.apple.foundationdb.record.lucene.LuceneExceptions;
import com.apple.foundationdb.record.lucene.LuceneIndexExpressions;
import com.apple.foundationdb.record.lucene.LuceneRecordCursor;
import com.apple.foundationdb.record.lucene.LuceneScanQueryParameters;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.google.protobuf.Message;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Helper class for highlighting search matches.
 */
public class LuceneHighlighting {
    private LuceneHighlighting() {
    }

    private static Set<String> getPrefixTerms(@Nonnull Set<String> terms) {
        Set<String> result = Collections.emptySet();
        Iterator<String> iter = terms.iterator();
        while (iter.hasNext()) {
            String term = iter.next();
            if (term.endsWith("*")) {
                term = term.substring(0, term.length() - 1);
                if (result.isEmpty()) {
                    result = new HashSet<>();
                }
                result.add(term);
                iter.remove();
            }
        }
        return result;
    }

    private static boolean isMatch(@Nonnull String candidate, @Nonnull Set<String> terms, @Nonnull Set<String> prefixes) {
        for (String term : terms) {
            if (StringUtils.containsIgnoreCase(candidate, term)) {
                return true;
            }
        }
        for (String term : prefixes) {
            if (StringUtils.containsIgnoreCase(candidate, term)) {
                return true;
            }
        }
        return false;
    }

    @Nonnull
    private static Set<String> getFieldTerms(@Nonnull Map<String, Set<String>> termMap, @Nonnull String fieldName) {
        final Set<String> terms = new HashSet<>();
        final Set<String> forField = termMap.get(fieldName);
        if (forField != null) {
            terms.addAll(forField);
        }
        final Set<String> forAll = termMap.get("");
        if (forAll != null) {
            terms.addAll(forAll);
        }
        return terms;
    }

    @Nonnull
    public static <M extends Message> List<HighlightedTerm> highlightedTermsForMessage(@Nullable FDBQueriedRecord<M> queriedRecord,
                                                                                       @Nullable String nestedName) {
        if (queriedRecord == null) {
            return Collections.emptyList();
        }
        IndexEntry indexEntry = queriedRecord.getIndexEntry();
        if (!(indexEntry instanceof LuceneRecordCursor.ScoreDocIndexEntry)) {
            return Collections.emptyList();
        }
        LuceneRecordCursor.ScoreDocIndexEntry docIndexEntry = (LuceneRecordCursor.ScoreDocIndexEntry)indexEntry;
        if (docIndexEntry.getLuceneQueryHighlightParameters() == null) {
            return Collections.emptyList();
        }

        return highlightedTermsForMessage(queriedRecord, queriedRecord.getRecord(), nestedName,
                docIndexEntry.getIndexKey(), docIndexEntry.getTermMap(), docIndexEntry.getAnalyzerSelector(), docIndexEntry.getLuceneQueryHighlightParameters());
    }

    // Modify the Lucene fields of a record message with highlighting the terms from the given termMap

    @Nonnull
    public static <M extends Message> List<HighlightedTerm> highlightedTermsForMessage(@Nonnull FDBRecord<M> rec, M message,
                                                                                       @Nullable String nestedName,
                                                                                       @Nonnull KeyExpression expression,
                                                                                       @Nonnull Map<String, Set<String>> termMap,
                                                                                       @Nonnull LuceneAnalyzerCombinationProvider analyzerSelector,
                                                                                       @Nonnull LuceneScanQueryParameters.LuceneQueryHighlightParameters luceneQueryHighlightParameters) {
        if (nestedName != null) {
            expression = getNestedFields(expression, nestedName);
            if (expression == null) {
                return Collections.emptyList();
            }
        }


        List<HighlightedTerm> result = new ArrayList<>();
        LuceneIndexExpressions.getFields(expression, new LuceneDocumentFromRecord.FDBRecordSource<>(rec, message),
                (source, fieldName, value, type, fieldNameOverride, namedFieldPath, namedFieldSuffix, stored, sorted, overriddenKeyRanges, groupingKeyIndex, keyIndex, fieldConfigsIgnored) -> {
                    if (type != LuceneIndexExpressions.DocumentFieldType.TEXT) {
                        return;
                    }
                    String termName = nestedName == null ? fieldName : nestedName + "_" + fieldName;
                    Set<String> terms = getFieldTerms(termMap, termName);
                    if (terms.isEmpty()) {
                        return;
                    }

                    Set<String> prefixes = getPrefixTerms(terms);
                    if (value instanceof String && isMatch((String)value, terms, prefixes)) {
                        Object o = highlight(analyzerSelector, luceneQueryHighlightParameters, fieldName, (String)value, termName);
                        if (o instanceof HighlightedTerm) {
                            result.add((HighlightedTerm)o);
                        }
                    }
                }, null);
        return result;
    }

    @Nullable
    private static Object highlight(final @Nonnull LuceneAnalyzerCombinationProvider analyzerSelector,
                                    final @Nonnull LuceneScanQueryParameters.LuceneQueryHighlightParameters luceneQueryHighlightParameters,
                                    final String fieldName,
                                    final String value,
                                    final String termName) {
        final LuceneAnalyzerWrapper queryAnalyzer = analyzerSelector.provideIndexAnalyzer(value);
        UnifiedHighlighter highlighter = makeHighlighter(fieldName, queryAnalyzer.getAnalyzer(), luceneQueryHighlightParameters.getSnippedSize());
        try {
            return highlighter.highlightWithoutSearcher(termName, luceneQueryHighlightParameters.getQuery(), value, luceneQueryHighlightParameters.getMaxMatchCount());
        } catch (IOException e) {
            throw LuceneExceptions.toRecordCoreException("Unexpected error processing highlights", e);
        }
    }

    @Nullable
    private static KeyExpression getNestedFields(@Nonnull KeyExpression expression, @Nonnull String nestedName) {
        if (expression instanceof GroupingKeyExpression) {
            expression = ((GroupingKeyExpression)expression).getGroupedSubKey();
        }
        List<KeyExpression> expressions = new ArrayList<>();
        if (expression instanceof ThenKeyExpression) {
            for (KeyExpression child : ((ThenKeyExpression)expression).getChildren()) {
                if (child instanceof NestingKeyExpression && ((NestingKeyExpression)child).getParent().getFieldName().equals(nestedName)) {
                    expressions.add(((NestingKeyExpression)child).getChild());
                }
            }
        }
        if (expressions.isEmpty()) {
            return null;
        } else if (expressions.size() == 1) {
            return expressions.get(0);
        } else {
            return Key.Expressions.concat(expressions);
        }
    }

    public static UnifiedHighlighter makeHighlighter(String fieldName, Analyzer analyzer, int snippetSize) {
        /*
         * Factory method to make correct highlighters for Lucene. The returned highlighter will
         * process at the word level, and will format passaged to return HighlightedTerm objects.
         */
        UnifiedHighlighter highlighter = new UnifiedHighlighter(null, analyzer);
        Supplier<BreakIterator> breakIterSupplier = BreakIterator::getWordInstance;
        highlighter.setBreakIterator(breakIterSupplier);
        if (snippetSize > 0) {
            highlighter.setFormatter(new SnippetFormatter(fieldName, breakIterSupplier, snippetSize));
        } else {
            highlighter.setFormatter(new WholeTextFormatter(fieldName));
        }

        return highlighter;
    }

}
