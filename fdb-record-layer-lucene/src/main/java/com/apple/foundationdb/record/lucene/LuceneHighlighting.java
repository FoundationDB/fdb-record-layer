/*
 * LuceneHighlighting.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helper class for highlighting search matches.
 */
public class LuceneHighlighting {
    private LuceneHighlighting() {
    }

    /**
     * Iterator that takes a token stream and implement a next function.
     * This cannot implement the Iterator interface as TokenStream does
     * not provide a way to do hasNext.
     */
    private static class TokenIterator implements Closeable {
        @Nonnull
        private final TokenStream ts;
        @Nonnull
        private final CharTermAttribute termAtt;
        @Nonnull
        private final OffsetAttribute offsetAtt;

        TokenIterator(@Nonnull TokenStream ts) throws IOException {
            this.ts = ts;
            termAtt = ts.addAttribute(CharTermAttribute.class);
            offsetAtt = ts.addAttribute(OffsetAttribute.class);
            ts.reset();
        }

        /**
         * Find next token. Skips over potential synonyms.
         * @return true if we got a new token
         * @throws IOException if something goes wrong
         */
        boolean next() throws IOException {
            int oldStartOffset = startOffset();
            int oldEndOffset = endOffset();
            // This can only loop if we keep discovering new synonyms. Once we've seen them all, this will either go to
            // the next valid token and return true, or there won't be any token left in the stream and we'll return false
            while (true) {
                boolean inc = ts.incrementToken();
                if (!inc) {
                    return false;
                }
                if (oldStartOffset != startOffset() || oldEndOffset != endOffset()) {
                    return true;
                }
            }
        }

        String getToken() {
            return termAtt.toString();
        }

        int startOffset() {
            return offsetAtt.startOffset();
        }

        int endOffset() {
            return offsetAtt.endOffset();
        }

        @Override
        public void close() throws IOException {
            ts.end();
            ts.close();

        }
    }

    /**
     * Helper class. Use search() as entry point.
     * This will iterate over the token stream and :
     * - find matches
     * - cut snippets
     * - return a list of highlighted positions
     */
    @SuppressWarnings("PMD.AvoidStringBufferField")
    private static class SearchAllAndHighlightImpl {
        private final TokenIterator it;
        private final TokenIterator standardIt;
        private final String text;
        private final boolean cutSnippets;
        private final int snippetSize;
        @Nullable
        private final List<Pair<Integer, Integer>> highlightedPositions;

        private final StringBuilder sb = new StringBuilder();
        private int upto = 0;
        private final ArrayDeque<String> beforeHighlightTokens = new ArrayDeque<>();
        int snippetRunningBudget = 0;
        Set<String> matchedInText = new HashSet<>();
        Set<String> matchedPrefixes = new HashSet<>();
        boolean prefixTextConnector = false;
        private String highlightedTextConnector = "...";

        SearchAllAndHighlightImpl(TokenIterator it,
                                  TokenIterator standardIt,
                                  String text,
                                  boolean cutSnippets,
                                  int snippetSize,
                                  @Nullable final List<Pair<Integer, Integer>> highlightedPositions) {
            this.it = it;
            this.standardIt = standardIt;
            this.text = text;
            this.cutSnippets = cutSnippets;
            this.snippetSize =  snippetSize <= 0 ? Integer.MAX_VALUE : snippetSize;
            this.highlightedPositions = highlightedPositions;
        }

        private int tokenCountBeforeHighlighted() {
            return (snippetSize - 1) / 2;
        }

        private void handleNonMatchToken() {
            if (snippetRunningBudget > 0) {
                addNonMatch(sb, text.substring(upto, standardIt.endOffset()));
                snippetRunningBudget--;
            }
            if (snippetRunningBudget == 0) {
                beforeHighlightTokens.addLast(text.substring(upto, standardIt.endOffset()));
                if (beforeHighlightTokens.size() > tokenCountBeforeHighlighted()) {
                    beforeHighlightTokens.pollFirst();
                    prefixTextConnector = true;
                }
            }
            upto = standardIt.endOffset();
        }

        private boolean handleMatchToken(final Set<String> matchedTokens) {
            if (!matchedTokens.contains(it.getToken())) {
                return false;
            }
            addBeforeTokens();
            addNonMatch(sb, text.substring(upto, standardIt.startOffset()));
            addWholeMatch(sb, text.substring(standardIt.startOffset(), standardIt.endOffset()), highlightedPositions);
            snippetRunningBudget--;
            upto = it.endOffset();
            matchedInText.add(standardIt.getToken());
            return true;
        }

        private boolean handlePrefixMatch(final Set<String> prefixTokens) {
            for (String prefixToken : prefixTokens) {
                if (it.getToken().startsWith(prefixToken)) {
                    addBeforeTokens();
                    addNonMatch(sb, text.substring(upto, it.startOffset()));
                    addPrefixMatch(sb, text.substring(it.startOffset(), it.endOffset()), prefixToken,
                            highlightedPositions);
                    upto = it.endOffset();
                    matchedPrefixes.add(prefixToken);
                    snippetRunningBudget--;
                    return true;
                }
            }
            return false;
        }

        private void addBeforeTokens() {
            snippetRunningBudget = snippetSize;
            if (prefixTextConnector) {
                addNonMatch(sb, highlightedTextConnector);
                highlightedTextConnector = " ..."; // TODO(alacurie) This probably does not work for RTL languages
                prefixTextConnector = false;
            }
            for (String token : beforeHighlightTokens) {
                addNonMatch(sb, token);
                snippetRunningBudget--;
            }
            beforeHighlightTokens.clear();
        }

        private void handleToken(final Set<String> matchedTokens, final Set<String> prefixTokens) {
            boolean matched = handleMatchToken(matchedTokens);
            if (!matched) {
                matched = handlePrefixMatch(prefixTokens);
            }
            if (!matched) {
                handleNonMatchToken();
            }
        }

        public String search(final Set<String> matchedTokens, final Set<String> prefixTokens, final boolean allMatchingRequired) throws IOException {
            while (it.next()) {
                do {
                    standardIt.next();
                    if (standardIt.startOffset() < it.startOffset()) {
                        // We're handling a stop word (we don't need to check if that word matches)
                        handleNonMatchToken();
                    } else {
                        // We're handling a real word (We need to check if that word matches)
                        handleToken(matchedTokens, prefixTokens);
                    }
                } while (standardIt.startOffset() < it.startOffset());
            }

            if (allMatchingRequired && (matchedPrefixes.size() < prefixTokens.size() || (matchedInText.size() < matchedTokens.size()))) {
                // Query text not actually found in document text. Return null
                return null;
            }

            // Text was found. Return text
            if (!cutSnippets) {
                addBeforeTokens();
                addNonMatch(sb, text.substring(upto));
            }

            if (snippetRunningBudget >= 0 && cutSnippets) {
                addNonMatch(sb, " ...");
            }
            return sb.toString();
        }
    }

    @Nullable
    static String searchAllAndHighlight(@Nonnull String fieldName, @Nonnull Analyzer queryAnalyzer, @Nonnull String text,
                                        @Nonnull Set<String> matchedTokens, @Nonnull Set<String> prefixTokens,
                                        boolean allMatchingRequired,
                                        @Nonnull LuceneScanQueryParameters.LuceneQueryHighlightParameters luceneQueryHighlightParameters,
                                        @Nullable List<Pair<Integer, Integer>> highlightedPositions) {
        try (TokenStream ts = queryAnalyzer.tokenStream(fieldName, new StringReader(text)) ;
                 StandardAnalyzer standardAnalyzer = new StandardAnalyzer() ; // TODO(alacurie) This will most likely not work for RTL languages
                 TokenStream standardTs = standardAnalyzer.tokenStream(fieldName, new StringReader(text)) ;
                 TokenIterator it = new TokenIterator(ts);
                 TokenIterator standardIt = new TokenIterator(standardTs)
        ) {
            var impl = new SearchAllAndHighlightImpl(it, standardIt, text, luceneQueryHighlightParameters.isCutSnippets(), luceneQueryHighlightParameters.getSnippedSize(), highlightedPositions);
            return impl.search(matchedTokens, prefixTokens, allMatchingRequired);
        } catch (IOException e) {
            return null;
        }
    }

    // Check this before highlighting tokens, so the highlighting is idempotent

    /** Called while highlighting a single result, to append a
     *  non-matching chunk of text from the suggestion to the
     *  provided fragments list.
     *  @param sb The {@code StringBuilder} to append to
     *  @param text The text chunk to add
     */
    private static void addNonMatch(StringBuilder sb, String text) {
        sb.append(text);
    }

    /**
     * Called while highlighting a single result, to append
     * the whole matched token to the provided fragments list.
     *
     * @param sb The {@code StringBuilder} to append to
     * @param surface The surface form (original) text
     */
    private static void addWholeMatch(StringBuilder sb, String surface,
                                      @Nullable List<Pair<Integer, Integer>> highlightedPositions) {
        int start = sb.length();
        sb.append(surface);
        if (highlightedPositions != null) {
            highlightedPositions.add(Pair.of(start, sb.length()));
        }
    }

    /**
     * Called while highlighting a single result, to append a
     * matched prefix token, to the provided fragments list.
     *
     * @param sb The {@code StringBuilder} to append to
     * @param surface The fragment of the surface form
     * (indexed during build, corresponding to
     * this match
     * @param prefixToken The prefix of the token that matched
     */
    private static void addPrefixMatch(StringBuilder sb, String surface, String prefixToken,
                                       @Nullable List<Pair<Integer, Integer>> highlightedPositions) {
        // TODO: apps can try to invert their analysis logic
        // here, e.g. downcase the two before checking prefix:
        if (prefixToken.length() >= surface.length()) {
            addWholeMatch(sb, surface, highlightedPositions);
            return;
        }
        int start = sb.length();
        sb.append(surface, 0, prefixToken.length());
        if (highlightedPositions != null) {
            highlightedPositions.add(Pair.of(start, sb.length()));
        }
        sb.append(surface.substring(prefixToken.length()));
    }

    // Modify the Lucene fields of a record message with highlighting the terms from the given termMap
    @Nonnull
    public static <M extends Message> void highlightTermsInMessage(@Nonnull KeyExpression expression, @Nonnull Message.Builder builder, @Nonnull Map<String, Set<String>> termMap,
                                                                   @Nonnull LuceneAnalyzerCombinationProvider analyzerSelector,
                                                                   @Nonnull LuceneScanQueryParameters.LuceneQueryHighlightParameters luceneQueryHighlightParameters) {
        RecordRebuildSource<M> recordRebuildSource = new RecordRebuildSource<>(null, builder.getDescriptorForType(), builder, builder.build());

        LuceneIndexExpressions.getFields(expression, recordRebuildSource,
                (source, fieldName, value, type, stored, sorted, overriddenKeyRanges, groupingKeyIndex, keyIndex, fieldConfigsIgnored) -> {
                    if (type != LuceneIndexExpressions.DocumentFieldType.TEXT) {
                        return;
                    }
                    Set<String> terms = getFieldTerms(termMap, fieldName);
                    if (terms.isEmpty()) {
                        return;
                    }
                    Set<String> prefixes = getPrefixTerms(terms);
                    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : source.message.getAllFields().entrySet()) {
                        final Descriptors.FieldDescriptor entryDescriptor = entry.getKey();
                        final Object entryValue = entry.getValue();
                        if (entryValue instanceof String) {
                            buildIfMatch(source, fieldName, value,
                                    entryDescriptor, entryValue, 0,
                                    terms, prefixes, analyzerSelector, luceneQueryHighlightParameters);
                        } else if (entryValue instanceof List) {
                            int index = 0;
                            for (Object entryValueElement : ((List<?>) entryValue)) {
                                buildIfMatch(source, fieldName, value,
                                        entryDescriptor, entryValueElement, index,
                                        terms, prefixes, analyzerSelector, luceneQueryHighlightParameters);
                                index++;
                            }
                        }
                    }
                }, null);
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

    private static <M extends Message> void buildIfMatch(RecordRebuildSource<M> source, String fieldName, Object fieldValue,
                                                         Descriptors.FieldDescriptor entryDescriptor, Object entryValue, int index,
                                                         @Nonnull Set<String> terms, @Nonnull Set<String> prefixes,
                                                         @Nonnull LuceneAnalyzerCombinationProvider analyzerSelector,
                                                         @Nonnull LuceneScanQueryParameters.LuceneQueryHighlightParameters luceneQueryHighlightParameters) {
        if (entryValue.equals(fieldValue) && isMatch((String)entryValue, terms, prefixes)) {
            String highlightedText = searchAllAndHighlight(fieldName, analyzerSelector.provideIndexAnalyzer((String)entryValue).getAnalyzer(), (String)entryValue, terms, prefixes, false, luceneQueryHighlightParameters, null);
            source.buildMessage(highlightedText, entryDescriptor, null, null, true, index);
        }
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

    static class RecordRebuildSource<M extends Message> implements LuceneIndexExpressions.RecordSource<RecordRebuildSource<M>> {
        @Nullable
        public final RecordRebuildSource<M> parent;
        @Nonnull
        public final Descriptors.Descriptor descriptor;
        @Nullable
        public final Descriptors.FieldDescriptor fieldDescriptor;
        @Nonnull
        public final Message.Builder builder;
        public final Message message;
        public final int indexIfRepeated;

        RecordRebuildSource(@Nullable RecordRebuildSource<M> parent, @Nonnull Descriptors.Descriptor descriptor, @Nonnull Message.Builder builder, @Nonnull Message message) {
            //this.rec = rec;
            this.parent = parent;
            this.descriptor = descriptor;
            this.fieldDescriptor = null;
            this.builder = builder;
            this.message = message;
            this.indexIfRepeated = 0;
        }

        RecordRebuildSource(@Nullable RecordRebuildSource<M> parent, @Nonnull Descriptors.FieldDescriptor fieldDescriptor, @Nonnull Message.Builder builder, @Nonnull Message message, int indexIfRepeated) {
            //this.rec = rec;
            this.parent = parent;
            this.descriptor = fieldDescriptor.getMessageType();
            this.fieldDescriptor = fieldDescriptor;
            this.builder = builder;
            this.message = message;
            this.indexIfRepeated = indexIfRepeated;
        }

        @Override
        public Descriptors.Descriptor getDescriptor() {
            return descriptor;
        }

        @Override
        public Iterable<RecordRebuildSource<M>> getChildren(@Nonnull FieldKeyExpression parentExpression) {
            final String parentField = parentExpression.getFieldName();
            final Descriptors.FieldDescriptor parentFieldDescriptor = descriptor.findFieldByName(parentField);

            final List<RecordRebuildSource<M>> children = new ArrayList<>();
            int index = 0;
            for (Key.Evaluated evaluated : parentExpression.evaluateMessage(null, message)) {
                final Message submessage = (Message)evaluated.toList().get(0);
                if (submessage != null) {
                    if (parentFieldDescriptor.isRepeated()) {
                        children.add(new RecordRebuildSource<M>(this, parentFieldDescriptor,
                                builder.newBuilderForField(parentFieldDescriptor),
                                submessage, index++));
                    } else {
                        children.add(new RecordRebuildSource<M>(this, parentFieldDescriptor,
                                builder.getFieldBuilder(parentFieldDescriptor),
                                submessage, index));
                    }
                }
            }
            return children;
        }

        @Override
        public Iterable<Object> getValues(@Nonnull FieldKeyExpression fieldExpression) {
            final List<Object> values = new ArrayList<>();
            for (Key.Evaluated evaluated : fieldExpression.evaluateMessage(null, message)) {
                Object value = evaluated.getObject(0);
                if (value != null) {
                    values.add(value);
                }
            }
            return values;
        }

        @SuppressWarnings("java:S3776")
        public void buildMessage(@Nullable Object value, Descriptors.FieldDescriptor subFieldDescriptor, @Nullable String customizedKey, @Nullable String mappedKeyField, boolean forLuceneField, int index) {
            final Descriptors.FieldDescriptor mappedKeyFieldDescriptor = mappedKeyField == null ? null : descriptor.findFieldByName(mappedKeyField);
            if (mappedKeyFieldDescriptor != null) {
                if (customizedKey == null) {
                    return;
                }
                builder.setField(mappedKeyFieldDescriptor, customizedKey);
            }

            if (value == null) {
                return;
            }
            if (subFieldDescriptor.isRepeated()) {
                if (subFieldDescriptor.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.MESSAGE)) {
                    Message.Builder subBuilder = builder.newBuilderForField(subFieldDescriptor);
                    subBuilder.mergeFrom((Message) builder.getRepeatedField(subFieldDescriptor, index)).mergeFrom((Message) value);
                    builder.setRepeatedField(subFieldDescriptor, index, subBuilder.build());
                } else {
                    builder.setRepeatedField(subFieldDescriptor, index, value);
                }

            } else {
                int count = builder.getAllFields().size();
                if (message != null && count == 0) {
                    builder.mergeFrom(message);
                }
                builder.setField(subFieldDescriptor, value);
            }

            if (parent != null) {
                parent.buildMessage(builder.build(), this.fieldDescriptor, mappedKeyFieldDescriptor == null ? customizedKey : null, mappedKeyFieldDescriptor == null ? mappedKeyField : null, forLuceneField, indexIfRepeated);
            }
        }
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

    /**
     * Result of {@link #highlightedTermsForMessage}.
     */
    public static class HighlightedTerm {
        private final String fieldName;
        private final String snippet;
        private final List<Pair<Integer, Integer>> highlightedPositions;

        public HighlightedTerm(final String fieldName, final String snippet, final List<Pair<Integer, Integer>> highlightedPositions) {
            this.fieldName = fieldName;
            this.snippet = snippet;
            this.highlightedPositions = highlightedPositions;
        }

        public String getFieldName() {
            return fieldName;
        }

        public String getSnippet() {
            return snippet;
        }

        public List<Pair<Integer, Integer>> getHighlightedPositions() {
            return highlightedPositions;
        }
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
    public static <M extends Message> List<HighlightedTerm> highlightedTermsForMessage(@Nonnull FDBRecord<M> rec, M message, @Nullable String nestedName,
                                                                                       @Nonnull KeyExpression expression, @Nonnull Map<String, Set<String>> termMap, @Nonnull LuceneAnalyzerCombinationProvider analyzerSelector,
                                                                                       @Nonnull LuceneScanQueryParameters.LuceneQueryHighlightParameters luceneQueryHighlightParameters) {
        if (nestedName != null) {
            expression = getNestedFields(expression, nestedName);
            if (expression == null) {
                return Collections.emptyList();
            }
        }
        List<HighlightedTerm> result = new ArrayList<>();
        LuceneIndexExpressions.getFields(expression, new LuceneDocumentFromRecord.FDBRecordSource<>(rec, message),
                (source, fieldName, value, type, stored, sorted, overriddenKeyRanges, groupingKeyIndex, keyIndex, fieldConfigsIgnored) -> {
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
                        List<Pair<Integer, Integer>> highlightedPositions = new ArrayList<>();
                        String highlightedText = searchAllAndHighlight(fieldName, analyzerSelector.provideQueryAnalyzer((String)value).getAnalyzer(), (String)value, terms, prefixes, false, luceneQueryHighlightParameters, highlightedPositions);
                        result.add(new HighlightedTerm(fieldName, highlightedText, highlightedPositions));
                    }
                }, null);
        return result;
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

}
