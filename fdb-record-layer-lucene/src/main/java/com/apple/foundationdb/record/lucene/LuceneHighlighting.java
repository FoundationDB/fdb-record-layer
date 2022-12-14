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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Helper class for highlighting search matches.
 */
public class LuceneHighlighting {
    private static final int tokenCountBeforeHighlighted = 3;
    private static final int tokenCountAfterHighlighted = 3;
    private static final String highlightedTextConnector = "... ";

    private LuceneHighlighting() {
    }

    @SuppressWarnings("squid:S3776") // Cognitive complexity is too high. Candidate for later refactoring
    @Nullable
    static String searchAllMaybeHighlight(@Nonnull String fieldName, @Nonnull Analyzer queryAnalyzer, @Nonnull String text,
                                          @Nonnull Set<String> matchedTokens, @Nullable String prefixToken,
                                          boolean allMatchingRequired,
                                          @Nonnull LuceneScanQueryParameters.LuceneQueryHighlightParameters luceneQueryHighlightParameters) {
        try (TokenStream ts = queryAnalyzer.tokenStream(fieldName, new StringReader(text))) {
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
            ts.reset();
            StringBuilder sb = luceneQueryHighlightParameters.isHighlight() ? new StringBuilder() : null;
            int upto = 0;
            Set<String> matchedInText = new HashSet<>();
            boolean matchedPrefix = false;
            ArrayDeque<String> pres = new ArrayDeque<>();
            ArrayDeque<String> ends = new ArrayDeque<>();
            int lastMatchPos = -tokenCountAfterHighlighted - 1;
            int currentPos = 0;
            while (ts.incrementToken()) {
                String token = termAtt.toString();
                int startOffset = offsetAtt.startOffset();
                int endOffset = offsetAtt.endOffset();
                if (upto < startOffset) {
                    if (luceneQueryHighlightParameters.isHighlight()) {
                        if (luceneQueryHighlightParameters.isCutSnippets()) {
                            if (currentPos - lastMatchPos <= tokenCountAfterHighlighted + 1) {
                                addNonMatch(sb, text.substring(upto, startOffset));
                            } else {
                                pres.add(text.substring(upto, startOffset));
                                if (pres.size() > tokenCountBeforeHighlighted) {
                                    pres.poll();
                                }
                                if (ends.size() < luceneQueryHighlightParameters.getSnippedSize() - tokenCountAfterHighlighted) {
                                    ends.add(text.substring(upto, startOffset));
                                }
                            }
                        } else {
                            addNonMatch(sb, text.substring(upto, startOffset));
                        }
                    }
                    upto = startOffset;
                } else if (upto > startOffset) {
                    continue;
                }

                if (matchedTokens.contains(token)) {
                    // Token matches.
                    if (luceneQueryHighlightParameters.isHighlight()) {
                        if (luceneQueryHighlightParameters.isCutSnippets() && currentPos - lastMatchPos > tokenCountBeforeHighlighted + tokenCountAfterHighlighted + 1) {
                            addNonMatch(sb, highlightedTextConnector);
                        }
                        while (!pres.isEmpty()) {
                            addNonMatch(sb, pres.poll());
                        }
                        ends.clear();
                        int start = startOffset;
                        while (start < endOffset) {
                            int index = text.toLowerCase(Locale.ROOT).indexOf(token, start);
                            if (index < 0 || index >= endOffset) {
                                addNonMatch(sb, text.substring(start, endOffset));
                                break;
                            }
                            int actualStartOffset = index;
                            int actualEndOffset = index + token.length();
                            addNonMatch(sb, text.substring(start, index));
                            String substring = text.substring(actualStartOffset, actualEndOffset);
                            if (substring.equalsIgnoreCase(token) && !tokenAlreadyHighlighted(text, actualStartOffset, actualEndOffset,
                                    luceneQueryHighlightParameters.getLeftTag(), luceneQueryHighlightParameters.getRightTag())) {
                                addWholeMatch(sb, substring,
                                        luceneQueryHighlightParameters.getLeftTag(), luceneQueryHighlightParameters.getRightTag());
                            } else {
                                addNonMatch(sb, substring);
                            }
                            start = actualEndOffset;
                        }
                    }
                    upto = endOffset;
                    matchedInText.add(token);
                    lastMatchPos = currentPos;
                } else if (prefixToken != null && token.startsWith(prefixToken)) {
                    if (luceneQueryHighlightParameters.isHighlight()) {
                        if (!tokenAlreadyHighlighted(text, startOffset, endOffset,
                                luceneQueryHighlightParameters.getLeftTag(), luceneQueryHighlightParameters.getRightTag())) {
                            addPrefixMatch(sb, text.substring(startOffset, endOffset), prefixToken,
                                    luceneQueryHighlightParameters.getLeftTag(), luceneQueryHighlightParameters.getRightTag());
                        } else {
                            addNonMatch(sb, text.substring(startOffset, endOffset));
                        }
                    }
                    upto = endOffset;
                    matchedPrefix = true;
                }
                currentPos++;
            }
            ts.end();

            if (allMatchingRequired && ((prefixToken != null && !matchedPrefix) || (matchedInText.size() < matchedTokens.size()))) {
                // Query text not actually found in document text. Return null
                return null;
            }

            // Text was found. Return text (highlighted or not)
            if (luceneQueryHighlightParameters.isHighlight()) {
                int endOffset = offsetAtt.endOffset();
                if (upto < endOffset && !luceneQueryHighlightParameters.isCutSnippets()) {
                    addNonMatch(sb, text.substring(upto));
                } else if (luceneQueryHighlightParameters.isCutSnippets()) {
                    while (!ends.isEmpty()) {
                        addNonMatch(sb, ends.poll());
                    }
                    addNonMatch(sb, highlightedTextConnector);
                }
                return sb.toString();
            } else {
                return text;
            }

        } catch (IOException e) {
            return null;
        }
    }

    // Check this before highlighting tokens, so the highlighting is idempotent
    private static boolean tokenAlreadyHighlighted(@Nonnull String text, int startOffset, int endOffset,
                                                   @Nonnull String leftTag, @Nonnull String rightTag) {
        return startOffset - leftTag.length() >= 0
               && endOffset + rightTag.length() <= text.length()
               && text.startsWith(leftTag, startOffset - leftTag.length())
               && text.startsWith(rightTag, endOffset);
    }

    /** Called while highlighting a single result, to append a
     *  non-matching chunk of text from the suggestion to the
     *  provided fragments list.
     *  @param sb The {@code StringBuilder} to append to
     *  @param text The text chunk to add
     */
    private static void addNonMatch(StringBuilder sb, String text) {
        sb.append(text);
    }

    /** Called while highlighting a single result, to append
     *  the whole matched token to the provided fragments list.
     * @param sb The {@code StringBuilder} to append to
     *  @param surface The surface form (original) text
     * @param leftTag the tag to add left to the surface
     * @param rightTag the tag to add right to the surface
     */
    private static void addWholeMatch(StringBuilder sb, String surface, String leftTag, String rightTag) {
        sb.append(leftTag);
        sb.append(surface);
        sb.append(rightTag);
    }

    /** Called while highlighting a single result, to append a
     *  matched prefix token, to the provided fragments list.
     * @param sb The {@code StringBuilder} to append to
     *  @param surface The fragment of the surface form
     *        (indexed during build, corresponding to
     *        this match
     * @param prefixToken The prefix of the token that matched
     * @param leftTag the tag to add left to the surface
     * @param rightTag the tag to add right to the surface
     */
    private static void addPrefixMatch(StringBuilder sb, String surface, String prefixToken, String leftTag, String rightTag) {
        // TODO: apps can try to invert their analysis logic
        // here, e.g. downcase the two before checking prefix:
        if (prefixToken.length() >= surface.length()) {
            addWholeMatch(sb, surface, leftTag, rightTag);
            return;
        }
        sb.append(leftTag);
        sb.append(surface.substring(0, prefixToken.length()));
        sb.append(rightTag);
        sb.append(surface.substring(prefixToken.length()));
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public static <M extends Message> FDBQueriedRecord<M> highlightTermsInRecord(@Nullable FDBQueriedRecord<M> queriedRecord) {
        if (queriedRecord == null) {
            return queriedRecord;
        }
        IndexEntry indexEntry = queriedRecord.getIndexEntry();
        if (!(indexEntry instanceof LuceneRecordCursor.ScoreDocIndexEntry)) {
            return queriedRecord;
        }
        LuceneRecordCursor.ScoreDocIndexEntry docIndexEntry = (LuceneRecordCursor.ScoreDocIndexEntry)indexEntry;
        if (!docIndexEntry.getLuceneQueryHighlightParameters().isHighlight()) {
            return queriedRecord;
        }
        M message = queriedRecord.getRecord();
        M.Builder builder = message.toBuilder();
        highlightTermsInMessage(docIndexEntry.getIndexKey(), builder,
                docIndexEntry.getTermMap(), docIndexEntry.getAnalyzerSelector(), docIndexEntry.getLuceneQueryHighlightParameters());
        FDBStoredRecord<M> storedRecord = queriedRecord.getStoredRecord().asBuilder().setRecord((M) builder.build()).build();
        return FDBQueriedRecord.indexed(new FDBIndexedRecord<>(indexEntry, storedRecord));
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
                    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : source.message.getAllFields().entrySet()) {
                        final Descriptors.FieldDescriptor entryDescriptor = entry.getKey();
                        final Object entryValue = entry.getValue();
                        if (entryValue instanceof String) {
                            buildIfMatch(source, fieldName, value,
                                    entryDescriptor, entryValue, 0,
                                    terms, analyzerSelector, luceneQueryHighlightParameters);
                        } else if (entryValue instanceof List) {
                            int index = 0;
                            for (Object entryValueElement : ((List<?>) entryValue)) {
                                buildIfMatch(source, fieldName, value,
                                        entryDescriptor, entryValueElement, index,
                                        terms, analyzerSelector, luceneQueryHighlightParameters);
                                index++;
                            }
                        }
                    }
                }, null);
    }

    private static <M extends Message> void buildIfMatch(RecordRebuildSource<M> source, String fieldName, Object fieldValue,
                                                         Descriptors.FieldDescriptor entryDescriptor, Object entryValue, int index,
                                                         @Nonnull Set<String> terms,
                                                         @Nonnull LuceneAnalyzerCombinationProvider analyzerSelector,
                                                         @Nonnull LuceneScanQueryParameters.LuceneQueryHighlightParameters luceneQueryHighlightParameters) {
        if (entryValue.equals(fieldValue) && terms.stream().anyMatch(t -> StringUtils.containsIgnoreCase((String)entryValue, t))) {
            String highlightedText = searchAllMaybeHighlight(fieldName, analyzerSelector.provideIndexAnalyzer((String)entryValue).getAnalyzer(), (String)entryValue, terms, null, false, luceneQueryHighlightParameters);
            source.buildMessage(highlightedText, entryDescriptor, null, null, true, index);
        }
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

}
