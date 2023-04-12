/*
 * SnippetFormatter.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import org.apache.lucene.search.uhighlight.Passage;
import org.apache.lucene.search.uhighlight.PassageFormatter;

import javax.annotation.Nonnull;
import java.text.BreakIterator;
import java.util.function.Supplier;

/**
 * A {@link PassageFormatter} which keeps a whole number of words
 * before and after matching text entries to provide context, and inserts ellipses in between
 * matched strings to form a summarized text.
 *
 * If you want a formatter which returns the whole string, use {@link WholeTextFormatter} instead.
 */
public class SnippetFormatter extends PassageFormatter {
    private final Supplier<BreakIterator> breakIterator;
    private final int snippetSize;

    private static final String ellipsis = "...";
    private final String fieldName;

    public SnippetFormatter(final String fieldName, final Supplier<BreakIterator> breakIterator, int snippetSize) {
        this.fieldName = fieldName;
        this.breakIterator = breakIterator;
        this.snippetSize = snippetSize;
        if(snippetSize<=0){
            throw new RecordCoreArgumentException("Cannot create a snippet formatter with a non-positive snippet size");
        }
    }

    @Override
    public Object format(final Passage[] passages, final String content) {
        BreakIterator breakIter = breakIterator.get();
        breakIter.setText(content);

        StringBuilder summaryBuilder = new StringBuilder();
        int[] highlightStarts = new int[passages.length];
        int[] highlightEnds = new int[passages.length];

        /*
         * We go snippetSize words before the match, and snippetSize words after the match, and
         * we add in those to form the "contextual" passage for each passage. We use these contextual
         * intervals to properly format the resulting string, inserting ... when the distance between
         * two contextual bounds do not overlap
         *
         * Sometimes it's possible for a bunch of Passage objects to be passed to this, but they don't actually
         * have any matches in them. When that happens, we want to return a HightlightedTerm with no matches,
         * but we don't know that until we've iterated the passages.
         */
        boolean hasMatches = false;
        int priorSnippetEnd = -1;
        int priorHighlightEnd = -1;
        for(int p = 0; p < passages.length;p++){
            Passage passage = passages[p];
            for(int i =0; i< passage.getNumMatches();i++){
                hasMatches = true;
                int start = passage.getMatchStarts()[i];
                int end = passage.getMatchEnds()[i];
                while (i + 1 < passage.getNumMatches() && passage.getMatchStarts()[i + 1] < end) {
                    end = passage.getMatchEnds()[++i];
                }
                end = Math.min(end, passage.getEndOffset());

                //go back and forth until we reach snippetSize number of tokens
                int snippetStart = start;
                int numTerms = snippetSize;
                while(snippetStart >0 && numTerms >0){
                    snippetStart = breakIter.preceding(snippetStart-1);
                    numTerms--;
                }
                int snippetEnd = end;
                if(snippetEnd<content.length()) {
                    numTerms = snippetSize;
                    while (snippetEnd != BreakIterator.DONE && numTerms > 0) {
                        snippetEnd = breakIter.following(snippetEnd + 1);
                        numTerms--;
                    }
                }
                //make sure that we always stay within our bounds
                snippetStart = Math.max(snippetStart, 0);
                snippetEnd = snippetEnd >= 0 && snippetEnd <= content.length() ? snippetEnd : content.length();

                /*
                 * Build out the string.
                 */
                if (priorSnippetEnd < 0) {
                    //this is the first snippet to append, so write out the leading snippet directly
                    if (snippetStart < ellipsis.length()) {
                        //user experience--if the snippet starts close to the start of the string, then just use
                        //the start value
                        snippetStart = 0;
                    } else {
                        summaryBuilder.append(ellipsis);
                    }
                } else {
                    //compute the range of the prior passage's trailing snippet. If it overlaps with ours,
                    //then write out the union of the two. Otherwise, insert an ellipsis in between
                    if (priorHighlightEnd <= snippetStart) {
                        //we only need to write the prior passage's trailing context if it isn't entirely contained
                        //in this passage's leading context
                        if (priorSnippetEnd > snippetStart) {
                            //there is overlap between the prior passage's trailing context and this passage's leading
                            //context. Therefore, only write the trailing snippet up to when we start
                            priorSnippetEnd = snippetStart;
                        }
                        //the last snippet ends before (or right at) when we begin, so we can safely write it out
                        summaryBuilder.append(content, priorHighlightEnd, priorSnippetEnd);
                        if (priorSnippetEnd < snippetStart) {
                            //only write out an ellipsis separator if there is distance between the two snippets
                            summaryBuilder.append(ellipsis);
                        }
                    }else{
                        //some of this passage's leading context has already been written, so only write
                        //until we get to the next passage
                        snippetStart = priorHighlightEnd;
                    }
                }

                //write out the leading context snippet
                summaryBuilder.append(content, snippetStart, start);
                //get the adjusted start position
                highlightStarts[p] = summaryBuilder.length();
                //write out the highlightable terms
                summaryBuilder.append(content, start, end);
                highlightEnds[p] = summaryBuilder.length();

                priorHighlightEnd = end;
                priorSnippetEnd = snippetEnd;
            }
        }
        if(!hasMatches){
            //there is no snippet to return, so the summarized text should be empty
            return new HighlightedTerm(fieldName, "", new int[] {}, new int[] {});
        }

        //write out the trailing snippet for the last element
        summaryBuilder.append(content,priorHighlightEnd,priorSnippetEnd);
        if(content.length()-priorSnippetEnd>ellipsis.length()){
            //user experience--only write out a trailing snippet if we're far enough away from the end of the string
            summaryBuilder.append(ellipsis);
        }

        return new HighlightedTerm(fieldName, summaryBuilder.toString(), highlightStarts, highlightEnds);
    }
}
