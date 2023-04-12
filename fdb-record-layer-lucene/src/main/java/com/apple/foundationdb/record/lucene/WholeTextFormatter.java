/*
 * WholeTextFormatter.java
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

import org.apache.lucene.search.uhighlight.Passage;
import org.apache.lucene.search.uhighlight.PassageFormatter;

/**
 * A {@link PassageFormatter} which creates Highlighted terms that contain the
 * whole context of the field as the summarized text (i.e. no summarization or shortening occurs).
 * <p>
 * If you want to summarize the content, use {@link SnippetFormatter} instead.
 */
public class WholeTextFormatter extends PassageFormatter {
    private final String fieldName;

    public WholeTextFormatter(final String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Object format(final Passage[] passages, final String content) {
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
        for (int p = 0; p < passages.length; p++) {
            Passage passage = passages[p];
            for (int i = 0; i < passage.getNumMatches(); i++) {
                hasMatches = true;
                int start = passage.getMatchStarts()[i];
                int end = passage.getMatchEnds()[i];
                while (i + 1 < passage.getNumMatches() && passage.getMatchStarts()[i + 1] < end) {
                    end = passage.getMatchEnds()[++i];
                }
                end = Math.min(end, passage.getEndOffset());
                highlightStarts[p] = start;
                highlightEnds[p] = end;
            }
        }

        if(!hasMatches){
            return new HighlightedTerm(fieldName,content,new int[]{},new int[]{});
        }else {
            return new HighlightedTerm(fieldName, content, highlightStarts, highlightEnds);
        }
    }
}
