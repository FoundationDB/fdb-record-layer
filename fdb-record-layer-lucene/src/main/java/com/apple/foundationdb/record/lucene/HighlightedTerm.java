/*
 * HighlightedTerm.java
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

/**
 * Representation of a Single pass of a highlighter. This holds a summarized text (text which has been shortened
 * with ellipses between highlight positions), along with a collection of intervals which are used
 * to locate the specific locations to be highlighted.
 */
public class HighlightedTerm {
    private final String fieldName;
    private final String summarizedText; //the paraphrased text, with ... in between highlight matches

    /*
     * For efficiency reasons, store the starts and ends in two separate, aligned arrays. It's a bit painful,
     * but it avoids having to create a bunch of Pair objects when there are many matches. The highlight positions
     * are disjoint from one another, because that's how Lucene returned highlight positions.
     */
    private final int[] highlightStarts;
    private final int[] highlightEnds;

    public HighlightedTerm(final String fieldName,
                           final String summarizedText,
                           int[] highlightStarts,
                           int[] highlightEnds) {
        this.fieldName = fieldName;
        this.summarizedText = summarizedText;
        this.highlightStarts = highlightStarts;
        this.highlightEnds = highlightEnds;
        if (highlightStarts.length != highlightEnds.length) {
            throw new RecordCoreArgumentException("There must be a start and end for each highlight");
        }
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getSummarizedText() {
        return summarizedText;
    }

    int getNumHighlights() {
        return highlightStarts.length;
    }

    int getHighlightStart(int pos) {
        return highlightStarts[pos];
    }

    int getHighlightEnd(int pos) {
        return highlightEnds[pos];
    }
}
