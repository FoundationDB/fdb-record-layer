/*
 * RecordIdFormatParser.java
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

package com.apple.foundationdb.record.lucene.idformat;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RecordIdFormatParser {

    public static final String BEGIN_TUPLE = "[";
    public static final String END_TUPLE = "]";

    private RecordIdFormatParser() {
    }

    public static RecordIdFormat parse(final String formatString) {
        RecordIdFormat.TupleElement tupleElement = parseTuple(formatString.trim());
        return new RecordIdFormat(tupleElement);
    }

    private static RecordIdFormat.FormatElement parseElement(final String s) {
        String trimmed = s.trim();
        // Try to parse as a format type enum
        RecordIdFormat.FormatElementType formatElementType = parseType(trimmed);
        if (formatElementType != null) {
            return formatElementType;
        } else {
            // Not a type, parse as a tuple
            return parseTuple(trimmed);
        }
    }

    private static RecordIdFormat.TupleElement parseTuple(final String s) {
        if ((!s.startsWith(BEGIN_TUPLE)) || (!s.endsWith(END_TUPLE))) {
            throw new RecordCoreFormatException("Format error: syntax error")
                    .addLogInfo("syntaxError", "missing beginning or end of tuple");
        }

        String content = s.substring(1, s.length() - 1);
        List<String> splits = tokenize(content);
        List<RecordIdFormat.FormatElement> elements = splits.stream()
                .map(RecordIdFormatParser::parseElement)
                .collect(Collectors.toList());

        return new RecordIdFormat.TupleElement(elements);
    }

    @Nullable
    private static RecordIdFormat.FormatElementType parseType(final String trimmed) {
        try {
            return RecordIdFormat.FormatElementType.valueOf(trimmed);
        } catch (IllegalArgumentException ex) {
            // not a match
            return null;
        }
    }

    public static List<String> tokenize(String s) {
        List<String> result = new ArrayList<>();
        int tokenStart = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == ',') {
                result.add(s.substring(tokenStart, i));
                tokenStart = i + 1;
            } else if (c == '[') {
                int match = matchBalanced(s, i);
                i = match;
            } else if (c == ']') {
                throw new RecordCoreFormatException("Unmatched ']'");
            } else {
                // do nothing
            }
        }
        // last element after the ','
        if (tokenStart < s.length()) {
            result.add(s.substring(tokenStart));
        }
        return result;
    }

    private static int matchBalanced(final String s, final int start) {
        int count = 0;
        for (int i = start; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '[') {
                count++;
            } else if (c == ']') {
                count--;
                if (count == 0) {
                    return i;
                }
            }
        }
        throw new RecordCoreFormatException("Unmatched [] in format string");
    }

}
