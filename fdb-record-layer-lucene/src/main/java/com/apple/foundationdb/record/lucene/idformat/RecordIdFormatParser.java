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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A simple parser for the string that represents the {@link RecordIdFormat}.
 * Format syntax is:
 * <pre>
 *      {@code [<format element>, ...]}
 * </pre>
 * Where the {@code <format element>} can be:
 * <pre>
 *      {@code <FormatElementType> | <TupleElement>}
 * </pre>
 * and the {@code <FormatElementType>} is one of the {@code NONE, NULL, INT8, UUID_AS_STRING} etc,
 * and the {@code <TupleElement>} is (similar to above):
 * <pre>
 *      {@code [<format element>, ...]}
 * </pre>
 *
 * For example, the following are string representations of valid formats:
 * <pre>
 *     {@code [INT8]}
 *     {@code [INT8, INT64]}
 *     {@code [NULL, [INT64_OR_NULL, UUID_AS_STRING], [NONE, UUID_AS_STRING]]}
 * </pre>
 */
public class RecordIdFormatParser {

    public static final String BEGIN_TUPLE_STR = "[";
    public static final String END_TUPLE_STR = "]";
    public static final char BEGIN_TUPLE_CHAR = '[';
    public static final char END_TUPLE_CHAR = ']';

    private RecordIdFormatParser() {
    }

    /**
     * Parse a string representing a format (see syntax above) and return an instance of {@link RecordIdFormat}.
     * @param formatString the given string representation
     * @return the constructed format
     * @throws RecordCoreFormatException in case of parse failure
     */
    @Nonnull
    public static RecordIdFormat parse(final String formatString) throws RecordCoreFormatException {
        // top level element is actually a tuple
        RecordIdFormat.TupleElement tupleElement = parseTuple(formatString.trim());
        return new RecordIdFormat(tupleElement);
    }

    @Nonnull
    private static RecordIdFormat.FormatElement parseElement(final String s) throws RecordCoreFormatException {
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

    @Nonnull
    private static RecordIdFormat.TupleElement parseTuple(final String s) {
        if ((!s.startsWith(BEGIN_TUPLE_STR)) || (!s.endsWith(END_TUPLE_STR))) {
            throw new RecordCoreFormatException("Format error: syntax error")
                    .addLogInfo("syntaxError", "missing beginning or end of tuple");
        }
        // strip "[" and "]"
        String content = s.substring(1, s.length() - 1);
        List<String> splits = tokenize(content);
        List<RecordIdFormat.FormatElement> elements = splits.stream()
                .map(RecordIdFormatParser::parseElement)
                .collect(Collectors.toList());

        return new RecordIdFormat.TupleElement(elements);
    }

    @Nullable
    private static RecordIdFormat.FormatElementType parseType(final String s) {
        try {
            return RecordIdFormat.FormatElementType.valueOf(s);
        } catch (IllegalArgumentException ex) {
            // not a match
            return null;
        }
    }

    /**
     * Break up a string into tokens, along "," deliminators, keeping any top level "[]" elements intact along
     * balanced pairs of brackets. For example, {@code "a, b, [c, [d,e], f], [g, h]]"} will be broken down into
     * <pre>
     *     {@code "a"}
     *     {@code "b"}
     *     {@code "[c, [d,e], f]]"}
     *     {@code "[g, h]"}
     * </pre>
     * @param s the given string
     * @return the parsed tokens
     */
    private static List<String> tokenize(String s) {
        List<String> result = new ArrayList<>();
        int tokenStart = 0;
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == ',') {
                result.add(s.substring(tokenStart, i));
                tokenStart = i + 1;
            } else if (c == BEGIN_TUPLE_CHAR) {
                i = matchBalanced(s, i);
            } else if (c == END_TUPLE_CHAR) {
                throw new RecordCoreFormatException("Unmatched ']'");
            }
            i += 1;
        }
        // last element after the ','
        if (tokenStart < s.length()) {
            result.add(s.substring(tokenStart));
        } else {
            throw new RecordCoreFormatException("Unmatched ',' (no token following)");
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
