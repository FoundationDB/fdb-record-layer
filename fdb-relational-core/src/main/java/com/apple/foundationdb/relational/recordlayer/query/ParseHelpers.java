/*
 * ParseHelpers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.half.Half;
import com.apple.foundationdb.record.query.plan.cascades.TreeLike;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.util.Hex;

import com.apple.foundationdb.relational.util.Assert;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Contains a set of utility methods that are relevant for parsing the AST.
 * TODO: this class should be removed.
 */
@API(API.Status.EXPERIMENTAL)
public final class ParseHelpers {

    // used only to be passed to expression lambdas in Record Layer (to be removed).
    @Nonnull
    public static final TypeRepository EMPTY_TYPE_REPOSITORY = TypeRepository.empty();


    /**
     * The fields of a day-time interval, ordered from most to least significant.
     */
    @Nonnull
    private static final List<ChronoUnit> DAY_TIME_FIELDS =
            ImmutableList.of(ChronoUnit.DAYS, ChronoUnit.HOURS, ChronoUnit.MINUTES, ChronoUnit.SECONDS);


    /**
     * A pattern for every legal day-time interval qualifier, indexed by the start field and then the end field.
     */
    @Nonnull
    private static final Supplier<Map<ChronoUnit, Map<ChronoUnit, Pattern>>> DAY_TIME_INTERVAL_PATTERNS_SUPPLIER =
            Suppliers.memoize(ParseHelpers::buildDayTimeIntervalPatterns);


    private ParseHelpers() {
    }

    /**
     * Attempt to parse an input value into the corresponding numerical literal {@link Value}.
     * @param valueAsString The value to parse.
     * @return The corresponding typed and literal {@link Value} object
     */
    @Nonnull
    public static Object parseDecimal(@Nonnull String valueAsString) {
        final var lastCharIdx = valueAsString.length() - 1;
        Assert.thatUnchecked(lastCharIdx >= 0);
        if (valueAsString.contains(".")) {
            final var lastCharacter = valueAsString.charAt(lastCharIdx);
            switch (lastCharacter) {
                case 'f': // fallthrough
                case 'F':
                    return Float.parseFloat(valueAsString.substring(0, lastCharIdx));
                case 'd': // fallthrough
                case 'D':
                    return Double.parseDouble(valueAsString.substring(0, lastCharIdx));
                case 'h': // fallthrough
                case 'H':
                    return Half.valueOf(valueAsString.substring(0, lastCharIdx));
                default:
                    return Double.parseDouble(valueAsString);
            }
        } else {
            final var lastCharacter = valueAsString.charAt(lastCharIdx);
            switch (lastCharacter) {
                case 'l': // fallthrough
                case 'L':
                    return Long.parseLong(valueAsString.substring(0, lastCharIdx));
                case 'i': // fallthrough
                case 'I':
                    return Integer.parseInt(valueAsString.substring(0, lastCharIdx));
                default:
                    long result = Long.parseLong(valueAsString);
                    if (Integer.MIN_VALUE <= result && result <= Integer.MAX_VALUE) {
                        return Math.toIntExact(result);
                    } else {
                        return result;
                    }
            }
        }
    }

    @Nonnull
    public static String underlineParsingError(@Nonnull Recognizer<?, ?> recognizer,
                                               @Nonnull Token offendingToken,
                                               int line,
                                               int charPositionInLine) {
        // I got this recipe from the book: "The Definitive ANTLR 4 Reference, 2nd Edition".
        final StringBuilder stringBuilder = new StringBuilder();
        final CommonTokenStream tokens = (CommonTokenStream) recognizer.getInputStream();
        final String input = tokens.getTokenSource().getInputStream().toString();
        final String[] lines = input.split("\n");
        final String errorLine = lines[line - 1];
        stringBuilder.append(errorLine).append("\n");
        stringBuilder.append(" ".repeat(Math.max(0, charPositionInLine)));
        int start = offendingToken.getStartIndex();
        int stop = offendingToken.getStopIndex();
        if (stop < start) {
            stringBuilder.append("^^"); // missing token
        } else if (start >= 0) {
            stringBuilder.append("^".repeat(Math.max(0, stop - start + 1)));
        }
        return stringBuilder.toString();
    }

    public static boolean isConstant(@Nonnull final RelationalParser.ExpressionsContext expressionsContext) {
        for (final var exp : expressionsContext.expression()) {
            if (!(exp instanceof RelationalParser.PredicatedExpressionContext)) {
                return false;
            }
            final var predicate = (RelationalParser.PredicatedExpressionContext) exp;
            if (predicate.predicate() != null) {
                return false;
            }
            final var expressionAtom = predicate.expressionAtom();
            if (!(expressionAtom instanceof RelationalParser.ConstantExpressionAtomContext)) {
                return false;
            }
        }
        return true;
    }

    @Nonnull
    public static byte[] parseBytes(String text) {
        try {
            if (text.toLowerCase(Locale.ROOT).startsWith("xstartswith_") && text.endsWith("'")) {
                String input = text.substring(text.indexOf("'") + 1, text.length() - 1);
                // pad a zero in the end if input has odd number of characters
                return input.length() % 2 == 0 ? Hex.decodeHex(input) : Hex.decodeHex(input + "0"); // of the form: XSTARTSWITH'CAFE'
            } else if (text.toLowerCase(Locale.ROOT).startsWith("x'") && text.endsWith("'")) {
                return Hex.decodeHex(text.substring(2, text.length() - 1)); // of the form: X'CAFE'
            } else if (text.toLowerCase(Locale.ROOT).startsWith("b64'") && text.endsWith("'")) {
                return Base64.getDecoder().decode(text.substring(4, text.length() - 1)); // of the form: B64'yv4='
            } else {
                throw new RelationalException("Could not parse bytes literal", ErrorCode.INVALID_BINARY_REPRESENTATION).toUncheckedWrappedException();
            }
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        } catch (IllegalArgumentException e) {
            throw new RelationalException("Could not parse bytes literal", ErrorCode.INVALID_BINARY_REPRESENTATION, e).toUncheckedWrappedException();
        }
    }

    public static long parseInterval(@Nonnull String intervalLiteral,
                                     boolean isNegative,
                                     @Nonnull ChronoUnit startField,
                                     @Nullable ChronoUnit endField) {
        final var effectiveEndField = endField == null ? startField : endField;
        final var patternForQualifier = DAY_TIME_INTERVAL_PATTERNS_SUPPLIER.get()
                .getOrDefault(startField, ImmutableMap.of()).get(effectiveEndField);
        Assert.notNullUnchecked(patternForQualifier, ErrorCode.INTERNAL_ERROR,
                () -> "invalid qualifier for day-time interval");
        final var matcher = patternForQualifier.matcher(intervalLiteral.strip());
        Assert.thatUnchecked(matcher.matches(), ErrorCode.INTERNAL_ERROR,
                () -> "'" + intervalLiteral + "' is not a valid day time interval literal");

        try {
            final var startFieldIndex = DAY_TIME_FIELDS.indexOf(startField);
            final var endFieldIndex = DAY_TIME_FIELDS.indexOf(effectiveEndField);
            var interval = Duration.ZERO;
            for (var fieldIndex = startFieldIndex; fieldIndex <= endFieldIndex; fieldIndex++) {
                final var field = DAY_TIME_FIELDS.get(fieldIndex);
                final var amount = Long.parseLong(matcher.group(field.name().toLowerCase(Locale.ROOT)));
                if (fieldIndex > startFieldIndex) {
                    // only the leading field is unbounded, the remaining ones must be within their natural range
                    final var maxAmount = field == ChronoUnit.HOURS ? 23L : 59L;
                    Assert.thatUnchecked(amount <= maxAmount, ErrorCode.INTERNAL_ERROR,
                            () -> "'" + intervalLiteral + "' is not a valid day time interval literal");
                }
                interval = interval.plus(Duration.of(amount, field));
            }
            // The fraction group is part of the pattern only when the literal ends in seconds
            if (effectiveEndField == ChronoUnit.SECONDS && matcher.group("fraction") != null) {
                final var fractionString = matcher.group("fraction");
                Assert.thatUnchecked(fractionString.length() <= 3, ErrorCode.INTERNAL_ERROR, () -> "maximum allowed precision for fractional seconds is 3");
                interval = interval.plusMillis(
                        Long.parseLong(matcher.group("fraction") + "0".repeat(3 - fractionString.length())));
            }
            final var shouldNegate = isNegative ^ "-".equals(matcher.group("sign"));
            return shouldNegate ? interval.negated().toMillis() : interval.toMillis();
        } catch (ArithmeticException | NumberFormatException ex) {
            throw new RelationalException("interval literal is larger than the maximum interval of " + Long.MAX_VALUE + " milliseconds", ErrorCode.INTERNAL_ERROR).toUncheckedWrappedException();
        }
    }

    public static boolean isNullsLast(@Nullable RelationalParser.OrderClauseContext orderClause, boolean isDescending) {
        if (orderClause == null || orderClause.nulls == null) {
            return isDescending; // Default behavior: ASC NULLS FIRST, DESC NULLS LAST
        }
        return orderClause.LAST() != null;
    }

    public static boolean isDescending(@Nullable RelationalParser.OrderClauseContext orderClause) {
        if (orderClause == null) {
            return false; // Default is ASC
        }
        return orderClause.DESC() != null;
    }

    @Nonnull
    private static Map<ChronoUnit, Map<ChronoUnit, Pattern>> buildDayTimeIntervalPatterns() {
        final var signPattern = "(?<sign>[-+])?";
        final var fieldPatterns = ImmutableList.of(
                " (?<days>\\d+)",
                " (?<hours>\\d+)",
                ":(?<minutes>\\d+)",
                ":(?<seconds>\\d+)(?:\\.(?<fraction>\\d+))?");
        final var allPatterns = ImmutableMap.<ChronoUnit, Map<ChronoUnit, Pattern>>builder();
        for (var startFieldIndex = 0; startFieldIndex < fieldPatterns.size(); startFieldIndex++) {
            final var patternsByEndField = ImmutableMap.<ChronoUnit, Pattern>builder();
            for (var endFieldIndex = startFieldIndex; endFieldIndex < fieldPatterns.size(); endFieldIndex++) {
                final var includedPatterns = fieldPatterns.subList(startFieldIndex, endFieldIndex + 1);
                // drop the separator preceding the leading field, as nothing comes before it
                final var pattern = new StringBuilder();
                pattern.append(signPattern);
                pattern.append(includedPatterns.get(0).substring(1));

                includedPatterns.subList(1, includedPatterns.size()).forEach(pattern::append);
                patternsByEndField.put(DAY_TIME_FIELDS.get(endFieldIndex), Pattern.compile(pattern.toString()));
            }
            allPatterns.put(DAY_TIME_FIELDS.get(startFieldIndex), patternsByEndField.build());
        }
        return allPatterns.build();
    }

    public static class ParseTreeLikeAdapter implements TreeLike<ParseTreeLikeAdapter> {

        @Nonnull
        private final ParseTree parseTree;

        @Nonnull
        private final Supplier<Iterable<? extends ParseTreeLikeAdapter>> children;

        private ParseTreeLikeAdapter(@Nonnull final ParseTree parseTree) {
            this.parseTree = parseTree;
            this.children = Suppliers.memoize(this::computeChildren);
        }

        @Nonnull
        @Override
        public ParseTreeLikeAdapter getThis() {
            return this;
        }

        @Nonnull
        public ParseTree getParseTree() {
            return parseTree;
        }

        @Nonnull
        public Iterable<? extends ParseTreeLikeAdapter> computeChildren() {
            final var result = ImmutableList.<ParseTreeLikeAdapter>builder();
            for (int i = 0; i < parseTree.getChildCount(); i++) {
                result.add(new ParseTreeLikeAdapter(parseTree.getChild(i)));
            }
            return result.build();
        }


        @Nonnull
        @Override
        public Iterable<? extends ParseTreeLikeAdapter> getChildren() {
            return children.get();
        }

        @Nonnull
        @Override
        public ParseTreeLikeAdapter withChildren(Iterable<? extends ParseTreeLikeAdapter> iterable) {
            throw new UnsupportedOperationException("adding children to parse tree is not supported");
        }

        @Nonnull
        public static ParseTreeLikeAdapter from(@Nonnull final ParseTree parseTree) {
            return new ParseTreeLikeAdapter(parseTree);
        }
    }
}
