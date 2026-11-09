/*
 * ParseHelpersTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParseHelpersTest {
    @Nonnull
    private static Duration duration(long days, long hours, long minutes, long seconds, long millis) {
        return Duration.ofDays(days)
                .plusHours(hours)
                .plusMinutes(minutes)
                .plusSeconds(seconds)
                .plusMillis(millis);
    }

    @Nonnull
    private static Stream<Arguments> validIntervalLiteralsSource() {
        return Stream.of(
                // DAY
                Arguments.of("0", ChronoUnit.DAYS, null, false, Duration.ZERO),
                Arguments.of("1", ChronoUnit.DAYS, ChronoUnit.DAYS, false, Duration.ofDays(1)),
                Arguments.of("02", ChronoUnit.DAYS, null, false, Duration.ofDays(2)),
                Arguments.of("365", ChronoUnit.DAYS, null, false, Duration.ofDays(365)),

                // DAY TO HOUR
                Arguments.of("1 5", ChronoUnit.DAYS, ChronoUnit.HOURS, false, duration(1, 5, 0, 0, 0)),
                Arguments.of("3 23", ChronoUnit.DAYS, ChronoUnit.HOURS, false, duration(3, 23, 0, 0, 0)),

                // DAY TO MINUTE
                Arguments.of("1 2:3", ChronoUnit.DAYS, ChronoUnit.MINUTES, false, duration(1, 2, 3, 0, 0)),
                Arguments.of("9 23:59", ChronoUnit.DAYS, ChronoUnit.MINUTES, false, duration(9, 23, 59, 0, 0)),

                // DAY TO SECOND
                Arguments.of("3 23:59:59", ChronoUnit.DAYS, ChronoUnit.SECONDS, false, duration(3, 23, 59, 59, 0)),
                Arguments.of("3 04:05:06.7", ChronoUnit.DAYS, ChronoUnit.SECONDS, false, duration(3, 4, 5, 6, 700)),
                Arguments.of("3 04:05:06.089", ChronoUnit.DAYS, ChronoUnit.SECONDS, false, duration(3, 4, 5, 6, 89)),
                Arguments.of("3 04:05:06.007", ChronoUnit.DAYS, ChronoUnit.SECONDS, false, duration(3, 4, 5, 6, 7)),
                Arguments.of("   3 04:05:06.007    ", ChronoUnit.DAYS, ChronoUnit.SECONDS, false, duration(3, 4, 5, 6, 7)),
                Arguments.of("+3 23:59:59", ChronoUnit.DAYS, ChronoUnit.SECONDS, false, duration(3, 23, 59, 59, 0)),
                Arguments.of("-3 23:59:59", ChronoUnit.DAYS, ChronoUnit.SECONDS, false, duration(3, 23, 59, 59, 0).negated()),
                Arguments.of("3 23:59:59", ChronoUnit.DAYS, ChronoUnit.SECONDS, true, duration(3, 23, 59, 59, 0).negated()),
                Arguments.of("-3 23:59:59", ChronoUnit.DAYS, ChronoUnit.SECONDS, true, duration(3, 23, 59, 59, 0)),

                // HOUR
                Arguments.of("1", ChronoUnit.HOURS, null, false, Duration.ofHours(1)),
                Arguments.of("05", ChronoUnit.HOURS, ChronoUnit.HOURS, false, Duration.ofHours(5)),
                Arguments.of("48", ChronoUnit.HOURS, null, false, Duration.ofHours(48)),

                // HOUR TO MINUTE
                Arguments.of("1:02", ChronoUnit.HOURS, ChronoUnit.MINUTES, false, duration(0, 1, 2, 0, 0)),
                Arguments.of("36:15", ChronoUnit.HOURS, ChronoUnit.MINUTES, false, duration(0, 36, 15, 0, 0)),

                // HOUR TO SECOND
                Arguments.of("1:2:3", ChronoUnit.HOURS, ChronoUnit.SECONDS, false, duration(0, 1, 2, 3, 0)),
                Arguments.of("12:34:56.12", ChronoUnit.HOURS, ChronoUnit.SECONDS, false, duration(0, 12, 34, 56, 120)),
                Arguments.of("100:00:00.5", ChronoUnit.HOURS, ChronoUnit.SECONDS, false, duration(0, 100, 0, 0, 500)),
                Arguments.of("+12:34:56.789", ChronoUnit.HOURS, ChronoUnit.SECONDS, false, duration(0, 12, 34, 56, 789)),
                Arguments.of("-12:34:56.789", ChronoUnit.HOURS, ChronoUnit.SECONDS, false, duration(0, 12, 34, 56, 789).negated()),
                Arguments.of("12:34:56.789", ChronoUnit.HOURS, ChronoUnit.SECONDS, true, duration(0, 12, 34, 56, 789).negated()),
                Arguments.of("-12:34:56.789", ChronoUnit.HOURS, ChronoUnit.SECONDS, true, duration(0, 12, 34, 56, 789)),

                // MINUTE
                Arguments.of("07", ChronoUnit.MINUTES, null, false, Duration.ofMinutes(7)),
                Arguments.of("90", ChronoUnit.MINUTES, null, false, Duration.ofMinutes(90)),

                // MINUTE TO SECOND
                Arguments.of("1:2", ChronoUnit.MINUTES, ChronoUnit.SECONDS, false, duration(0, 0, 1, 2, 0)),
                Arguments.of("120:30", ChronoUnit.MINUTES, ChronoUnit.SECONDS, false, duration(0, 0, 120, 30, 0)),
                Arguments.of("05:01.25", ChronoUnit.MINUTES, ChronoUnit.SECONDS, false, duration(0, 0, 5, 1, 250)),
                Arguments.of("+05:01.25", ChronoUnit.MINUTES, ChronoUnit.SECONDS, false, duration(0, 0, 5, 1, 250)),
                Arguments.of("-05:01.125", ChronoUnit.MINUTES, ChronoUnit.SECONDS, false, duration(0, 0, 5, 1, 125).negated()),
                Arguments.of("05:01.125", ChronoUnit.MINUTES, ChronoUnit.SECONDS, true, duration(0, 0, 5, 1, 125).negated()),
                Arguments.of("-05:01.125", ChronoUnit.MINUTES, ChronoUnit.SECONDS, true, duration(0, 0, 5, 1, 125)),

                // SECOND
                Arguments.of("3600", ChronoUnit.SECONDS, null, false, Duration.ofHours(1)),
                Arguments.of("0.001", ChronoUnit.SECONDS, null, false, Duration.ofMillis(1)),
                Arguments.of("+1.5", ChronoUnit.SECONDS, null, false, Duration.ofMillis(1500)),
                Arguments.of("-1.5", ChronoUnit.SECONDS, null, false, Duration.ofMillis(1500).negated()),
                Arguments.of("1.5", ChronoUnit.SECONDS, null, true, Duration.ofMillis(1500).negated()),
                Arguments.of("-1.5", ChronoUnit.SECONDS, null, true, Duration.ofMillis(1500)));
    }

    @ParameterizedTest
    @MethodSource("validIntervalLiteralsSource")
    void testParseValidIntervalLiterals(@Nonnull String intervalLiteralString,
                                        @Nonnull ChronoUnit startField,
                                        @Nullable ChronoUnit endField,
                                        boolean isNegative,
                                        @Nonnull Duration expected) {
        assertEquals(expected.toMillis(),
                ParseHelpers.parseIntervalLiteral(intervalLiteralString, isNegative, startField, endField));
    }

    @Nonnull
    private static Stream<Arguments> invalidIntervalLiteralsSource() {
        return Stream.of(
                // malformed literals
                Arguments.of("", ChronoUnit.DAYS, null, ErrorCode.INVALID_INTERVAL_FORMAT),
                Arguments.of("abc", ChronoUnit.DAYS, null, ErrorCode.INVALID_INTERVAL_FORMAT),
                Arguments.of("--1", ChronoUnit.DAYS, null, ErrorCode.INVALID_INTERVAL_FORMAT),
                // invalid order of qualifiers
                Arguments.of("1 2", ChronoUnit.HOURS, ChronoUnit.DAYS, ErrorCode.INVALID_INTERVAL_FORMAT),
                // literal does not match the qualifier
                Arguments.of("1 2", ChronoUnit.DAYS, null, ErrorCode.INVALID_INTERVAL_FORMAT),
                Arguments.of("1", ChronoUnit.DAYS, ChronoUnit.HOURS, ErrorCode.INVALID_INTERVAL_FORMAT),
                Arguments.of("1:2", ChronoUnit.DAYS, ChronoUnit.HOURS, ErrorCode.INVALID_INTERVAL_FORMAT),
                Arguments.of("1.5", ChronoUnit.MINUTES, null, ErrorCode.INVALID_INTERVAL_FORMAT),
                // non-leading fields out of range
                Arguments.of("1 24", ChronoUnit.DAYS, ChronoUnit.HOURS, ErrorCode.INTERVAL_FIELD_OVERFLOW),
                Arguments.of("1:60", ChronoUnit.HOURS, ChronoUnit.MINUTES, ErrorCode.INTERVAL_FIELD_OVERFLOW),
                Arguments.of("1:60", ChronoUnit.MINUTES, ChronoUnit.SECONDS, ErrorCode.INTERVAL_FIELD_OVERFLOW),
                // fractional seconds precision exceeds milliseconds
                Arguments.of("1.1234", ChronoUnit.SECONDS, null, ErrorCode.UNSUPPORTED_OPERATION),
                // Interval too large
                Arguments.of("99999999999999999999", ChronoUnit.DAYS, null, ErrorCode.INTERVAL_FIELD_OVERFLOW));
    }

    @ParameterizedTest
    @MethodSource("invalidIntervalLiteralsSource")
    void testParseInvalidIntervalLiterals(@Nonnull String intervalLiteralString,
                                          @Nonnull ChronoUnit startField,
                                          @Nullable ChronoUnit endField,
                                          @Nonnull ErrorCode expectedErrorCode) {
        final var exception = assertThrows(UncheckedRelationalException.class,
                () -> ParseHelpers.parseIntervalLiteral(intervalLiteralString, false, startField, endField));
        assertEquals(expectedErrorCode, exception.unwrap().getErrorCode());
    }
}
