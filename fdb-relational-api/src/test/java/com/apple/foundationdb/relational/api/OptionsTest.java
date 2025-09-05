/*
 * OptionsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.OptionsTestHelper;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OptionsTest {
    @Test
    void none() {
        assertNull(Options.NONE.getOption(Options.Name.INDEX_HINT));
    }

    @Test
    void simpleOptions() throws SQLException {
        Options options = Options.builder()
                .withOption(Options.Name.INDEX_HINT, "foo")
                .withOption(Options.Name.MAX_ROWS, 1)
                .build();
        assertEquals("foo", options.getOption(Options.Name.INDEX_HINT));
        assertEquals(Integer.valueOf(1), options.getOption(Options.Name.MAX_ROWS));
    }

    @Test
    void parentChildDistinctOptions() throws SQLException {
        Options parent = Options.builder()
                .withOption(Options.Name.INDEX_HINT, "foo")
                .build();

        Options child = Options.builder()
                .withOption(Options.Name.MAX_ROWS, 1)
                .build();

        Options options = parent.withChild(child);
        assertEquals("foo", options.getOption(Options.Name.INDEX_HINT));
        assertEquals(Integer.valueOf(1), options.getOption(Options.Name.MAX_ROWS));
        // Assert we get child back if parent == child params.
        Assertions.assertThat(child.withChild(child)).isEqualTo(child);
        // Build a child w/ non-zero parent opts... should throw.
        Options.Builder builder = Options.builder()
                .withOption(Options.Name.MAX_ROWS, 1);
        builder.setParentOption(parent);
        child = builder.build();
        SQLException re = null;
        try {
            parent.withChild(child);
        } catch (SQLException e) {
            re = e;
        }
        org.junit.jupiter.api.Assertions.assertNotNull(re);
        Assertions.assertThat(re.getSQLState()).isEqualTo(ErrorCode.INTERNAL_ERROR.getErrorCode());
    }

    @Test
    void parentChildOverrideOptions() throws SQLException {
        Options parent = Options.builder()
                .withOption(Options.Name.INDEX_HINT, "foo")
                .build();

        Options child = Options.builder()
                .withOption(Options.Name.INDEX_HINT, "bar")
                .build();

        Options options = parent.withChild(child);
        assertEquals("bar", options.getOption(Options.Name.INDEX_HINT));
    }

    @Test
    void grandParentOptions() throws SQLException {
        Options grandParent = Options.builder()
                .withOption(Options.Name.INDEX_HINT, "foo")
                .build();
        Options parent = Options.builder()
                .withOption(Options.Name.MAX_ROWS, 1)
                .build();

        parent = grandParent.withChild(parent);

        Options child = Options.builder()
                .withOption(Options.Name.INDEX_HINT, "bar")
                .build();

        Options options = parent.withChild(child);
        assertEquals("bar", options.getOption(Options.Name.INDEX_HINT));
        assertEquals(Integer.valueOf(1), options.getOption(Options.Name.MAX_ROWS));
    }

    @Test
    void violatedContract() {
        RelationalAssertions.assertThrowsSqlException(() -> Options.builder().withOption(Options.Name.INDEX_HINT, 0))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessage("Option INDEX_HINT should be of type class java.lang.String but is class java.lang.Integer");

        RelationalAssertions.assertThrowsSqlException(() -> Options.builder().withOption(Options.Name.MAX_ROWS, "foo"))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessage("Option MAX_ROWS should be of type class java.lang.Integer but is class java.lang.String");

        RelationalAssertions.assertThrowsSqlException(() -> Options.builder().withOption(Options.Name.MAX_ROWS, -52))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessage("Option MAX_ROWS should be in range [0, 2147483647] but is -52");

        RelationalAssertions.assertThrowsSqlException(() -> Options.builder().withOption(Options.Name.CONTINUATION, new Object()))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessage("Option CONTINUATION should be of type interface com.apple.foundationdb.relational.api.Continuation but is class java.lang.Object");

        RelationalAssertions.assertThrowsSqlException(() -> Options.builder().withOption(Options.Name.DISABLED_PLANNER_RULES, "foo"))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessage("Option DISABLED_PLANNER_RULES should be of a collection type instead of java.lang.String");

        RelationalAssertions.assertThrowsSqlException(() -> Options.builder().withOption(Options.Name.DISABLED_PLANNER_RULES, ImmutableSet.of(-1)))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessage("Element of collection option DISABLED_PLANNER_RULES violated contract: Option DISABLED_PLANNER_RULES should be of type class java.lang.String but is class java.lang.Integer");
    }

    @Test
    void testDefault() {
        assertEquals((Integer) Options.NONE.getOption(Options.Name.MAX_ROWS), Integer.MAX_VALUE);
    }

    @Test
    void testPropertiesConversion() throws Exception {
        final Options nonDefault = OptionsTestHelper.nonDefaultOptions();
        final Properties asProps = Options.toProperties(nonDefault);
        final Options fromProps = Options.fromProperties(asProps);
        assertEquals(nonDefault, fromProps);
    }

}
