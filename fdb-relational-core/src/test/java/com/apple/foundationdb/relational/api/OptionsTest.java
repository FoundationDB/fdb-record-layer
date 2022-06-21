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
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OptionsTest {

    @Test
    void none() {
        assertNull(Options.NONE.getOption(Options.Name.INDEX_HINT));
    }

    @Test
    void simpleOptions() throws RelationalException {
        Options options = Options.builder()
                .withOption(Options.Name.INDEX_HINT, "foo")
                .withOption(Options.Name.CONTINUATION_PAGE_SIZE, 1)
                .build();
        assertEquals("foo", options.getOption(Options.Name.INDEX_HINT));
        assertEquals(Integer.valueOf(1), options.getOption(Options.Name.CONTINUATION_PAGE_SIZE));
    }

    @Test
    void parentChildDistinctOptions() throws RelationalException {
        Options parent = Options.builder()
                .withOption(Options.Name.INDEX_HINT, "foo")
                .build();

        Options child = Options.builder()
                .withOption(Options.Name.CONTINUATION_PAGE_SIZE, 1)
                .build();

        Options options = Options.combine(parent, child);
        assertEquals("foo", options.getOption(Options.Name.INDEX_HINT));
        assertEquals(Integer.valueOf(1), options.getOption(Options.Name.CONTINUATION_PAGE_SIZE));
    }

    @Test
    void parentChildOverrideOptions() throws RelationalException {
        Options parent = Options.builder()
                .withOption(Options.Name.INDEX_HINT, "foo")
                .build();

        Options child = Options.builder()
                .withOption(Options.Name.INDEX_HINT, "bar")
                .build();

        Options options = Options.combine(parent, child);
        assertEquals("bar", options.getOption(Options.Name.INDEX_HINT));
    }

    @Test
    void grandParentOptions() throws RelationalException {
        Options grandParent = Options.builder()
                .withOption(Options.Name.INDEX_HINT, "foo")
                .build();
        Options parent = Options.builder()
                .withOption(Options.Name.CONTINUATION_PAGE_SIZE, 1)
                .build();

        parent = Options.combine(grandParent, parent);

        Options child = Options.builder()
                .withOption(Options.Name.INDEX_HINT, "bar")
                .build();

        Options options = Options.combine(parent, child);
        assertEquals("bar", options.getOption(Options.Name.INDEX_HINT));
        assertEquals(Integer.valueOf(1), options.getOption(Options.Name.CONTINUATION_PAGE_SIZE));
    }

    @Test
    void violatedContract() throws RelationalException {
        RelationalAssertions.assertThrows(() -> Options.builder().withOption(Options.Name.INDEX_HINT, 0))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessage("Option INDEX_HINT should be of type class java.lang.String but is class java.lang.Integer");

        RelationalAssertions.assertThrows(() -> Options.builder().withOption(Options.Name.CONTINUATION_PAGE_SIZE, "foo"))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessage("Option CONTINUATION_PAGE_SIZE should be of type class java.lang.Integer but is class java.lang.String");

        RelationalAssertions.assertThrows(() -> Options.builder().withOption(Options.Name.CONTINUATION_PAGE_SIZE, -52))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessage("Option CONTINUATION_PAGE_SIZE should be in range [0, 2147483647] but is -52");

        RelationalAssertions.assertThrows(() -> Options.builder().withOption(Options.Name.CONTINUATION, new Object()))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessage("Option CONTINUATION should be of type interface com.apple.foundationdb.relational.api.Continuation but is class java.lang.Object");

    }
}
