/*
 * ArrayAssert.java
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

package com.apple.foundationdb.relational.utils;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * assertj assertion for handling java.sql.Array objects.
 */
@API(API.Status.EXPERIMENTAL)
public class ArrayAssert extends AbstractAssert<ArrayAssert, Array> {
    protected ArrayAssert(Array array) {
        super(array, ArrayAssert.class);
    }

    public static ArrayAssert assertThat(Array actual) {
        return new ArrayAssert(actual);
    }

    public static boolean checkEquals(Array actual, Array expected) {
        try {
            ArrayAssert.assertThat(actual).isEqualTo(expected);
            return true;
        } catch (AssertionError afe) {
            return false;
        }
    }

    @Override
    public ArrayAssert isEqualTo(Object expected) {
        if (expected instanceof Array) {
            return isEqualTo((Array) expected);
        } else if (expected instanceof List) {
            return isEqualTo((List<?>) expected);
        }
        Assertions.fail("Expected value should be an Array or List.");
        return this;
    }

    private ArrayAssert isEqualTo(Array expected) {
        if (expected == null) {
            Assertions.assertThat(actual).isNull();
            return this;
        }
        try {
            ResultSet rs = actual.getResultSet();
            Assertions.assertThat(rs).isInstanceOf(RelationalResultSet.class);
            RelationalResultSet actualArrRs = (RelationalResultSet) rs;

            ResultSet ers = expected.getResultSet();
            Assertions.assertThat(ers).isInstanceOf(RelationalResultSet.class);
            RelationalResultSet expectedArrRs = (RelationalResultSet) ers;

            ResultSetAssert.assertThat(actualArrRs).isExactlyInAnyOrder(expectedArrRs);
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    private ArrayAssert isEqualTo(List<?> expected) {
        if (expected == null) {
            Assertions.assertThat(actual).isNull();
            return this;
        }
        try {
            ResultSet rs = actual.getResultSet();
            Assertions.assertThat(rs).isInstanceOf(RelationalResultSet.class);
            var actualValues = (Object[]) actual.getArray();
            Assertions.assertThat(actualValues.length).isEqualTo(expected.size());

            SoftAssertions assertions = new SoftAssertions();
            for (int i = 0; i < actualValues.length; i++) {
                final var actualValue = actualValues[i];
                final var expectedValue = expected.get(i);
                if (actualValue instanceof RelationalStruct) {
                    assertions.proxy(RelationalStructAssert.class, RelationalStruct.class, (RelationalStruct) actualValue).containsColumnsByName((Map<String, Object>) expectedValue);
                } else {
                    assertions.assertThat(expected.get(i)).isEqualTo(actualValue);
                }
            }
            assertions.assertAll();
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        return this;
    }
}
