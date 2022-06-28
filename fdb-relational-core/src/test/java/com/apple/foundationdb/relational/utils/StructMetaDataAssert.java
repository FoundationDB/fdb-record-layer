/*
 * StructMetaDataAssert.java
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

package com.apple.foundationdb.relational.utils;


import com.apple.foundationdb.relational.api.StructMetaData;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;

import java.sql.SQLException;
import java.sql.Types;

public class StructMetaDataAssert extends AbstractAssert<StructMetaDataAssert, StructMetaData> {

    public static StructMetaDataAssert assertThat(StructMetaData actual) {
        return new StructMetaDataAssert(actual);
    }

    protected StructMetaDataAssert(StructMetaData structMetaData) {
        super(structMetaData, StructMetaDataAssert.class);
    }

    @Override
    public StructMetaDataAssert isEqualTo(Object expected) {
        if (expected instanceof StructMetaData) {
            return isEqualTo((StructMetaData) expected);
        } else {
            return super.isEqualTo(expected);
        }
    }

    public StructMetaDataAssert hasColumn(String colName) {
        try {
            for (int i = 1; i <= actual.getColumnCount(); i++) {
                if (actual.getColumnName(i).equalsIgnoreCase(colName)) {
                    return this; // success!
                }
            }
            failWithMessage("Missing column <%s>", colName);
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        return this;
    }

    public StructMetaDataAssert isEqualTo(StructMetaData expectedMetaData) {
        try {
            hasColumnCount(expectedMetaData.getColumnCount());
            SoftAssertions sa = new SoftAssertions();
            for (int i = 1; i <= expectedMetaData.getColumnCount(); i++) {
                String aName = actual.getColumnName(i);
                String eName = expectedMetaData.getColumnName(i);
                sa.assertThat(aName).describedAs("position %d Name", i).isEqualTo(eName);

                int aType = actual.getColumnType(i);
                int eType = expectedMetaData.getColumnType(i);
                sa.assertThat(aType).describedAs("position %d Type", i).isEqualTo(eType);

                if (aType == Types.STRUCT) {
                    StructMetaData aStructMd = actual.getNestedMetaData(i);
                    StructMetaData eStructMd = expectedMetaData.getNestedMetaData(i);

                    sa.proxy(StructMetaDataAssert.class, StructMetaData.class, aStructMd).isEqualTo(eStructMd);
                } else if (aType == Types.ARRAY) {
                    StructMetaData aArray = actual.getArrayMetaData(i);
                    StructMetaData eArray = expectedMetaData.getArrayMetaData(i);

                    sa.proxy(StructMetaDataAssert.class, StructMetaData.class, aArray).isEqualTo(eArray);
                }
            }
            sa.assertAll();
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        return this;
    }

    public StructMetaDataAssert hasColumnCount(int expectedNumColumns) {
        try {
            Assertions.assertThat(actual.getColumnCount())
                    .describedAs("Incorrect column count!")
                    .isEqualTo(expectedNumColumns);
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        return this;
    }

}
