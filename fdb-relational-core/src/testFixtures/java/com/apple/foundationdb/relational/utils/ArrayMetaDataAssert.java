/*
 * ArrayMetaDataAssert.java
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

import com.apple.foundationdb.annotation.API;


import com.apple.foundationdb.relational.api.ArrayMetaData;
import com.apple.foundationdb.relational.api.SqlTypeNamesSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.StructResultSetMetaData;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;

@API(API.Status.EXPERIMENTAL)
public class ArrayMetaDataAssert extends AbstractAssert<ArrayMetaDataAssert, ArrayMetaData> {

    public static ArrayMetaDataAssert assertThat(ArrayMetaData actual) {
        return new ArrayMetaDataAssert(actual);
    }

    protected ArrayMetaDataAssert(ArrayMetaData arrayMetaData) {
        super(arrayMetaData, ArrayMetaDataAssert.class);
    }

    @Override
    public ArrayMetaDataAssert isEqualTo(Object expected) {
        if (expected instanceof ArrayMetaData) {
            return isEqualTo((ArrayMetaData) expected);
        } else {
            return super.isEqualTo(expected);
        }
    }

    public ArrayMetaDataAssert isEqualTo(ArrayMetaData expectedMetaData) {
        try {
            SoftAssertions sa = new SoftAssertions();
            String aName = actual.getElementName();
            String eName = expectedMetaData.getElementName();
            sa.assertThat(aName).describedAs("Component Name").isEqualTo(eName);

            int aType = actual.getElementType();
            int eType = expectedMetaData.getElementType();
            sa.assertThat(aType).describedAs("Component Type").isEqualTo(eType);

            if (aType == Types.STRUCT) {
                StructMetaData aStructMd = actual.getElementStructMetaData();
                StructMetaData eStructMd = expectedMetaData.getElementStructMetaData();

                sa.proxy(StructMetaDataAssert.class, StructMetaData.class, aStructMd).isEqualTo(eStructMd);
            } else if (aType == Types.ARRAY) {
                var aArray = actual.getElementArrayMetaData();
                var eArray = expectedMetaData.getElementArrayMetaData();

                sa.proxy(ArrayMetaDataAssert.class, ArrayMetaData.class, aArray).isEqualTo(eArray);
            }
            sa.assertAll();
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        return this;
    }

    @Nonnull
    public ArrayMetaDataAssert hasComponent(@Nonnull String name, int sqlType) throws SQLException {
        if (!actual.getElementName().equalsIgnoreCase(name)) {
            Assertions.fail(String.format(Locale.ROOT, "Expecting componentName %s, got %s", name, actual.getElementName()));
        }
        if (actual.getElementType() != sqlType) {
            Assertions.fail(String.format(Locale.ROOT, "Expecting componentType %s, got %s for Array component %s", SqlTypeNamesSupport.getSqlTypeName(sqlType),
                    SqlTypeNamesSupport.getSqlTypeName(actual.getElementType()), actual.getElementName()));
        }
        return this;
    }

    public ResultSetMetaDataAssert hasStructMetadata(@Nonnull String name) throws SQLException {
        if (actual.getElementType() != Types.STRUCT) {
            Assertions.fail(String.format(Locale.ROOT, "Expecting componentType to be STRUCT, got %s", SqlTypeNamesSupport.getSqlTypeName(actual.getElementType())));
        }
        if (!actual.getElementName().equalsIgnoreCase(name)) {
            Assertions.fail(String.format(Locale.ROOT, "Expecting componentName %s, got %s", name, actual.getElementName()));
        }
        return new ResultSetMetaDataAssert(new StructResultSetMetaData(actual.getElementStructMetaData()));
    }
}
