/*
 * FieldDescription.java
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

import com.apple.foundationdb.annotation.API;

import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Types;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A description of an individual field.
 *
 * Note that this representation is indicative of JDBC-API level information. That is, it is used to hold
 * information that is necessary to represent a given column field in a JDBC MetaData API.
 */
@API(API.Status.EXPERIMENTAL)
public class FieldDescription {
    private final String fieldName;
    private final int sqlTypeCode; //taken from java.sql.Types

    private final int nullable;

    private final StructMetaData fieldMetaData;

    private final ArrayMetaData arrayMetaData;

    //indicates a column that isn't part of the DDL for a query, but is part of the returned
    //tuple (and therefore necessary to keep so that our positional ordering is intact)
    private final boolean phantom;
    private final Supplier<Integer> hashCodeSupplier;

    /**
     * Create a primitive field.
     *
     * This is a convenience factory method for a more complicated constructor. Equivalent to
     * {@link #primitive(String, int, int, boolean)}, where {@code phantom == false}.
     *
     * @param fieldName the name of the field.
     * @param sqlTypeCode the SQL type code for this field. Should match values found in {@link Types}
     * @param nullable one of {@link java.sql.DatabaseMetaData#columnNoNulls},
     *      {@link java.sql.DatabaseMetaData#columnNullable}, or {@link java.sql.DatabaseMetaData#columnNullableUnknown}.
     * @return a FieldDescription for the field.
     */
    public static FieldDescription primitive(@Nonnull String fieldName, int sqlTypeCode, int nullable) {
        return primitive(fieldName, sqlTypeCode, nullable, false);
    }

    /**
     * Create a primitive field.
     *
     * This is a convenience factory method for a more complicated constructor.
     *
     * @param fieldName the name of the field.
     * @param sqlTypeCode the SQL type code for this field. Should match values found in {@link Types}
     * @param nullable one of {@link java.sql.DatabaseMetaData#columnNoNulls},
     *      {@link java.sql.DatabaseMetaData#columnNullable}, or {@link java.sql.DatabaseMetaData#columnNullableUnknown}.
     * @param phantom if true, this column should be treated as "phantom" in the API. That is, its entries
     *                  are present in the returned row, but are not represented as part of the "return value". In other
     *                  words, the values take up space in the physical arrays, but are not considered part of the actual
     *                  return of the query, requiring the implementation to adjust for the field's position.
     * @return a FieldDescription for the field.
     */
    public static FieldDescription primitive(@Nonnull String fieldName, int sqlTypeCode, int nullable, boolean phantom) {
        return new FieldDescription(fieldName, sqlTypeCode, nullable, phantom, null, null);
    }

    /**
     * Create a struct field.
     *
     * This is a convenience factory method for a more complicated constructor.
     *
     * @param fieldName the name of the field.
     * @param nullable one of {@link java.sql.DatabaseMetaData#columnNoNulls},
     *      {@link java.sql.DatabaseMetaData#columnNullable}, or {@link java.sql.DatabaseMetaData#columnNullableUnknown}.
     * @param definition the definition of the struct value itself.
     * @return a FieldDescription for the field.
     */
    public static FieldDescription struct(@Nonnull String fieldName, int nullable, StructMetaData definition) {
        return new FieldDescription(fieldName, Types.STRUCT, nullable, false, definition, null);
    }

    /**
     * Create an array field.
     *
     * This is a convenience factory method for a more complicated constructor.
     *
     * @param fieldName the name of the field.
     * @param nullable one of {@link java.sql.DatabaseMetaData#columnNoNulls},
     *      {@link java.sql.DatabaseMetaData#columnNullable}, or {@link java.sql.DatabaseMetaData#columnNullableUnknown}.
     * @param definition the metadata description of the contents of the array.
     * @return a FieldDescription for the field.
     */
    public static FieldDescription array(@Nonnull String fieldName, int nullable, ArrayMetaData definition) {
        return new FieldDescription(fieldName, Types.ARRAY, nullable, false, null, definition);
    }

    private FieldDescription(String fieldName,
                             int sqlType,
                             int nullable,
                             boolean phantom,
                             @Nullable StructMetaData fieldMetaData,
                             @Nullable ArrayMetaData arrayMetaData) {
        this.fieldName = fieldName;
        this.sqlTypeCode = sqlType;
        this.nullable = nullable;
        this.phantom = phantom;
        this.fieldMetaData = fieldMetaData;
        this.arrayMetaData = arrayMetaData;
        this.hashCodeSupplier = Suppliers.memoize(this::calculateHashCode);
    }

    public String getName() {
        return fieldName;
    }

    public int getSqlTypeCode() {
        return sqlTypeCode;
    }

    public boolean isStruct() {
        return sqlTypeCode == Types.STRUCT;
    }

    public boolean isArray() {
        return sqlTypeCode == Types.ARRAY;
    }

    public StructMetaData getFieldMetaData() {
        return fieldMetaData;
    }

    public ArrayMetaData getArrayMetaData() {
        return arrayMetaData;
    }

    public int isNullable() {
        return nullable;
    }

    public boolean isPhantom() {
        return phantom;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof FieldDescription)) {
            return false;
        }
        final var otherDescription = (FieldDescription) other;
        if (otherDescription == this) {
            return true;
        }
        if (!fieldName.equals(otherDescription.fieldName) ||
                sqlTypeCode != otherDescription.sqlTypeCode ||
                nullable != otherDescription.nullable) {
            return false;
        }
        if (fieldMetaData == null) {
            if (otherDescription.fieldMetaData != null) {
                return false;
            }
        } else {
            if (!fieldMetaData.equals(otherDescription.fieldMetaData)) {
                return false;
            }
        }
        if (arrayMetaData == null) {
            return otherDescription.arrayMetaData == null;
        } else {
            return arrayMetaData.equals(otherDescription.arrayMetaData);
        }
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int calculateHashCode() {
        return Objects.hash(fieldName, sqlTypeCode, nullable, fieldMetaData, arrayMetaData);
    }
}
