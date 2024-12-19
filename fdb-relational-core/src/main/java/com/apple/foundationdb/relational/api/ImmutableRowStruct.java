/*
 * ImmutableRowStruct.java
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

import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Objects;

/**
 * An implementation of {@link RelationalStruct} which is backed by a {@link Row} object.
 */
public class ImmutableRowStruct extends RowStruct {

    private final Row theRow;

    public ImmutableRowStruct(@Nonnull Row theRow, @Nonnull StructMetaData metaData) {
        super(metaData);
        this.theRow = theRow;
    }

    @Override
    protected Object getObjectInternal(int zeroBasedPos) throws SQLException {
        wasNull = false;
        Objects.requireNonNull(theRow);
        try {
            Object o = theRow.getObject(zeroBasedPos);
            if (o == null) {
                wasNull = true;
            }
            return o;
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ImmutableRowStruct)) {
            return false;
        }
        final var otherImmutableRowStruct = (ImmutableRowStruct) other;
        if (otherImmutableRowStruct == this) {
            return true;
        }
        try {
            if (theRow.equals(otherImmutableRowStruct.theRow) && getMetaData().equals(otherImmutableRowStruct.getMetaData())) {
                return true;
            }
        } catch (SQLException e) {
            throw new RelationalException(e).toUncheckedWrappedException();
        }
        return false;
    }

    @Override
    public int hashCode() {
        try {
            return Objects.hash(theRow, getMetaData());
        } catch (SQLException e) {
            throw new RelationalException(e).toUncheckedWrappedException();
        }
    }
}
