/*
 * ResultSetTestUtils.java
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

import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResultSetTestUtils {

    public static Row currentRow(@Nonnull ResultSet resultSet) throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        List<Object> objects = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            objects.add(resultSet.getObject(i));
        }
        return new ArrayRow(objects.toArray());
    }

    public static Row structToRow(RelationalStruct o) {
        final StructMetaData metaData;
        try {
            metaData = o.getMetaData();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        List<Object> objects = new ArrayList<>();
        try {
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                objects.add(o.getObject(i));
            }
            return new ArrayRow(objects.toArray());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
