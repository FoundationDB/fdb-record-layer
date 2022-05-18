/*
 * DdlQuery.java
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

package com.apple.foundationdb.relational.api.ddl;

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

/**
 * Interface for executing administrative metadata queries.
 */
public interface DdlQuery extends DdlPreparedAction<RelationalResultSet> {

    @Nonnull
    Type getResultSetMetadata();

    @Nonnull
    static Type constructTypeFrom(@Nonnull final List<String> columnNames) {
        return Type.Record.fromFields(IntStream.range(0, columnNames.size())
                .mapToObj(i ->
                        Type.Record.Field.of(
                                Type.primitiveType(Type.TypeCode.STRING, true),
                                Optional.of(columnNames.get(i)),
                                Optional.of(i))).collect(Collectors.toList()));
    }

    class NoOpDdlQuery implements DdlQuery {

        public static final NoOpDdlQuery INSTANCE = new NoOpDdlQuery();

        private NoOpDdlQuery() {
        }

        @Override
        public RelationalResultSet executeAction(Transaction txn) throws RelationalException {
            return null;
        }

        @Nonnull
        @Override
        public Type getResultSetMetadata() {
            return new Type.Any();
        }
    }
}
