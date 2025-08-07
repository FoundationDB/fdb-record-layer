/*
 * TransactionAction.java
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

package com.apple.foundationdb.relational.compare;

import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface TransactionAction {

    /**
     * Put the output of your tests into the ResultSet, and the testing framework will compare them to make
     * sure that they match.
     *
     * @param parameters the parameters for the action
     * @param dataSource the statement for the data
     * @return the results of the action
     * @throws RelationalException if something goes wrong
     */
    TestResult execute(@Nonnull ParameterSet parameters, @Nonnull RelationalStatement dataSource) throws RelationalException;

    /**
     * When set to true, then we will require that all results of this query should be sorted in an identical manner.
     * Use this when testing ORDER BY clauses etc.
     *
     * By default, we ignore sort order in our comparisons.
     * @return true if the sort order should be enforced, false otherwise.
     */
    default boolean enforceSortOrder() {
        return false;
    }

    interface ParameterSet {
        @Nullable
        Message getParameter(String table);

        @Nullable
        default Message getParameter(Descriptors.Descriptor type) {
            return getParameter(type.getName());
        }

        boolean hasData();
    }
}
