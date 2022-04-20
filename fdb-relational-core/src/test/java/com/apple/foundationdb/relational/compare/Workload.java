/*
 * Workload.java
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

package com.apple.foundationdb.relational.compare;

import com.google.protobuf.Descriptors;

import java.util.Set;

public interface Workload {

    Set<SchemaAction> getSchemaActions();

    Set<LoadAction> getLoadActions();

    int getSampleSize();

    default long getRandomSeed() {
        return System.currentTimeMillis();
    }

    Set<TransactionAction> getQueries();

    String getDatabaseName();

    interface LoadAction {
        String getSchemaName();

        Descriptors.Descriptor getDataType();

        DataGenerator getDataToLoad();

        default int getWriteBufferSize() {
            return 1;
        }
    }

    interface SchemaAction {
        //TODO(bfines) add in logic for adding Indexes, etc.

        Set<Descriptors.Descriptor> getTables();

        String getSchemaName();

        /**
         * Set if the schema should be torn down after the test runs.
         *
         * @return true if the Schema should be torn down at the end of the test run.
         */
        boolean tearDownAfterTest();

        Descriptors.FileDescriptor getSchemaFileDescriptor();

        String getLoadSchemaStatement();
    }
}
