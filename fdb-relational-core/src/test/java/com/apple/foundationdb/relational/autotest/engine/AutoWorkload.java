/*
 * AutoWorkload.java
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

package com.apple.foundationdb.relational.autotest.engine;

import com.apple.foundationdb.relational.autotest.Connector;
import com.apple.foundationdb.relational.autotest.DataSet;
import com.apple.foundationdb.relational.autotest.SchemaDescription;
import com.apple.foundationdb.relational.autotest.WorkloadConfig;

import java.net.URI;
import java.util.List;

class AutoWorkload {
    private final List<Connector> connectors;
    private final URI dbPath;
    // this is the stuff that makes up a test
    private final SchemaDescription schema;
    private final DataSet dataSet;
    private final String displayName;
    private final WorkloadConfig config;

    public AutoWorkload(SchemaDescription schema,
                        DataSet dataSet,
                        URI dbPath,
                        List<Connector> connectors,
                        String displayName,
                        WorkloadConfig config) {
        this.schema = schema;
        this.connectors = connectors;
        this.dbPath = dbPath;
        this.dataSet = dataSet;
        this.displayName = displayName;
        this.config = config;
    }

    public List<Connector> getConnectors() {
        return connectors;
    }

    public SchemaDescription getSchema() {
        return schema;
    }

    public URI getDatabasePath() {
        return dbPath;
    }

    public String getDisplayName() {
        return displayName;
    }

    public DataSet getData() {
        return dataSet;
    }

    public WorkloadConfig getConfig() {
        return config;
    }
}
