/*
 * DatabaseRule.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.net.URI;

public class DatabaseRule implements BeforeEachCallback, AfterEachCallback {
    private final String dbName;
    private final RecordLayerCatalogRule catalog;
    private final DatabaseTemplate.Builder template;

    public DatabaseRule(String dbName, RecordLayerCatalogRule catalog) {
        this.catalog = catalog;
        this.template = DatabaseTemplate.newBuilder();
        this.dbName = dbName;
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        catalog.deleteDatabase(URI.create("/" + dbName));
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        catalog.createDatabase(URI.create("/" + dbName), template.build());
    }

    public DatabaseRule withSchema(String schema, String template) {
        this.template.withSchema(schema, template);
        return this;
    }

    public URI getConnectionUri() {
        return URI.create("jdbc:embed:/" + dbName);
    }

    public String getPathString() {
        return "/" + dbName;
    }
}
