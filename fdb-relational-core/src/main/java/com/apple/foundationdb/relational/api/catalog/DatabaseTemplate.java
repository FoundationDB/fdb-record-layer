/*
 * DatabaseTemplate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.catalog;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@ExcludeFromJacocoGeneratedReport //this is going to be removed soon
@Deprecated //we don't want to do things this way any more
@API(API.Status.EXPERIMENTAL)
public class DatabaseTemplate {
    private final Map<String, String> schemaToTemplateNameMap;

    public DatabaseTemplate(Map<String, String> schemaToTemplateNameMap) {
        this.schemaToTemplateNameMap = schemaToTemplateNameMap;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Get the mapping from defined schema names to their corresponding template names.
     *
     * @return a map of schema names to their corresponding template names;
     */
    public Map<String, String> getSchemaToTemplateNameMap() {
        return Collections.unmodifiableMap(schemaToTemplateNameMap);
    }

    @ExcludeFromJacocoGeneratedReport //this is going to be removed soon
    public static class Builder {
        private final Map<String, String> schemaToTemplateNameMap = new HashMap<>();

        public Builder withSchema(String schema, String schemaTemplate) {
            //TODO(bfines) here might be a decent place to verify that the schema template actually exists
            schemaToTemplateNameMap.put(schema, schemaTemplate);
            return this;
        }

        public DatabaseTemplate build() {
            return new DatabaseTemplate(Collections.unmodifiableMap(schemaToTemplateNameMap));
        }
    }
}
