/*
 * LucenePlanSerialization.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.LuceneRecordQueryPlanProto;
import com.google.protobuf.ExtensionRegistry;

import javax.annotation.Nonnull;

/**
 * Define a module-wide protobuf extension registry to be passed to all protobuf plan parsing calls.
 */
public class LucenePlanSerialization {
    private static final ExtensionRegistry extensionRegistry;

    static {
        extensionRegistry = ExtensionRegistry.newInstance();
        extensionRegistry.add(LuceneRecordQueryPlanProto.luceneIndexQueryPlan);
        extensionRegistry.add(LuceneRecordQueryPlanProto.luceneScanQueryParameters);
        extensionRegistry.add(LuceneRecordQueryPlanProto.luceneScanSpellCheckParameters);
    }

    @Nonnull
    public static ExtensionRegistry getExtensionRegistry() {
        return extensionRegistry;
    }
}
