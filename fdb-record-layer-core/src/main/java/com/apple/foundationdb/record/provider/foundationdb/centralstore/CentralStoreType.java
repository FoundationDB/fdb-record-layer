/*
 * CentralStoreType.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.centralstore;

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.protobuf.DescriptorProtos;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * An interface for the record types in {@link CentralStore}.
 *
 * <p>
 * An union store type would typically be annotated to allow the classpath-based registry to find it.
 * </p>
 * <pre><code>
 * &#64;AutoService(CentralStoreType.class)
 * </code></pre>
 */
public interface CentralStoreType {
    @Nonnull
    DescriptorProtos.DescriptorProto getTypeDescriptor();

    // This key expression with be concatenated with the record type key as the primary key.
    @Nonnull
    KeyExpression getPrimaryFields();

    // Each type should have a unique type key. 0~99 is reserved for Record Layer.
    int getRecordTypeKey();

    @Nonnull
    List<Index> getIndexes();
}
