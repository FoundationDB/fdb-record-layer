/*
 * MultiVersionMetaDataProvider.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

/**
 * {@link RecordMetaDataProvider} that can provide multiple different versions of the metadata.
 */
public class MultiVersionMetaDataProvider implements RecordMetaDataProvider {
    private final ImmutableList<RecordMetaData> metaDatas;
    private int metaDataVersion = 0;

    public MultiVersionMetaDataProvider(RecordMetaData... metaDatas) {
        this.metaDatas = ImmutableList.copyOf(metaDatas);
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return metaDatas.get(metaDataVersion);
    }

    public int getMetaDataVersion() {
        return metaDataVersion;
    }

    public void setMetaDataVersion(int metaDataVersion) {
        this.metaDataVersion = metaDataVersion;
    }
}
