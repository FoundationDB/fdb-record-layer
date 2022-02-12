/*
 * SynonymMapConfig.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.synonym;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;

import javax.annotation.Nonnull;
import java.io.InputStream;

@API(API.Status.EXPERIMENTAL)
public interface SynonymMapConfig {
    /**
     * Get the name of the synonymMap to build.
     * @return the name
     */
    String getName();

    /**
     * Get solr format input stream to build the synonymMap.
     * @return the input stream
     */
    InputStream getSynonymInputStream();

    @Nonnull
    static InputStream openFile(@Nonnull String file) {
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
        if (stream == null) {
            throw new RecordCoreException("Synonym file not found").addLogInfo(LuceneLogMessageKeys.FILE_NAME, file);
        }
        return stream;
    }
}
