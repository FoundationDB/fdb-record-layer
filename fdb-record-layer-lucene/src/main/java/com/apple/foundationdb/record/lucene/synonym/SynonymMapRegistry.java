/*
 * SynonymAnalyzer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.metadata.MetaDataException;
import org.apache.lucene.analysis.synonym.SynonymMap;

import javax.annotation.Nonnull;

/**
 * Registry for {@link SynonymAnalyzer}s.
 */
@API(API.Status.EXPERIMENTAL)
public interface SynonymMapRegistry {
    /**
     * Gets the synonym analyzer of the given set of synonyms.
     *
     * @param name the name of the synonym set
     * @return the analyzer over the given set of synonyms
     * @throws MetaDataException if no such tokenizer exists
     */
    @Nonnull
    SynonymMap getSynonymMap(@Nonnull String name);
}
