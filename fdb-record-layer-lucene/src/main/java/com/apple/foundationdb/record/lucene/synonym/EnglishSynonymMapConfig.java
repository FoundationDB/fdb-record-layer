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

import com.google.auto.service.AutoService;

import java.io.InputStream;

@AutoService(SynonymMapConfig.class)
public class EnglishSynonymMapConfig implements SynonymMapConfig {
    public static final String CONFIG_NAME = "US_EN";
    private static final String FILE_NAME = "wn_s.txt";

    @Override
    public String getName() {
        return CONFIG_NAME;
    }

    @Override
    public InputStream getSynonymInputStream() {
        return SynonymMapConfig.openFile(FILE_NAME);
    }
}
