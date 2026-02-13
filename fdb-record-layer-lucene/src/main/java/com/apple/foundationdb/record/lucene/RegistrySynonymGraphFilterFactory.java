/*
 * RegistrySynonymGraphFilterFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.lucene.synonym.SynonymMapRegistryImpl;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import java.util.Map;

/**
 * A SynonymGraphFilterFactory which uses an underlying Registry to statically cache
 * synonym mappings, which is _significantly_ more efficient when using lots of distinct analyzers (such
 * as during highlighting, or with lots of parallel record stores).
 */
public final class RegistrySynonymGraphFilterFactory extends TokenFilterFactory {
    private final SynonymMap map;

    private final boolean ignoreCase;

    public RegistrySynonymGraphFilterFactory(final Map<String, String> args) {
        super(args);
        String synonymName = get(args, "synonyms");
        this.ignoreCase = getBoolean(args, "ignoreCase", true);
        this.map = SynonymMapRegistryImpl.instance().getSynonymMap(synonymName);
    }

    @Override
    public TokenStream create(final TokenStream input) {
        if (map == null) {
            return input;
        } else {
            return new SynonymGraphFilter(input, map, ignoreCase);
        }
    }
}
