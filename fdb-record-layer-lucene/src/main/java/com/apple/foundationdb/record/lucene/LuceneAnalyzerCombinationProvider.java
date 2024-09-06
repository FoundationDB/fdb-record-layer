/*
 * LuceneAnalyzerCombinationProvider.java
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

package com.apple.foundationdb.record.lucene;

import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Provide a combination of analyzers for multiple fields of one Lucene index.
 * The combinations of analyzers for index time and query time are potentially different.
 * The default analyzer chooser is used for all fields of one Lucene index except the fields which has overrides in the analyzer chooser per field mapping.
 */
public class LuceneAnalyzerCombinationProvider {

    public static final String DELINEATOR_BETWEEN_KEY_AND_VALUE = ":";

    public static final String DELINEATOR_BETWEEN_KEY_VALUE_PAIRS = ",";
    private static final Logger log = LoggerFactory.getLogger(LuceneAnalyzerCombinationProvider.class);
    private AnalyzerChooser defaultIndexAnalyzerChooser;
    private AnalyzerChooser defaultQueryAnalyzerChooser;
    private Map<String, AnalyzerChooser> indexAnalyzerChooserPerFieldOverride;
    private Map<String, AnalyzerChooser> queryAnalyzerChooserPerFieldOverride;

    public LuceneAnalyzerCombinationProvider(@Nonnull AnalyzerChooser defaultIndexAnalyzerChooser, @Nonnull AnalyzerChooser defaultQueryAnalyzerChooser,
                                             @Nullable Map<String, AnalyzerChooser> indexAnalyzerChooserPerFieldOverride, @Nullable Map<String, AnalyzerChooser> queryAnalyzerChooserPerFieldOverride) {
        this.defaultIndexAnalyzerChooser = defaultIndexAnalyzerChooser;
        this.defaultQueryAnalyzerChooser = defaultQueryAnalyzerChooser;
        this.indexAnalyzerChooserPerFieldOverride = indexAnalyzerChooserPerFieldOverride;
        this.queryAnalyzerChooserPerFieldOverride = queryAnalyzerChooserPerFieldOverride;
    }

    public LuceneAnalyzerWrapper provideIndexAnalyzer(@Nonnull String text) {
        return provideIndexAnalyzer(Collections.singletonList(text));
    }

    public LuceneAnalyzerWrapper provideIndexAnalyzer(@Nonnull List<String> texts) {
        return buildAnalyzerWrapper(texts, defaultIndexAnalyzerChooser, indexAnalyzerChooserPerFieldOverride);
    }

    public LuceneAnalyzerWrapper provideQueryAnalyzer(@Nonnull String text) {
        return provideQueryAnalyzer(Collections.singletonList(text));
    }

    public LuceneAnalyzerWrapper provideQueryAnalyzer(@Nonnull List<String> texts) {
        return buildAnalyzerWrapper(texts, defaultQueryAnalyzerChooser, queryAnalyzerChooserPerFieldOverride);
    }

    @SuppressWarnings("PMD.CloseResource")
    private static LuceneAnalyzerWrapper buildAnalyzerWrapper(@Nonnull List<String> texts,
                                                              @Nonnull AnalyzerChooser defaultAnalyzerChooser,
                                                              @Nullable Map<String, AnalyzerChooser> customizedAnalyzerChooserPerField) {
        final ProfileCode profileCode = new ProfileCode();
        final LuceneAnalyzerWrapper defaultAnalyzerWrapper = defaultAnalyzerChooser.chooseAnalyzer(texts);
        profileCode.add("chooseAnalyzer" + defaultAnalyzerChooser.getClass().getSimpleName());
        if (customizedAnalyzerChooserPerField != null) {
            // The order of keys matters because the identifier for each map needs to be consistent
            // TODO collect into a tree map, and not an intermediate map
            SortedMap<String, LuceneAnalyzerWrapper> analyzerWrapperMap = new TreeMap<>(customizedAnalyzerChooserPerField.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().chooseAnalyzer(texts))));
            profileCode.add("createAnalyzerWrapperMap");

            PerFieldAnalyzerWrapper analyzerWrapper = new PerFieldAnalyzerWrapper(defaultAnalyzerWrapper.getAnalyzer(),
                    analyzerWrapperMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getAnalyzer())));
            profileCode.add("perFieldAnalyzerWrapper");
            final String analyzerId = buildAnalyzerIdentifier(defaultAnalyzerWrapper, analyzerWrapperMap);
            profileCode.add("analyzerId");
            final LuceneAnalyzerWrapper luceneAnalyzerWrapper = new LuceneAnalyzerWrapper(analyzerId, analyzerWrapper);
            profileCode.add("result");
            log.debug(profileCode.message("buildAnalyzerWrapper"));
            return luceneAnalyzerWrapper;
        } else {
            return defaultAnalyzerWrapper;
        }
    }

    private static String buildAnalyzerIdentifier(@Nonnull LuceneAnalyzerWrapper defaultAnalyzerWrapper, @Nonnull Map<String, LuceneAnalyzerWrapper> analyzerWrapperMap) {
        final StringBuilder builder = new StringBuilder();
        builder.append(defaultAnalyzerWrapper.getUniqueIdentifier());
        for (String id : analyzerWrapperMap.keySet()) {
            builder.append(LuceneIndexOptions.DELIMITER_BETWEEN_ELEMENTS);
            builder.append(id);
        }
        return builder.toString();
    }
}
