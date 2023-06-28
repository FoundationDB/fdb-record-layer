/*
 * LuceneQueryParserFactory.java
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

package com.apple.foundationdb.record.lucene.search;

import com.apple.foundationdb.record.metadata.MetaDataException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * The provider for the implementations of {@link LuceneQueryParserFactory}.
 * The factory implementation can be provided through an implementation of the {@link LuceneQueryParserFactory} that is marked with
 * an {@link com.google.auto.service.AutoService} annotation (at most 1 such implementation can be marked). If no
 * implementation is marked as {@link com.google.auto.service.AutoService} then a default implementation will be selected.
 */
public class LuceneQueryParserFactoryProvider {
    @Nonnull
    private static final LuceneQueryParserFactoryProvider INSTANCE = new LuceneQueryParserFactoryProvider();

    @Nonnull
    private final LuceneQueryParserFactory parserFactory;

    private LuceneQueryParserFactoryProvider() {
        parserFactory = initRegistry();
    }

    @Nonnull
    public static LuceneQueryParserFactoryProvider instance() {
        return INSTANCE;
    }

    @Nonnull
    public LuceneQueryParserFactory getParserFactory() {
        return parserFactory;
    }

    @Nonnull
    private static LuceneQueryParserFactory initRegistry() {
        LuceneQueryParserFactory first = null;
        for (LuceneQueryParserFactory factory : ServiceLoader.load(LuceneQueryParserFactory.class)) {
            if (first == null) {
                first = factory;
            } else {
                throw new MetaDataException("Too many query parser factories");
            }
        }

        if (first == null) {
            first = new DefaultParserFactory();
        }

        return first;
    }

    /**
     * The default implementation is a {@link ConfigAwareQueryParser} with the default list of stop words.
     * This can be overridden by extenders, e.g. to provide another list of stop words.
     */
    private static class DefaultParserFactory implements LuceneQueryParserFactory {
        @Nonnull
        @Override
        public QueryParser createConfigAwareQueryParser(String[] fields, Analyzer analyzer, @Nonnull final Map<String, PointsConfig> pointsConfig) {
            QueryParser parser = new LuceneOptimizedStopWordsQueryParser(fields, analyzer, pointsConfig, EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
            parser.setDefaultOperator(QueryParser.Operator.OR);
            return parser;
        }
    }
}
