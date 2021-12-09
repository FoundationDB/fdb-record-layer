/*
 * RecordLayerAnalyzer.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.util.IOUtils;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public abstract class RecordLayerAnalyzer extends Analyzer {
    /**
     * An immutable stopword set.
     */
    protected final CharArraySet stopwords;

    /**
     * Returns the analyzer's stopword set or an empty set if the analyzer has no
     * stopwords.
     *
     * @return the analyzer's stopword set or an empty set if the analyzer has no
     *         stopwords
     */
    public CharArraySet getStopwordSet() {
        return stopwords;
    }

    /**
     * Creates a new instance initialized with the given stopword set.
     *
     * @param stopwords
     *          the analyzer's stopword set
     */
    protected RecordLayerAnalyzer(final CharArraySet stopwords) {
        super(Analyzer.PER_FIELD_REUSE_STRATEGY);
        // analyzers should use char array set for stopwords!
        this.stopwords = stopwords == null ? CharArraySet.EMPTY_SET : CharArraySet
                .unmodifiableSet(CharArraySet.copy(stopwords));
    }

    /**
     * Creates a new Analyzer with an empty stopword set.
     */
    protected RecordLayerAnalyzer() {
        this(null);
    }

    /**
     * Creates a CharArraySet from a file resource associated with a class. (See
     * {@link Class#getResourceAsStream(String)}).
     *
     * @param ignoreCase
     *          <code>true</code> if the set should ignore the case of the
     *          stopwords, otherwise <code>false</code>
     * @param aClass
     *          a class that is associated with the given stopwordResource
     * @param resource
     *          name of the resource file associated with the given class
     * @param comment
     *          comment string to ignore in the stopword file
     * @return a CharArraySet containing the distinct stopwords from the given
     *         file
     * @throws IOException
     *           if loading the stopwords throws an {@link IOException}
     */
    protected static CharArraySet loadStopwordSet(final boolean ignoreCase,
                                                  final Class<? extends Analyzer> aClass, final String resource,
                                                  final String comment) throws IOException {
        Reader reader = null;
        try {
            reader = IOUtils.getDecodingReader(aClass.getResourceAsStream(resource), StandardCharsets.UTF_8);
            return WordlistLoader.getWordSet(reader, comment, new CharArraySet(16, ignoreCase));
        } finally {
            IOUtils.close(reader);
        }

    }

    /**
     * Creates a CharArraySet from a path.
     *
     * @param stopwords
     *          the stopwords file to load
     * @return a CharArraySet containing the distinct stopwords from the given
     *         file
     * @throws IOException
     *           if loading the stopwords throws an {@link IOException}
     */
    protected static CharArraySet loadStopwordSet(Path stopwords) throws IOException {
        Reader reader = null;
        try {
            reader = Files.newBufferedReader(stopwords, StandardCharsets.UTF_8);
            return WordlistLoader.getWordSet(reader);
        } finally {
            IOUtils.close(reader);
        }
    }

    /**
     * Creates a CharArraySet from a file.
     *
     * @param stopwords
     *          the stopwords reader to load
     *
     * @return a CharArraySet containing the distinct stopwords from the given
     *         reader
     * @throws IOException
     *           if loading the stopwords throws an {@link IOException}
     */
    protected static CharArraySet loadStopwordSet(Reader stopwords) throws IOException {
        try {
            return WordlistLoader.getWordSet(stopwords);
        } finally {
            IOUtils.close(stopwords);
        }
    }

    public void addTextFieldToDocument(@Nonnull final String fieldName, @Nonnull final Object value,
                                       @Nonnull final LuceneFieldKeyExpression expression, @Nonnull final Document document) {
        if (expression.getType() != LuceneKeyExpression.FieldType.STRING) {
            throw new RecordCoreException("Invalid field type to add text field to document")
                    .addLogInfo(LogMessageKeys.LUCENE_FIELD_TYPE, expression.getType().name());
        }
        document.add(new TextField(fieldName, value == null ? "" : value.toString(), expression.isStored() ? Field.Store.YES : Field.Store.NO));
    }

    @Nonnull
    public List<String> overrideFieldNamesIfNeeded(@Nonnull final List<String> fieldNames) {
        return fieldNames;
    }
}
