/*
 * LuceneFunctionKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionWithChildren;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.lucene.search.ScoreDoc;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.apple.foundationdb.record.lucene.LuceneFunctionNames.LUCENE_SORT_BY_RELEVANCE;

/**
 * Lucene function key expressions.
 * These are just markers, interpreted by {@link LuceneDocumentFromRecord}.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class LuceneFunctionKeyExpression extends FunctionKeyExpression {
    protected LuceneFunctionKeyExpression(@Nonnull String name, @Nonnull KeyExpression arguments) {
        super(name, arguments);
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> rec, @Nullable Message message, @Nonnull Key.Evaluated argvals) {
        return arguments.evaluateMessage(rec, message);
    }

    @Override
    public boolean createsDuplicates() {
        return arguments.createsDuplicates();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        throw new IllegalStateException("Should not be used in a plan");
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        throw new IllegalStateException("Should not be used in a query");
    }

    /**
     * The {@code lucene_field_name} key function.
     * Takes two arguments, a child key expression and a name and overrides the field naming of that child (and its descendants) to be the specified name.
     * Common cases of the name expression would be a literal string or null or another field whose value determines the name at index time.
     */
    public static class LuceneFieldName extends LuceneFunctionKeyExpression {
        public LuceneFieldName(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Override
        public int getMinArguments() {
            return 2;
        }

        @Override
        public int getMaxArguments() {
            return Integer.MAX_VALUE;
        }

        @Override
        public int getColumnSize() {
            return getNamedExpression().getColumnSize();
        }

        @Override
        public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
            final List<Descriptors.FieldDescriptor> result = super.validate(descriptor);
            if (!(arguments instanceof ThenKeyExpression && ((ThenKeyExpression)arguments).getChildren().size() == 2 && ((ThenKeyExpression)arguments).getChildren().get(1).getColumnSize() == 1)) {
                throw new InvalidExpressionException("Lucene field name subexpression should be single column");
            }
            return result;
        }

        /**
         * Get the expression for which a name is given.
         * @return the expression that is named
         */
        @Nonnull
        public KeyExpression getNamedExpression() {
            return ((KeyExpressionWithChildren)arguments).getChildren().get(0);
        }

        /**
         * Get the expression to determine the name.
         * @return the expression to be evaluated to produce the name
         */
        @Nonnull
        public KeyExpression getNameExpression() {
            return ((KeyExpressionWithChildren)arguments).getChildren().get(1);
        }
    }

    /**
     * The {@code lucene_stored} key function.
     * The field argument to this function is additionally stored in the Lucene documents and not just indexed.
     */
    public static class LuceneStored extends LuceneFunctionKeyExpression {
        public LuceneStored(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Override
        public int getMinArguments() {
            return 1;
        }

        @Override
        public int getMaxArguments() {
            return 1;
        }

        @Override
        public int getColumnSize() {
            return getStoredExpression().getColumnSize();
        }

        /**
         * Get the expression that is marked as stored.
         * @return the stored expression
         */
        @Nonnull
        public KeyExpression getStoredExpression() {
            return arguments;
        }
    }

    /**
     * The {@code lucene_sorted} key function.
     * The field argument to this function is additionally sorted in the Lucene index.
     */
    public static class LuceneSorted extends LuceneFunctionKeyExpression {
        public LuceneSorted(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Override
        public int getMinArguments() {
            return 1;
        }

        @Override
        public int getMaxArguments() {
            return 1;
        }

        @Override
        public int getColumnSize() {
            return getSortedExpression().getColumnSize();
        }

        /**
         * Get the expression that is marked as stored.
         * @return the stored expression
         */
        @Nonnull
        public KeyExpression getSortedExpression() {
            return arguments;
        }
    }

    /**
     * The {@code lucent_text} key function.
     * The field arguments to this function are tokenized as full text when building the index.
     */
    public static class LuceneText extends LuceneFunctionKeyExpression {
        public LuceneText(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Override
        public int getMinArguments() {
            return 1;
        }

        @Override
        public int getMaxArguments() {
            return Integer.MAX_VALUE;
        }

        @Override
        public int getColumnSize() {
            return getFieldExpression().getColumnSize();
        }

        /**
         * Get the record field that is tokenized as text using Lucene.
         * @return the field expression
         */
        @Nonnull
        public KeyExpression getFieldExpression() {
            if (arguments.getColumnSize() > 1) {
                return ((KeyExpressionWithChildren)arguments).getChildren().get(0);
            } else {
                return arguments;
            }
        }

        @Nonnull
        public Map<String, Object> getFieldConfigs() {
            Map<String, Object> configs = new HashMap<>();
            if (arguments instanceof ThenKeyExpression) {
                for (KeyExpression child : ((ThenKeyExpression) arguments).getChildren()) {
                    if (child instanceof LuceneFieldConfig) {
                        LuceneFieldConfig fieldConfig = (LuceneFieldConfig) child;
                        if (!(fieldConfig.arguments instanceof LiteralKeyExpression)) {
                            throw new InvalidExpressionException("Lucene field config should have value arguments");
                        }
                        configs.put(fieldConfig.getName(), ((LiteralKeyExpression) fieldConfig.arguments).getValue());
                    }
                }
            }
            return configs;
        }
    }

    /**
     * The key function for Lucene field configuration.
     * The value arguments to this function are applied as the configs for the corresponding Lucene text field when building the index.
     */
    public static class LuceneFieldConfig extends LuceneFunctionKeyExpression {
        public LuceneFieldConfig(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Override
        public int getMinArguments() {
            return 1;
        }

        @Override
        public int getMaxArguments() {
            return Integer.MAX_VALUE;
        }

        @Override
        public int getColumnSize() {
            return 1;
        }
    }

    /**
     * Key function representing one of the Lucene built-in sorting techniques.
     */
    public static class LuceneSortBy extends LuceneFunctionKeyExpression {
        public LuceneSortBy(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Override
        public int getMinArguments() {
            return 0;
        }

        @Override
        public int getMaxArguments() {
            return 0;
        }

        @Override
        public int getColumnSize() {
            return 1;
        }

        @Nonnull
        @Override
        public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable final FDBRecord<M> rec, @Nullable final Message message, @Nonnull final Key.Evaluated argvals) {
            Key.Evaluated result;
            if (rec instanceof FDBQueriedRecord && ((FDBQueriedRecord<M>)rec).getIndexEntry() instanceof LuceneRecordCursor.ScoreDocIndexEntry) {
                final ScoreDoc scoreDoc = ((LuceneRecordCursor.ScoreDocIndexEntry)((FDBQueriedRecord<M>)rec).getIndexEntry()).getScoreDoc();
                final Object value;
                if (isRelevance()) {
                    value = scoreDoc.score;
                } else {
                    value = scoreDoc.doc;
                }
                result = Key.Evaluated.scalar(value);
            } else {
                result = Key.Evaluated.NULL;
            }
            return Collections.singletonList(result);
        }

        public boolean isRelevance() {
            return LUCENE_SORT_BY_RELEVANCE.equals(getName());
        }
    }

}
