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
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

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
    public int planHash(@Nonnull final PlanHashKind hashKind) {
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
    }

}
