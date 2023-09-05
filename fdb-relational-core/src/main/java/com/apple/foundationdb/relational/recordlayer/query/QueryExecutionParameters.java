/*
 * QueryExecutionParameters.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

public interface QueryExecutionParameters {

    @Nonnull
    EvaluationContext getEvaluationContext(@Nonnull final TypeRepository typeRepository);

    default EvaluationContext getEvaluationContext() {
        return getEvaluationContext(ParserUtils.EMPTY_TYPE_REPOSITORY);
    }

    @Nonnull
    ExecuteProperties.Builder getExecutionPropertiesBuilder();

    @Nullable
    byte[] getContinuation();

    int getParameterHash();

    @Nonnull
    PreparedStatementParameters getPreparedStatementParameters();

    boolean isForExplain(); // todo (yhatem) remove.

    class LiteralsBuilder {

        @Nonnull
        private final List<Object> literals;

        @Nonnull
        private final Stack<List<Object>> current;

        private int arrayLiteralScopeCount;
        private int structLiteralScopeCount;

        private LiteralsBuilder() {
            this.literals = new LinkedList<>();
            current = new Stack<>();
            current.push(this.literals);
        }

        int addLiteral(@Nullable Object literal) {
            current.peek().add(literal);
            return current.peek().size() - 1;
        }

        int startArrayLiteral() {
            arrayLiteralScopeCount++;
            return startComplexLiteral();
        }

        int startStructLiteral() {
            structLiteralScopeCount++;
            return startComplexLiteral();
        }

        private int startComplexLiteral() {
            final var index = current.peek().size();
            current.push(new LinkedList<>());
            return index;
        }

        void finishArrayLiteral() {
            Assert.thatUnchecked(!current.empty());
            Assert.thatUnchecked(arrayLiteralScopeCount > 0);
            final var array = current.pop();
            current.peek().add(array);
            arrayLiteralScopeCount--;
        }

        void finishStructLiteral(@Nullable Type.Record type) {
            Assert.thatUnchecked(!current.empty());
            Assert.thatUnchecked(structLiteralScopeCount > 0);
            final var array = current.pop();
            if (type != null) {
                // TODO: Consider creating the TypeRepository in the beginning and reusing throughout the lifetime of the builder.
                final var builder = TypeRepository.newBuilder();
                type.defineProtoType(builder);
                final var messageBuilder = builder.build().newMessageBuilder(type);
                final var fieldDescriptors = messageBuilder.getDescriptorForType().getFields();
                for (int i = 0; i < array.size(); i++) {
                    final var value = array.get(i);
                    messageBuilder.setField(fieldDescriptors.get(i), value);
                }
                current.peek().add(messageBuilder.build());
            } else {
                current.peek().add(array);
            }
            structLiteralScopeCount--;
        }

        boolean isAddingComplexLiteral() {
            return arrayLiteralScopeCount + structLiteralScopeCount != 0;
        }

        @Nonnull
        public List<Object> getLiterals() {
            return literals;
        }

        public boolean isEmpty() {
            return literals.isEmpty();
        }

        @Nonnull
        public static LiteralsBuilder newBuilder() {
            return new LiteralsBuilder();
        }
    }
}
