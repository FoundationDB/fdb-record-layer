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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.function.Supplier;

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

    @Nonnull
    Literals getLiterals();

    boolean isForExplain(); // todo (yhatem) remove.

    @Nonnull
    PlanHashable.PlanHashMode getPlanHashMode();

    class Literals {
        @Nonnull
        private final List<OrderedLiteral> orderedLiterals;

        @Nonnull
        private final Supplier<Map<String, Object>> asMapSupplier;

        public Literals(@Nonnull final List<OrderedLiteral> orderedLiterals) {
            this.orderedLiterals = ImmutableList.copyOf(orderedLiterals);
            this.asMapSupplier = Suppliers.memoize(() ->
                    this.orderedLiterals
                            .stream()
                            .filter(orderedLiteral -> orderedLiteral.getLiteralObject() != null)
                            .collect(ImmutableMap.toImmutableMap(OrderedLiteral::getConstantId,
                                    OrderedLiteral::getLiteralObject)));
        }

        @Nonnull
        public List<OrderedLiteral> getOrderedLiterals() {
            return orderedLiterals;
        }

        public boolean isEmpty() {
            return orderedLiterals.isEmpty();
        }

        public Map<String, Object> asMap() {
            return asMapSupplier.get();
        }
    }

    class LiteralsBuilder {
        @Nonnull
        private final Multiset<OrderedLiteral> literals;

        @Nonnull
        private final Stack<Multiset<OrderedLiteral>> current;

        private int arrayLiteralScopeCount;
        private int structLiteralScopeCount;

        private LiteralsBuilder() {
            this.literals = TreeMultiset.create(OrderedLiteral.COMPARATOR);
            current = new Stack<>();
            current.push(this.literals);
        }

        void addLiteral(@Nonnull final OrderedLiteral orderedLiteral) {
            if (orderedLiteral.getLiteralObject() instanceof byte[]) {
                current.peek()
                        .add(new OrderedLiteral(orderedLiteral.getType(),
                                ByteString.copyFrom(((byte[]) orderedLiteral.getLiteralObject())),
                                orderedLiteral.getUnnamedParameterIndex(), orderedLiteral.getParameterName(),
                                orderedLiteral.getTokenIndex()));
            } else {
                Verify.verify(!current.peek().contains(orderedLiteral));
                current.peek().add(orderedLiteral);
            }
        }

        void startArrayLiteral() {
            arrayLiteralScopeCount++;
            startComplexLiteral();
        }

        void startStructLiteral() {
            structLiteralScopeCount++;
            startComplexLiteral();
        }

        private void startComplexLiteral() {
            current.push(TreeMultiset.create(OrderedLiteral.COMPARATOR));
        }

        void finishArrayLiteral(@Nullable final Integer unnamedParameterIndex,
                                @Nullable final String parameterName,
                                final boolean shouldProcessLiterals,
                                final int tokenIndex) {
            Assert.thatUnchecked(!current.empty());
            Assert.thatUnchecked(arrayLiteralScopeCount > 0);
            final var nestedArrayLiterals = current.pop();
            // coalesce the array elements
            final var arrayElements = Lists.newArrayListWithExpectedSize(nestedArrayLiterals.size());
            Type elementType = null;
            for (final OrderedLiteral nestedArrayLiteral : nestedArrayLiterals) {
                final var currentElementType = nestedArrayLiteral.getType();
                if (elementType != null) {
                    Verify.verify(currentElementType.equals(elementType));
                } else {
                    elementType = currentElementType;
                }
                arrayElements.add(nestedArrayLiteral.getLiteralObject());
            }
            final var orderedLiteral = new OrderedLiteral(new Type.Array(elementType), arrayElements, unnamedParameterIndex,
                    parameterName, tokenIndex);
            if (shouldProcessLiterals) {
                Verify.verify(!current.peek().contains(orderedLiteral));
                current.peek().add(orderedLiteral);
            }
            arrayLiteralScopeCount--;
        }

        void finishStructLiteral(@Nonnull final Type.Record type,
                                 @Nullable final Integer unnamedParameterIndex,
                                 @Nullable final String parameterName,
                                 final int tokenIndex) {
            Assert.thatUnchecked(!current.empty());
            Assert.thatUnchecked(structLiteralScopeCount > 0);
            final var fields = current.pop();
            // TODO: Consider creating the TypeRepository in the beginning and reusing throughout the lifetime of the builder.
            final var builder = TypeRepository.newBuilder();
            type.defineProtoType(builder);
            final var messageBuilder = Objects.requireNonNull(builder.build().newMessageBuilder(type));
            final var fieldDescriptors = messageBuilder.getDescriptorForType().getFields();
            int i = 0;
            for (final OrderedLiteral fieldOrderedLiteral : fields) {
                final var fieldValue = fieldOrderedLiteral.getLiteralObject();
                if (fieldValue != null) {
                    messageBuilder.setField(fieldDescriptors.get(i), fieldValue);
                }
                i++;
            }
            Verify.verify(tokenIndex >= 0);

            current.peek().add(new OrderedLiteral(type, messageBuilder.build(), unnamedParameterIndex, parameterName,
                    tokenIndex));
            structLiteralScopeCount--;
        }

        boolean isAddingComplexLiteral() {
            return arrayLiteralScopeCount + structLiteralScopeCount != 0;
        }

        @Nonnull
        public Literals build() {
            return new Literals(literals.stream().collect(ImmutableList.toImmutableList()));
        }

        public boolean isEmpty() {
            return literals.isEmpty();
        }

        @Nonnull
        public static LiteralsBuilder newBuilder() {
            return new LiteralsBuilder();
        }
    }

    class OrderedLiteral {
        private static final Comparator<OrderedLiteral> COMPARATOR = Comparator.comparing(OrderedLiteral::getTokenIndex);

        @Nonnull
        private final Type type;
        @Nullable
        private final Object literalObject;

        @Nullable
        private final Integer unnamedParameterIndex;
        @Nullable
        private final String parameterName;

        /**
         * Token position of literal in query.
         */
        private final int tokenIndex;

        public OrderedLiteral(@Nonnull final Type type, @Nullable final Object literalObject,
                              @Nullable final Integer unnamedParameterIndex, @Nullable final String parameterName,
                              final int tokenIndex) {
            Verify.verify(unnamedParameterIndex == null || parameterName == null);
            this.type = type;
            this.literalObject = literalObject;
            this.unnamedParameterIndex = unnamedParameterIndex;
            this.parameterName = parameterName;
            this.tokenIndex = tokenIndex;
        }

        @Nonnull
        public Type getType() {
            return type;
        }

        @Nullable
        public Object getLiteralObject() {
            return literalObject;
        }

        @Nullable
        public Integer getUnnamedParameterIndex() {
            return unnamedParameterIndex;
        }

        @Nullable
        public String getParameterName() {
            return parameterName;
        }

        public int getTokenIndex() {
            return tokenIndex;
        }

        @Nonnull
        public String getConstantId() {
            return constantId(tokenIndex);
        }

        public boolean isQueryLiteral() {
            return unnamedParameterIndex == null && parameterName == null;
        }

        public boolean isUnnamedParameter() {
            return unnamedParameterIndex != null;
        }

        public boolean isNamedParameter() {
            return parameterName != null;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof OrderedLiteral)) {
                return false;
            }
            final OrderedLiteral that = (OrderedLiteral) o;
            return tokenIndex == that.tokenIndex;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tokenIndex);
        }

        @Override
        public String toString() {
            return parameterName != null
                   ? "?" + parameterName
                   : (unnamedParameterIndex != null
                      ? "?" + unnamedParameterIndex : "∅") + ":" +
                     literalObject + "@" + tokenIndex;
        }

        @Nonnull
        public static OrderedLiteral forQueryLiteral(@Nonnull final Type type, @Nullable final Object literalObject, final int tokenIndex) {
            return new OrderedLiteral(type, literalObject, null, null, tokenIndex);
        }

        @Nonnull
        public static OrderedLiteral forUnnamedParameter(@Nonnull final Type type, @Nullable final Object literalObject,
                                                         final int unnamedParameterIndex, final int tokenIndex) {
            return new OrderedLiteral(type, literalObject, unnamedParameterIndex, null, tokenIndex);
        }

        @Nonnull
        public static OrderedLiteral forNamedParameter(@Nonnull final Type type, @Nullable final Object literalObject,
                                                       @Nonnull final String parameterName, final int tokenIndex) {
            return new OrderedLiteral(type, literalObject, null, parameterName, tokenIndex);
        }

        @Nonnull
        public static String constantId(final int tokenIndex) {
            return "c" + tokenIndex;
        }
    }
}
