/*
 * Literals.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.function.Supplier;

public class Literals {

    @Nonnull
    private static final Literals EMPTY = new Literals(ImmutableList.of());

    @Nonnull
    private final List<OrderedLiteral> orderedLiterals;

    @Nonnull
    private final Supplier<Map<String, Object>> asMapSupplier;

    @Nonnull
    private final Supplier<Set<String>> valueFreeConstantIdsSupplier;

    private Literals(@Nonnull final List<OrderedLiteral> orderedLiterals) {
        this.orderedLiterals = ImmutableList.copyOf(orderedLiterals);
        // Using unmodifiableMap because it allows null values, which are valid here
        // and represent either null constants or null prepared parameters in queries.
        // Value-free literals are excluded: a present key with a null value means "bound to NULL", whereas an absent
        // key means "no value at all", which is what leaves the constant id unbound in the evaluation context.
        this.asMapSupplier = Suppliers.memoize(() ->
                Collections.unmodifiableMap(
                        this.orderedLiterals
                                .stream()
                                .filter(literal -> !literal.isValueFree())
                                .collect(LinkedHashMap::new, (m, v) -> m.put(v.getConstantId(), v.getLiteralObject()), LinkedHashMap::putAll)));
        this.valueFreeConstantIdsSupplier = Suppliers.memoize(() -> this.orderedLiterals.stream()
                .filter(OrderedLiteral::isValueFree)
                .map(OrderedLiteral::getConstantId)
                .collect(ImmutableSet.toImmutableSet()));
    }

    @Nonnull
    public List<OrderedLiteral> getOrderedLiterals() {
        return orderedLiterals;
    }

    public Map<String, Object> asMap() {
        return asMapSupplier.get();
    }

    /**
     * Returns whether the literal at the given constant id is value-free, i.e. declares a type but carries no value.
     * Note that this is different from a literal bound to {@code NULL}, which does contribute a binding.
     *
     * @param constantId the constant id to look up
     * @return {@code true} if a value-free literal is registered at {@code constantId}
     */
    public boolean isValueFree(@Nonnull final String constantId) {
        return valueFreeConstantIdsSupplier.get().contains(constantId);
    }

    /**
     * Returns these literals as constant bindings in an {@link EvaluationContext}. Note that this binds the set of
     * <em>values</em>, not the set of literals: a value-free literal declares a type but carries no value, so a
     * non-empty literal table can still bind nothing.
     *
     * @param typeRepository the type repository to build the context with
     * @return an {@link EvaluationContext} binding the values of these literals
     */
    @Nonnull
    public EvaluationContext toEvaluationContext(@Nonnull final TypeRepository typeRepository) {
        final var constantBindings = asMap();
        if (constantBindings.isEmpty()) {
            return EvaluationContext.forTypeRepository(typeRepository);
        }
        return EvaluationContext.newBuilder()
                .setConstant(Quantifier.constant(), constantBindings)
                .build(typeRepository);
    }

    @Nonnull
    public static Literals empty() {
        return EMPTY;
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static final class Builder {

        @Nonnull
        private final Multiset<OrderedLiteral> literals;

        @Nonnull
        private final Stack<Multiset<OrderedLiteral>> current;

        @Nonnull
        private final Map<Object, OrderedLiteral> literalReverseLookup; // allowing null keys.

        @Nonnull
        private Optional<String> scope;

        private int arrayLiteralScopeCount;
        private int structLiteralScopeCount;

        private Builder() {
            this.literals = TreeMultiset.create(OrderedLiteral.COMPARATOR);
            current = new Stack<>();
            current.push(this.literals);
            literalReverseLookup = new HashMap<>();
            scope = Optional.empty();
        }

        private void startComplexLiteral() {
            current.push(TreeMultiset.create(OrderedLiteral.COMPARATOR));
        }

        /**
         * Sets a scope for all newly created literals, i.e. all newly added literals will have their id
         * comprising the scope in addition to their token index.
         * @param scope the scope.
         * @return {@code this} builder.
         */
        @Nonnull
        public Builder setScope(@Nonnull final String scope) {
            this.scope = Optional.of(scope);
            return this;
        }

        public List<OrderedLiteral> importLiteralsRetrieveNewLiterals(@Nonnull final Literals other) {
            return other.getOrderedLiterals().stream().filter(literal -> addLiteral(literal, false))
                    .collect(ImmutableList.toImmutableList());
        }

        public void importLiterals(@Nonnull final Literals other) {
            other.getOrderedLiterals().forEach(literalToImport -> addLiteral(literalToImport, false));
        }

        public void addLiteral(@Nonnull final OrderedLiteral orderedLiteral) {
            addLiteral(orderedLiteral, true);
        }

        private boolean addLiteral(@Nonnull final OrderedLiteral orderedLiteral, boolean verifyNotExists) {
            final var currentLiterals = current.peek();
            if (orderedLiteral.getLiteralObject() instanceof byte[]) {
                // it is not clear why there is a special code path for array literal that avoids the
                // assertion below that makes sure each token index appears at most once in the literals map.
                final var byteOrderedLiteral = new OrderedLiteral(orderedLiteral.getType(),
                        ByteString.copyFrom(((byte[])orderedLiteral.getLiteralObject())),
                        orderedLiteral.getUnnamedParameterIndex(), orderedLiteral.getParameterName(),
                        orderedLiteral.getTokenIndex(), orderedLiteral.getScopeMaybe());
                literalReverseLookup.putIfAbsent(byteOrderedLiteral.getLiteralObject(), byteOrderedLiteral);
                currentLiterals.add(byteOrderedLiteral);
            } else {
                if (verifyNotExists) {
                    Assert.thatUnchecked(!currentLiterals.contains(orderedLiteral));
                } else {
                    if (currentLiterals.contains(orderedLiteral)) {
                        // The constant id is already taken. Importing is only a no-op if it is taken by the identical
                        // literal; anything else means two different literals claim one constant id. Note that a
                        // value-free literal and one bound to NULL both have a null object, so they are distinguished
                        // here only by deepEquals also comparing the value-free flag.
                        final var duplicate = currentLiterals.elementSet().stream()
                                .filter(element -> element.equals(orderedLiteral)).findFirst().orElseThrow();
                        Assert.thatUnchecked(orderedLiteral.deepEquals(duplicate), ErrorCode.INTERNAL_ERROR,
                                "conflicting literals for the same constant id");
                        return false;
                    }
                }
                if (!orderedLiteral.isValueFree()) {
                    // A value-free literal must not be registered for reverse lookup: it would claim the null key and
                    // become the dedup target for a subsequent literal genuinely bound to NULL.
                    literalReverseLookup.putIfAbsent(orderedLiteral.getLiteralObject(), orderedLiteral);
                }
                currentLiterals.add(orderedLiteral);
            }
            return true;
        }

        @Nonnull
        public OrderedLiteral addLiteral(@Nonnull final Type type, @Nullable final Object literalObject,
                                         @Nullable final Integer unnamedParameterIndex, @Nullable final String parameterName,
                                         final int tokenIndex) {
            final var literal = new OrderedLiteral(type, literalObject, unnamedParameterIndex, parameterName, tokenIndex, scope);
            addLiteral(literal);
            return literal;
        }

        /**
         * Adds a <em>value-free</em> literal for a named parameter, i.e. one that reserves its constant id and declares
         * its type but carries no value, and therefore contributes no binding to the evaluation context.
         *
         * @param type the declared type of the parameter
         * @param parameterName the name of the parameter
         * @param tokenIndex the token position of the parameter in the query
         * @return the newly added value-free {@link OrderedLiteral}
         */
        @Nonnull
        public OrderedLiteral addValueFreeLiteral(@Nonnull final Type type, @Nonnull final String parameterName,
                                                 final int tokenIndex) {
            final var literal = OrderedLiteral.forValueFreeNamedParameter(type, parameterName, tokenIndex, scope);
            addLiteral(literal);
            return literal;
        }

        public void startArrayLiteral() {
            arrayLiteralScopeCount++;
            startComplexLiteral();
        }

        public void startStructLiteral() {
            structLiteralScopeCount++;
            startComplexLiteral();
        }


        @Nonnull
        public Optional<OrderedLiteral> getFirstValueDuplicateMaybe(@Nullable Object value) {
            // if this is called while building a complex literal, bail out, since we do not support
            // non-linear reference graphs at the moment.
            if (current.size() > 1) {
                return Optional.empty();
            }
            // ofNullable rather than of: a value-free literal is not registered for reverse lookup, so there may be no
            // entry for the value at all.
            return Optional.ofNullable(literalReverseLookup.get(value));
        }

        @Nonnull
        public Optional<OrderedLiteral> getFirstDuplicateOfConstantIdMaybe(@Nonnull String constantId) {
            // if this is called while building a complex literal, bail out, since we do not support
            // non-linear reference graphs at the moment.
            if (current.size() > 1) {
                return Optional.empty();
            }
            final var literal = literals.stream().filter(l ->
                    l.getConstantId().equals(constantId)).findFirst().orElseThrow();
            if (literal.isValueFree()) {
                // A value-free literal has no value, so there is nothing to deduplicate against.
                return Optional.empty();
            }
            return Optional.ofNullable(literalReverseLookup.get(literal.getLiteralObject()));
        }

        public void finishArrayLiteral(@Nullable final Integer unnamedParameterIndex,
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
                    Assert.thatUnchecked(currentElementType.equals(elementType), ErrorCode.DATATYPE_MISMATCH,
                            "Elements of array literal are not of identical type!");
                } else {
                    elementType = currentElementType;
                }
                arrayElements.add(nestedArrayLiteral.getLiteralObject());
            }
            final var orderedLiteral = new OrderedLiteral(new Type.Array(elementType), arrayElements, unnamedParameterIndex,
                    parameterName, tokenIndex, scope);
            if (shouldProcessLiterals) {
                Verify.verify(!current.peek().contains(orderedLiteral));
                literalReverseLookup.putIfAbsent(orderedLiteral.getLiteralObject(), orderedLiteral);
                current.peek().add(orderedLiteral);
            }
            arrayLiteralScopeCount--;
        }

        public void finishStructLiteral(@Nonnull final Type.Record type,
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
                    messageBuilder.setField(fieldDescriptors.get(i), transformFieldValue(fieldValue));
                }
                i++;
            }
            Verify.verify(tokenIndex >= 0);
            final var orderedLiteral = new OrderedLiteral(type, messageBuilder.build(), unnamedParameterIndex,
                    parameterName, tokenIndex, scope);
            literalReverseLookup.putIfAbsent(orderedLiteral.getLiteralObject(), orderedLiteral);
            current.peek().add(orderedLiteral);
            structLiteralScopeCount--;
        }

        private static Object transformFieldValue(@Nonnull Object fieldValue) {
            if (fieldValue instanceof UUID) {
                return TupleFieldsHelper.toProto((UUID) fieldValue);
            }
            return fieldValue;
        }

        public boolean isAddingComplexLiteral() {
            return arrayLiteralScopeCount + structLiteralScopeCount != 0;
        }

        public boolean isEmpty() {
            return literals.isEmpty();
        }

        @Nonnull
        public Literals build() {
            return new Literals(literals.stream().collect(ImmutableList.toImmutableList()));
        }

        @Nonnull
        public String constructConstantId(int tokenIndex) {
            return "c" + scope.orElse("") + tokenIndex;
        }
    }
}
