/*
 * OrderedLiteral.java
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

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.continuation.TypedQueryArgument;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class OrderedLiteral {
    static final Comparator<OrderedLiteral> COMPARATOR = (o1, o2) -> {
        if (!o1.isScoped() && !o2.isScoped()) {
            return Integer.compare(o1.getTokenIndex(), o2.getTokenIndex());
        }
        if (!o1.isScoped() || !o2.isScoped()) {
            return o1.isScoped() ? 1 : -1;
        }
        final var o1Scope = Assert.optionalUnchecked(o1.getScopeMaybe());
        final var o2Scope = Assert.optionalUnchecked(o2.getScopeMaybe());
        final var scopeComparisonResult = o1Scope.compareTo(o2Scope);
        if (scopeComparisonResult != 0) {
            return scopeComparisonResult;
        }
        return Integer.compare(o1.getTokenIndex(), o2.getTokenIndex());
    };

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

    /**
     * The scope that the literal is defined within. It can be empty.
     */
    @Nonnull
    private final Optional<String> scope;

    /**
     * Whether this literal carries no value at all, as opposed to carrying the value {@code NULL}. A value-free
     * literal reserves its constant id and declares its {@link #type}, but contributes no binding to the evaluation
     * context (see {@link Literals#asMap()}), which is what leaves the constant id unbound. This is how a typed
     * parameter is planned when no value is known yet, e.g. a stored query signature parameter at warm-up.
     */
    private final boolean valueFree;

    OrderedLiteral(@Nonnull final Type type, @Nullable final Object literalObject,
                   @Nullable final Integer unnamedParameterIndex, @Nullable final String parameterName,
                   final int tokenIndex, @Nonnull Optional<String> scope) {
        this(type, literalObject, unnamedParameterIndex, parameterName, tokenIndex, scope, false);
    }

    private OrderedLiteral(@Nonnull final Type type, @Nullable final Object literalObject,
                           @Nullable final Integer unnamedParameterIndex, @Nullable final String parameterName,
                           final int tokenIndex, @Nonnull Optional<String> scope, final boolean valueFree) {
        Verify.verify(unnamedParameterIndex == null || parameterName == null);
        Verify.verify(!valueFree || literalObject == null);
        this.type = type;
        this.literalObject = literalObject;
        this.unnamedParameterIndex = unnamedParameterIndex;
        this.parameterName = parameterName;
        this.tokenIndex = tokenIndex;
        this.scope = scope;
        this.valueFree = valueFree;
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

    int getTokenIndex() {
        return tokenIndex;
    }

    public boolean isScoped() {
        return scope.isPresent();
    }

    @Nonnull
    public Optional<String> getScopeMaybe() {
        return scope;
    }

    @Nonnull
    public String getConstantId() {
        return constantId(tokenIndex, getScopeMaybe());
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

    /**
     * Returns whether this literal carries no value, as opposed to carrying the value {@code NULL}.
     * @return {@code true} if this literal is value-free.
     */
    public boolean isValueFree() {
        return valueFree;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OrderedLiteral)) {
            return false;
        }
        final OrderedLiteral that = (OrderedLiteral)o;
        return scope.equals(that.scope) && tokenIndex == that.tokenIndex;
    }

    @Override
    public int hashCode() {
        if (scope.isPresent()) {
            return Objects.hash(scope, tokenIndex);
        }
        return Objects.hash(tokenIndex);
    }

    /**
     * Returns whether this literal is the identical literal to {@code other}, i.e. the same constant id filled the
     * same way. This is <em>not</em> a comparison of values for the purpose of deduplication — that is keyed on the
     * literal object itself, see {@link Literals.Builder#getFirstValueDuplicateMaybe}. It is used when importing a
     * literal table into another one, to assert that a literal landing on an already-occupied constant id is the one
     * already there and can therefore be skipped. Two value-free literals at the same constant id are identical;
     * a value-free literal and one bound to {@code NULL} are not, even though both have a null literal object.
     *
     * @param other the literal to compare against
     * @return {@code true} if the two are the identical literal
     */
    public boolean deepEquals(@Nonnull final OrderedLiteral other) {
        return this.equals(other)
                && Objects.equals(parameterName, other.parameterName)
                && Objects.equals(unnamedParameterIndex, other.unnamedParameterIndex)
                && valueFree == other.valueFree
                && Objects.equals(literalObject, other.literalObject);
    }

    @Override
    public String toString() {
        if (valueFree) {
            // No value to show, so show the declared type in its place, e.g. "?param_a:{LONG}@c12".
            return "?" + parameterName + ":{" + type + "}@" + scope.orElse("") + tokenIndex;
        }
        return parameterName != null ?
               "?" + parameterName :
               (unnamedParameterIndex != null ?
                "?" + unnamedParameterIndex : "∅") + ":" +
                       literalObject + "@" + scope.orElse("") + tokenIndex;
    }

    @Nonnull
    TypedQueryArgument toProto(@Nonnull final PlanSerializationContext serializationContext, int literalTableIndex) {
        // A value-free literal cannot be serialized: the wire format encodes a missing value as an unset (or empty)
        // LiteralObject, which fromProto() reconstructs as a literal bound to NULL. Reaching here means a warm-up
        // context leaked into execution, since only warm-up produces value-free literals and it never continues a query.
        Assert.thatUnchecked(!isValueFree(), ErrorCode.INTERNAL_ERROR, "attempt to serialize a value-free literal");
        final var type = getType();
        final var argumentBuilder = TypedQueryArgument.newBuilder()
                .setType(type.toTypeProto(serializationContext))
                .setLiteralsTableIndex(literalTableIndex)
                .setTokenIndex(getTokenIndex());
        scope.ifPresent(argumentBuilder::setScope);
        argumentBuilder.setObject(LiteralsUtils.objectToLiteralObjectProto(type, getLiteralObject()));
        if (!isQueryLiteral()) {
            // actual parameter
            Verify.verify(isNamedParameter() || isUnnamedParameter());
            if (isNamedParameter()) {
                argumentBuilder.setParameterName(Objects.requireNonNull(getParameterName()));
            } else {
                argumentBuilder.setUnnamedParameterIndex(Objects.requireNonNull(getUnnamedParameterIndex()));
            }
        }
        return argumentBuilder.build();
    }

    @Nonnull
    private static OrderedLiteral forQueryLiteral(@Nonnull final Type type, @Nullable final Object literalObject, final int tokenIndex,
                                                  @Nonnull final Optional<String> scope) {
        return new OrderedLiteral(type, literalObject, null, null, tokenIndex, scope);
    }

    @Nonnull
    private static OrderedLiteral forUnnamedParameter(@Nonnull final Type type, @Nullable final Object literalObject,
                                                      final int unnamedParameterIndex, final int tokenIndex,
                                                      @Nonnull final Optional<String> scope) {
        return new OrderedLiteral(type, literalObject, unnamedParameterIndex, null, tokenIndex, scope);
    }

    @Nonnull
    private static OrderedLiteral forNamedParameter(@Nonnull final Type type, @Nullable final Object literalObject,
                                                    @Nonnull final String parameterName, final int tokenIndex,
                                                    @Nonnull final Optional<String> scope) {
        return new OrderedLiteral(type, literalObject, null, parameterName, tokenIndex, scope);
    }

    /**
     * Creates a value-free literal for a named parameter: it reserves the constant id and declares the type, but
     * carries no value and therefore contributes no binding. Only named parameters can be value-free today, since the
     * sole producer is a stored query signature parameter warmed with no value.
     *
     * @param type the declared type of the parameter
     * @param parameterName the name of the parameter
     * @param tokenIndex the token position of the parameter in the query
     * @param scope the scope the literal is defined within
     * @return a value-free {@link OrderedLiteral}
     */
    @Nonnull
    static OrderedLiteral forValueFreeNamedParameter(@Nonnull final Type type, @Nonnull final String parameterName,
                                                     final int tokenIndex, @Nonnull final Optional<String> scope) {
        return new OrderedLiteral(type, null, null, parameterName, tokenIndex, scope, true);
    }

    @Nonnull
    public static OrderedLiteral fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                           @Nonnull final TypeRepository typeRepository,
                                           @Nonnull final TypedQueryArgument argumentProto) {
        final var argumentType = Type.fromTypeProto(serializationContext, argumentProto.getType());
        final Optional<String> scopeMaybe = argumentProto.hasScope() ? Optional.of(argumentProto.getScope()) : Optional.empty();
        if (argumentProto.hasUnnamedParameterIndex()) {
            return OrderedLiteral.forUnnamedParameter(argumentType,
                    LiteralsUtils.objectFromLiteralObjectProto(typeRepository, argumentType, argumentProto.getObject()),
                    argumentProto.getUnnamedParameterIndex(), argumentProto.getTokenIndex(), scopeMaybe);
        } else if (argumentProto.hasParameterName()) {
            return OrderedLiteral.forNamedParameter(argumentType,
                    LiteralsUtils.objectFromLiteralObjectProto(typeRepository, argumentType, argumentProto.getObject()),
                    argumentProto.getParameterName(), argumentProto.getTokenIndex(), scopeMaybe);
        } else {
            return OrderedLiteral.forQueryLiteral(argumentType,
                    LiteralsUtils.objectFromLiteralObjectProto(typeRepository, argumentType, argumentProto.getObject()),
                    argumentProto.getTokenIndex(), scopeMaybe);
        }
    }

    @Nonnull
    public static String constantId(final int tokenIndex, @Nonnull final Optional<String> scope) {
        return "c" + scope.orElse("") + tokenIndex;
    }

    @Nonnull
    @VisibleForTesting
    public static String constantId(final int tokenIndex) {
        return constantId(tokenIndex, Optional.empty());
    }
}
