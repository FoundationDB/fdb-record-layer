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
import com.apple.foundationdb.relational.continuation.TypedQueryArgument;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import org.jspecify.annotations.Nullable;
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
    private final Optional<String> scope;

    OrderedLiteral(final Type type, @Nullable final Object literalObject,
                   @Nullable final Integer unnamedParameterIndex, @Nullable final String parameterName,
                   final int tokenIndex, Optional<String> scope) {
        Verify.verify(unnamedParameterIndex == null || parameterName == null);
        this.type = type;
        this.literalObject = literalObject;
        this.unnamedParameterIndex = unnamedParameterIndex;
        this.parameterName = parameterName;
        this.tokenIndex = tokenIndex;
        this.scope = scope;
    }

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

    public Optional<String> getScopeMaybe() {
        return scope;
    }

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

    public boolean deepEquals(final OrderedLiteral other) {
        return this.equals(other)
                && Objects.equals(parameterName, other.parameterName)
                && Objects.equals(unnamedParameterIndex, other.unnamedParameterIndex)
                && Objects.equals(literalObject, other.literalObject);
    }

    @Override
    public String toString() {
        return parameterName != null ?
               "?" + parameterName :
               (unnamedParameterIndex != null ?
                "?" + unnamedParameterIndex : "∅") + ":" +
                       literalObject + "@" + scope.orElse("") + tokenIndex;
    }

    TypedQueryArgument toProto(final PlanSerializationContext serializationContext, int literalTableIndex) {
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

    private static OrderedLiteral forQueryLiteral(final Type type, @Nullable final Object literalObject, final int tokenIndex,
                                                  final Optional<String> scope) {
        return new OrderedLiteral(type, literalObject, null, null, tokenIndex, scope);
    }

    private static OrderedLiteral forUnnamedParameter(final Type type, @Nullable final Object literalObject,
                                                      final int unnamedParameterIndex, final int tokenIndex,
                                                      final Optional<String> scope) {
        return new OrderedLiteral(type, literalObject, unnamedParameterIndex, null, tokenIndex, scope);
    }

    private static OrderedLiteral forNamedParameter(final Type type, @Nullable final Object literalObject,
                                                    final String parameterName, final int tokenIndex,
                                                    final Optional<String> scope) {
        return new OrderedLiteral(type, literalObject, null, parameterName, tokenIndex, scope);
    }

    public static OrderedLiteral fromProto(final PlanSerializationContext serializationContext,
                                           final TypeRepository typeRepository,
                                           final TypedQueryArgument argumentProto) {
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

    public static String constantId(final int tokenIndex, final Optional<String> scope) {
        return "c" + scope.orElse("") + tokenIndex;
    }

    @VisibleForTesting
    public static String constantId(final int tokenIndex) {
        return constantId(tokenIndex, Optional.empty());
    }
}
