/*
 * ParserContext.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.OfTypeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.apple.foundationdb.relational.api.exceptions.ErrorCode.DATATYPE_MISMATCH;

/**
 * Context keeping state related to plan generation, it also captures execution-specific attributes that influences
 * how the physical plan is handled at runtime, such as continuation, limit, and offset.
 */
public class MutablePlanGenerationContext implements QueryExecutionContext {
    @Nonnull
    private final PreparedParams preparedParams;

    @Nonnull
    private final LiteralsBuilder literalsBuilder;

    private final int parameterHash;

    @Nonnull
    private final PlanHashable.PlanHashMode planHashMode;

    @Nonnull
    private final List<ConstantObjectValue> constantObjectValues;

    private boolean shouldProcessLiteral;

    private boolean forExplain;

    @Nullable
    private byte[] continuation;

    @Nonnull
    private final ImmutableList.Builder<QueryPredicate> equalityConstraints;

    public MutablePlanGenerationContext(@Nonnull PreparedParams preparedParams,
                                        @Nonnull PlanHashable.PlanHashMode planHashMode,
                                        int parameterHash) {
        this.preparedParams = preparedParams;
        this.planHashMode = planHashMode;
        this.parameterHash = parameterHash;
        literalsBuilder = LiteralsBuilder.newBuilder();
        constantObjectValues = new LinkedList<>();
        shouldProcessLiteral = true;
        forExplain = false;
        setContinuation(null);
        equalityConstraints = ImmutableList.builder();
    }

    public void startArrayLiteral() {
        literalsBuilder.startArrayLiteral();
    }

    public void finishArrayLiteral(@Nullable Integer unnamedParameterIndex,
                                   @Nullable String parameterName,
                                   int tokenIndex) {
        literalsBuilder.finishArrayLiteral(unnamedParameterIndex, parameterName, shouldProcessLiteral, tokenIndex);
    }

    public void startStructLiteral() {
        literalsBuilder.startStructLiteral();
    }

    public void finishStructLiteral(@Nonnull Type.Record type,
                                    @Nullable Integer unnamedParameterIndex,
                                    @Nullable String parameterName,
                                    int tokenIndex) {
        literalsBuilder.finishStructLiteral(type, unnamedParameterIndex, parameterName, tokenIndex);
    }

    public void addStrippedLiteralOrParameter(@Nonnull OrderedLiteral orderedLiteral) {
        literalsBuilder.addLiteral(orderedLiteral);
    }

    public void addLiteralReference(@Nonnull ConstantObjectValue constantObjectValue) {
        if (!literalsBuilder.isAddingComplexLiteral()) {
            constantObjectValues.add(constantObjectValue);
        }
    }

    @Nonnull
    public Optional<OrderedLiteral> getFirstCovReference(@Nullable Object literal, int requestedTokenIndex, @Nonnull Type type) {
        final var orderedLiteralMaybe = literalsBuilder.getFirstValueDuplicateMaybe(literal);
        if (orderedLiteralMaybe.isEmpty()) {
            return Optional.empty();
        }
        addEqualityConstraint(requestedTokenIndex, orderedLiteralMaybe.get().getTokenIndex(), type);
        return orderedLiteralMaybe;
    }

    @Nonnull
    public Optional<OrderedLiteral> getFirstDuplicate(@Nonnull String tokenId) {
        final var firstDuplicateMaybe = literalsBuilder.getFirstDuplicateOfTokenIdMaybe(tokenId);
        if (firstDuplicateMaybe.isEmpty()) {
            return firstDuplicateMaybe;
        }
        final var firstDuplicate = firstDuplicateMaybe.get();
        addEqualityConstraint(firstDuplicate.getConstantId(), tokenId, firstDuplicate.getType());
        return firstDuplicateMaybe;
    }

    private void addEqualityConstraint(int leftTokenIndex, int rightTokenIndex, @Nonnull Type type) {
        addEqualityConstraint(OrderedLiteral.constantId(leftTokenIndex), OrderedLiteral.constantId(rightTokenIndex), type);
    }

    private void addEqualityConstraint(@Nonnull String leftTokenId, @Nonnull String rightTokenId, @Nonnull Type type) {
        if (leftTokenId.equals(rightTokenId)) {
            return;
        }
        final var leftCov = ConstantObjectValue.of(Quantifier.constant(), leftTokenId, type);
        final var rightCov = ConstantObjectValue.of(Quantifier.constant(), rightTokenId, type);
        final var equalityPredicate = new ValuePredicate(leftCov, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, rightCov));
        equalityConstraints.add(equalityPredicate);
    }

    @Nonnull
    @Override
    public Literals getLiteralsBuilder() {
        return literalsBuilder.build();
    }

    @Nonnull
    @Override
    public PlanHashable.PlanHashMode getPlanHashMode() {
        return planHashMode;
    }

    // this is temporary until we have a proper clean up.
    public boolean isForDdl() {
        return !shouldProcessLiteral;
    }

    @Nonnull
    @Override
    public EvaluationContext getEvaluationContext(@Nonnull TypeRepository typeRepository) {
        if (literalsBuilder.isEmpty()) {
            return EvaluationContext.forTypeRepository(typeRepository);
        }
        final var builder = EvaluationContext.newBuilder();
        builder.setConstant(Quantifier.constant(), getLiteralsBuilder().asMap());
        return builder.build(typeRepository);
    }

    @Nonnull
    @Override
    public ExecuteProperties.Builder getExecutionPropertiesBuilder() {
        return ExecuteProperties.newBuilder();
    }

    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP", justification = "Intentional")
    @Override
    @Nullable
    public byte[] getContinuation() {
        return continuation;
    }

    @Override
    public int getParameterHash() {
        return parameterHash;
    }

    @Override
    public boolean isForExplain() {
        return forExplain;
    }

    @Nonnull
    public QueryPlanConstraint getLiteralReferencesConstraint() {
        final ImmutableList.Builder<QueryPredicate> predicateBuilder = ImmutableList.builder();
        constantObjectValues.forEach(parameter -> predicateBuilder.add(new ValuePredicate(OfTypeValue.from(parameter),
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true))));
        predicateBuilder.addAll(equalityConstraints.build());
        return QueryPlanConstraint.ofPredicates(predicateBuilder.build());
    }

    public void setForExplain(boolean forExplain) {
        this.forExplain = forExplain;
    }

    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP2", justification = "Intentional")
    public void setContinuation(@Nullable byte[] continuation) {
        this.continuation = continuation;
    }

    public boolean shouldProcessLiteral() {
        return shouldProcessLiteral;
    }

    private void setShouldProcessLiteral(boolean shouldProcessLiteral) {
        this.shouldProcessLiteral = shouldProcessLiteral;
    }

    /**
     * Runs a closure without literal processing, i.e. without translating a literal found in the
     * AST into a {@link ConstantObjectValue}. This is necessary in cases where the literal is found
     * in a context that does not contribute to the logical plan such as {@code limit} and {@code continuation}.
     * @param supplier The closure to run with literal processing disabled.
     * @return The result of the closure
     * @param <T> The type of the closure result.
     */
    @Nonnull
    public <T> T withDisabledLiteralProcessing(@Nonnull Supplier<T> supplier) {
        boolean oldShouldProcessLiteral = shouldProcessLiteral();
        setShouldProcessLiteral(false);
        final T result = supplier.get();
        setShouldProcessLiteral(oldShouldProcessLiteral);
        return result;
    }

    @Nonnull
    public Value processQueryLiteral(@Nonnull Type type, @Nullable Object literal, int tokenIndex) {
        return processQueryLiteralOrParameter(type, literal, null, null, tokenIndex);
    }

    @Nonnull
    public Value processQueryLiteralOrParameter(@Nonnull Type type,
                                                @Nullable Object literal,
                                                @Nullable Integer unnamedParameterIndex,
                                                @Nullable String parameterName,
                                                int tokenIndex) {
        final var literalValue = new LiteralValue<>(literal);
        if (!shouldProcessLiteral()) {
            return literalValue;
        } else {
            final var orderedLiteral = new OrderedLiteral(type, literal, unnamedParameterIndex, parameterName, tokenIndex);
            addStrippedLiteralOrParameter(orderedLiteral);
            final var result = ConstantObjectValue.of(Quantifier.constant(), orderedLiteral.getConstantId(),
                    literalValue.getResultType());
            addLiteralReference(result);
            return ConstantObjectValue.of(Quantifier.constant(), getFirstCovReference(literal, tokenIndex, type).map(OrderedLiteral::getConstantId).orElse(orderedLiteral.getConstantId()),
                    literalValue.getResultType());
        }
    }

    @Nonnull
    public ConstantObjectValue processComplexLiteral(@Nonnull String constantId, @Nonnull Type type) {
        final var result = ConstantObjectValue.of(Quantifier.constant(), constantId, type);
        if (shouldProcessLiteral()) {
            addLiteralReference(result);
        }
        return ConstantObjectValue.of(Quantifier.constant(), getFirstDuplicate(constantId).map(OrderedLiteral::getConstantId).orElse(constantId), type);
    }

    @Nonnull
    public ConstantObjectValue processPreparedStatementArrayParameter(@Nonnull Array param,
                                                                      @Nullable Type.Array type,
                                                                      @Nullable Integer unnamedParameterIndex,
                                                                      @Nullable String parameterName,
                                                                      final int tokenIndex) {
        Type.Array resolvedType = type;
        startArrayLiteral();
        final var arrayElements = new ArrayList<>();
        try {
            if (type == null) {
                resolvedType = SqlTypeSupport.arrayMetadataToArrayType(((RelationalArray) param).getMetaData(), false);
            }
            try (ResultSet rs = param.getResultSet()) {
                while (rs.next()) {
                    arrayElements.add(rs.getObject(2));
                }
            }
        } catch (SQLException e) {
            throw new RelationalException(e).toUncheckedWrappedException();
        }
        if (!arrayElements.isEmpty()) {
            Assert.thatUnchecked(resolvedType.equals(LiteralsUtils.resolveArrayTypeFromObjectsList(arrayElements)),
                    DATATYPE_MISMATCH, "Cannot convert literal to " + resolvedType);
        }
        for (int i = 0; i < arrayElements.size(); i++) {
            final Object o = arrayElements.get(i);
            processPreparedStatementParameter(o, resolvedType.getElementType(), unnamedParameterIndex, parameterName, i);
        }
        finishArrayLiteral(unnamedParameterIndex, parameterName, tokenIndex);
        return processComplexLiteral(OrderedLiteral.constantId(tokenIndex), resolvedType);
    }

    @Nonnull
    public Value processPreparedStatementStructParameter(@Nonnull Struct param,
                                                         @Nullable Type.Record type,
                                                         @Nullable Integer unnamedParameterIndex,
                                                         @Nullable String parameterName,
                                                         int tokenIndex) {
        Type.Record resolvedType = type;
        startStructLiteral();
        Object[] attributes;
        try {
            if (type == null) {
                resolvedType = SqlTypeSupport.structMetadataToRecordType(((RelationalStruct) param).getMetaData(), false);
            }
            attributes = param.getAttributes();
        } catch (SQLException e) {
            throw new RelationalException(e).toUncheckedWrappedException();
        }
        Assert.thatUnchecked(resolvedType.getFields().size() == attributes.length);
        for (int i = 0; i < attributes.length; i++) {
            processPreparedStatementParameter(attributes[i], resolvedType.getFields().get(i).getFieldType(),
                    unnamedParameterIndex, parameterName, i);
        }
        finishStructLiteral(resolvedType, unnamedParameterIndex, parameterName, tokenIndex);
        return processComplexLiteral(OrderedLiteral.constantId(tokenIndex), resolvedType);
    }

    @Nonnull
    public Value processNamedPreparedParam(@Nonnull String param, int tokenIndex) {
        final var value = preparedParams.namedParamValue(param);
        //TODO type should probably be Type.any() instead of null
        return processPreparedStatementParameter(value, null, null, param, tokenIndex);
    }

    @Nonnull
    public Value processUnnamedPreparedParam(int tokenIndex) {
        // TODO (Make prepared statement parameters stateless)
        final int currentUnnamedParameterIndex = preparedParams.currentUnnamedParamIndex();
        final var param = preparedParams.nextUnnamedParamValue();
        //TODO type should probably be Type.any() instead of null
        return processPreparedStatementParameter(param, null, currentUnnamedParameterIndex, null, tokenIndex);
    }

    @Nonnull
    private Value processPreparedStatementParameter(@Nullable Object param,
                                                    @Nullable Type type,
                                                    @Nullable Integer unnamedParameterIndex,
                                                    @Nullable String parameterName,
                                                    int tokenIndex) {
        if (param instanceof Array) {
            Assert.thatUnchecked(type == null || type.isArray(), DATATYPE_MISMATCH, "Array type field required as prepared statement parameter");
            return processPreparedStatementArrayParameter((Array) param, (Type.Array) type, unnamedParameterIndex, parameterName, tokenIndex);
        } else if (param instanceof Struct) {
            Assert.thatUnchecked(type == null || type.isRecord(), DATATYPE_MISMATCH, "Required type field required as prepared statement parameter");
            return processPreparedStatementStructParameter((Struct) param, (Type.Record) type, unnamedParameterIndex, parameterName, tokenIndex);
        } else if (param instanceof byte[]) {
            return processQueryLiteralOrParameter(Type.primitiveType(Type.TypeCode.BYTES), ZeroCopyByteString.wrap((byte[]) param),
                    unnamedParameterIndex, parameterName, tokenIndex);
        } else {
            return processQueryLiteralOrParameter(type == null ? Type.any() : type, param, unnamedParameterIndex, parameterName, tokenIndex);
        }
    }
}
