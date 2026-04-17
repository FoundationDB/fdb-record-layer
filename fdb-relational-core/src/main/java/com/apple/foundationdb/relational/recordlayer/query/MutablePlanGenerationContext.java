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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.EvaluatesToValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.OfTypeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.WithMetadata;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.metadata.StructTypeValidator;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.apple.foundationdb.relational.api.metadata.DataType;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.apple.foundationdb.relational.api.exceptions.ErrorCode.DATATYPE_MISMATCH;

/**
 * Context keeping state related to plan generation, it also captures execution-specific attributes that influences
 * how the physical plan is handled at runtime, such as continuation, limit, and offset.
 */
@API(API.Status.EXPERIMENTAL)
public class MutablePlanGenerationContext implements QueryExecutionContext {
    @Nonnull
    private final PreparedParams preparedParams;

    @Nonnull
    private final Literals.Builder literalsBuilder;

    private final int parameterHash;

    @Nonnull
    private final PlanHashable.PlanHashMode planHashMode;

    @Nonnull
    private final String query;

    @Nonnull
    private final String canonicalQueryString;

    @Nonnull
    private final List<ConstantObjectValue> constantObjectValues;

    private boolean shouldProcessLiteral;

    private boolean forExplain;

    @Nullable
    private byte[] continuation;

    @Nonnull
    private final ImmutableList.Builder<QueryPredicate> equalityConstraints;

    @Nonnull
    private final Map<String, DataType.StructType> dynamicStructDefinitions;

    private void startStructLiteral() {
        literalsBuilder.startStructLiteral();
    }

    private void finishStructLiteral(@Nonnull Type.Record type,
                                     @Nullable Integer unnamedParameterIndex,
                                     @Nullable String parameterName,
                                     int tokenIndex) {
        literalsBuilder.finishStructLiteral(type, unnamedParameterIndex, parameterName, tokenIndex);
    }

    private void addLiteralReference(@Nonnull ConstantObjectValue constantObjectValue) {
        if (!literalsBuilder.isAddingComplexLiteral()) {
            constantObjectValues.add(constantObjectValue);
        }
    }

    /**
     * Checks if the provided literal argument has already been referenced by a registered {@link OrderedLiteral}.
     * <br>
     * If a matching {@link OrderedLiteral} is found, it is returned, and an equality constraint is created
     * between it and a constant reference representing the lexical position of the current literal token in the query.
     * Otherwise, an empty {@link Optional} is returned.
     * <br>
     * For example, given the query {@code SELECT A + 3 FROM T WHERE B > 3}:
     * <ul>
     *     <li>When encountering the first {@code 3} literal at lexical position 4, calling
     *     {@code getFirstCovReference(3, 4, Type.Int)} will return an empty {@link Optional} because this is the
     *     first occurrence of the literal.</li>
     *     <li>Calling {@code getFirstCovReference(3, 10, Type.Int)} for the second {@code 3} literal found at lexical
     *     position 10 will return the {@link OrderedLiteral} corresponding to the first {@code 3}.  It will also establish
     *     an equality constraint between the two literal tokens: {@code Equality(Cov(const_4), Cov(const_10))}.</li>
     * </ul>
     *
     * @param literal The literal to search for.
     * @param requestedTokenIndex The lexical index of the literal token within the query.
     * @param type The data type of the literal.
     * @return An {@link Optional} containing the corresponding {@link OrderedLiteral} if the literal was previously
     *         encountered; otherwise, an empty {@link Optional}.
     */
    @Nonnull
    private Optional<OrderedLiteral> getFirstDuplicate(@Nullable Object literal, int requestedTokenIndex,
                                                       @Nonnull Type type) {
        final var orderedLiteralMaybe = literalsBuilder.getFirstValueDuplicateMaybe(literal);
        if (orderedLiteralMaybe.isEmpty()) {
            return Optional.empty();
        }
        addEqualityConstraint(literalsBuilder.constructConstantId(requestedTokenIndex),
                orderedLiteralMaybe.get().getConstantId(), type);
        return orderedLiteralMaybe;
    }

    @Nonnull
    private Optional<OrderedLiteral> getFirstDuplicate(@Nonnull final String constantId) {
        final var firstDuplicateMaybe = literalsBuilder.getFirstDuplicateOfConstantIdMaybe(constantId);
        if (firstDuplicateMaybe.isEmpty()) {
            return firstDuplicateMaybe;
        }
        final var firstDuplicate = firstDuplicateMaybe.get();
        addEqualityConstraint(firstDuplicate.getConstantId(), constantId, firstDuplicate.getType());
        return firstDuplicateMaybe;
    }

    private void addEqualityConstraint(@Nonnull String leftTokenId, @Nonnull String rightTokenId, @Nonnull Type type) {
        if (leftTokenId.equals(rightTokenId)) {
            return;
        }
        final var leftCov = ConstantObjectValue.of(Quantifier.constant(), leftTokenId, type);
        final var rightCov = ConstantObjectValue.of(Quantifier.constant(), rightTokenId, type);

        // we can replace the relatively complex predicate below with a much simpler one: (leftCov isNotDistinctFrom rightCov)
        // once https://github.com/FoundationDB/fdb-record-layer/issues/3504 is in, but this is ok for now.

        // Term1: left != null AND right != null AND left = right
        final var leftIsNotNull = new ValuePredicate(leftCov, new Comparisons.NullComparison(Comparisons.Type.NOT_NULL));
        final var rightIsNotNull = new ValuePredicate(rightCov, new Comparisons.NullComparison(Comparisons.Type.NOT_NULL));
        final var equalityPredicate = new ValuePredicate(leftCov, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, rightCov));
        final var notNullComparison = AndPredicate.and(ImmutableList.of(leftIsNotNull, rightIsNotNull, equalityPredicate));

        // Term2: left = null AND right = null
        final var leftIsNull = new ValuePredicate(leftCov, new Comparisons.NullComparison(Comparisons.Type.IS_NULL));
        final var rightIsNull = new ValuePredicate(rightCov, new Comparisons.NullComparison(Comparisons.Type.IS_NULL));
        final var bothAreNullComparison = AndPredicate.and(leftIsNull, rightIsNull);

        // Term1 OR Term2
        final var constraint = OrPredicate.or(notNullComparison, bothAreNullComparison);
        equalityConstraints.add(constraint);
    }


    private void setShouldProcessLiteral(boolean shouldProcessLiteral) {
        this.shouldProcessLiteral = shouldProcessLiteral;
    }

    @Nonnull
    private ConstantObjectValue processPreparedStatementArrayParameter(@Nonnull Array param,
                                                                       @Nullable Type.Array type,
                                                                       @Nullable Integer unnamedParameterIndex,
                                                                       @Nullable String parameterName,
                                                                       final int tokenIndex) {
        Type.Array resolvedType = type;
        startArrayLiteral();
        final var arrayElements = new ArrayList<>();
        try {
            if (type == null) {
                resolvedType = (Type.Array) DataTypeUtils.toRecordLayerType(((RelationalArray) param).getMetaData().asRelationalType());
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
        return processComplexLiteral(tokenIndex, resolvedType);
    }

    @Nonnull
    private Value processQueryLiteralOrParameter(@Nonnull Type type,
                                                 @Nullable Object literal,
                                                 @Nullable Integer unnamedParameterIndex,
                                                 @Nullable String parameterName,
                                                 int tokenIndex) {
        final var literalValue = new LiteralValue<>(literal);
        if (!shouldProcessLiteral()) {
            return literalValue;
        } else {
            final var orderedLiteral = literalsBuilder.addLiteral(type, literal, unnamedParameterIndex, parameterName, tokenIndex);
            final var result = ConstantObjectValue.of(Quantifier.constant(), orderedLiteral.getConstantId(),
                    literalValue.getResultType());
            addLiteralReference(result);
            return getFirstDuplicate(literal, tokenIndex, type)
                    .map(duplicate ->
                            ConstantObjectValue.of(Quantifier.constant(), duplicate.getConstantId(), literalValue.getResultType()))
                    .orElse(result);
        }
    }

    @Nonnull
    private Value processPreparedStatementParameter(@Nullable Object param,
                                                    @Nonnull Type type,
                                                    @Nullable Integer unnamedParameterIndex,
                                                    @Nullable String parameterName,
                                                    int tokenIndex) {
        if (param instanceof Array) {
            Assert.thatUnchecked(type.isArray(), DATATYPE_MISMATCH, "Array type field required as prepared statement parameter instead of " + type);
            return processPreparedStatementArrayParameter((Array)param, (Type.Array)type, unnamedParameterIndex, parameterName, tokenIndex);
        } else if (param instanceof Struct) {
            Assert.thatUnchecked(type.isRecord(), DATATYPE_MISMATCH, "Required type field required as prepared statement parameter instead of " + type);
            return processPreparedStatementStructParameter((Struct)param, (Type.Record)type, unnamedParameterIndex, parameterName, tokenIndex);
        } else if (param instanceof byte[]) {
            return processQueryLiteralOrParameter(Type.primitiveType(Type.TypeCode.BYTES), ZeroCopyByteString.wrap((byte[])param),
                    unnamedParameterIndex, parameterName, tokenIndex);
        } else {
            return processQueryLiteralOrParameter(type, param, unnamedParameterIndex, parameterName, tokenIndex);
        }
    }

    public MutablePlanGenerationContext(@Nonnull PreparedParams preparedParams,
                                        @Nonnull PlanHashable.PlanHashMode planHashMode,
                                        @Nonnull String query,
                                        @Nonnull String canonicalQueryString,
                                        int parameterHash) {
        this.preparedParams = preparedParams;
        this.planHashMode = planHashMode;
        this.query = query;
        this.canonicalQueryString = canonicalQueryString;
        this.parameterHash = parameterHash;
        literalsBuilder = Literals.newBuilder();
        constantObjectValues = new LinkedList<>();
        shouldProcessLiteral = true;
        forExplain = false;
        continuation = null;
        equalityConstraints = ImmutableList.builder();
        dynamicStructDefinitions = new HashMap<>();
    }

    @Nonnull
    public PreparedParams getPreparedParams() {
        return preparedParams;
    }

    public void startArrayLiteral() {
        literalsBuilder.startArrayLiteral();
    }

    public void finishArrayLiteral(@Nullable Integer unnamedParameterIndex,
                                   @Nullable String parameterName,
                                   int tokenIndex) {
        literalsBuilder.finishArrayLiteral(unnamedParameterIndex, parameterName, shouldProcessLiteral, tokenIndex);
    }

    @Nonnull
    @Override
    public Literals getLiterals() {
        // todo: this should be more efficient, we just need an immutable view over the elements of the builder.
        return literalsBuilder.build();
    }

    @Nonnull
    public Literals.Builder getLiteralsBuilder() {
        return literalsBuilder;
    }

    @Nonnull
    @Override
    public PlanHashable.PlanHashMode getPlanHashMode() {
        return planHashMode;
    }

    @Nonnull
    public String getQuery() {
        return query;
    }

    @Nonnull
    public String getCanonicalQueryString() {
        return canonicalQueryString;
    }

    // this is temporary until we have a proper clean up.
    public boolean isForDdl() {
        return !shouldProcessLiteral;
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
    public QueryPlanConstraint getPlanConstraintsForLiteralReferences() {
        final ImmutableList.Builder<QueryPredicate> predicateBuilder = ImmutableList.builder();

        // add type constraint for every ConstantObjectValue.
        constantObjectValues.forEach(cov ->
                predicateBuilder.add(new ValuePredicate(OfTypeValue.from(cov),
                        new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true))));

        // add any literal equality constraints.
        predicateBuilder.addAll(equalityConstraints.build());

        // add literal evaluation for specific values to enable
        // triggering constant folding internally.
        final var evaluationContext = getEvaluationContext();
        constantObjectValues.forEach(cov ->
                predicateBuilder.add(new ValuePredicate(EvaluatesToValue.of(cov, evaluationContext),
                        new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true))));

        return QueryPlanConstraint.ofPredicates(predicateBuilder.build());
    }

    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP2", justification = "Intentional")
    public void setContinuation(@Nullable byte[] continuation) {
        this.continuation = continuation;
    }

    public boolean shouldProcessLiteral() {
        return shouldProcessLiteral;
    }

    public void setForExplain(boolean forExplain) {
        this.forExplain = forExplain;
    }

    @Nonnull
    public Value processQueryLiteral(@Nonnull Type type, @Nullable Object literal, int tokenIndex) {
        return processQueryLiteralOrParameter(type, literal, null, null, tokenIndex);
    }

    /**
     * Runs a closure without literal processing, i.e. without translating a literal found in the
     * AST into a {@link ConstantObjectValue}. This is necessary in cases where the literal is found
     * in a context that does not contribute to the logical plan such as {@code limit} and {@code continuation}.
     *
     * @param supplier The closure to run with literal processing disabled.
     * @param <T> The type of the closure result.
     *
     * @return The result of the closure
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
    public ConstantObjectValue processComplexLiteral(int tokenIndex, @Nonnull Type type) {
        final var constantId = literalsBuilder.constructConstantId(tokenIndex);
        final var result = ConstantObjectValue.of(Quantifier.constant(), constantId, type);
        if (shouldProcessLiteral()) {
            addLiteralReference(result);
        }
        return ConstantObjectValue.of(Quantifier.constant(),
                getFirstDuplicate(constantId).map(OrderedLiteral::getConstantId).orElse(constantId), type);
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
                resolvedType = (Type.Record) DataTypeUtils.toRecordLayerType(((RelationalStruct) param).getMetaData().getRelationalDataType());
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
        return processComplexLiteral(tokenIndex, resolvedType);
    }

    public void importAuxiliaryLiterals(@Nonnull final Literals auxiliaryLiterals) {
        final var newLiterals = literalsBuilder.importLiteralsRetrieveNewLiterals(auxiliaryLiterals);
        for (final var literal : newLiterals) {
            final var literalValue = new LiteralValue<>(literal.getLiteralObject());
            final var duplicateLiteralMaybe = literalsBuilder.getFirstValueDuplicateMaybe(literal.getLiteralObject());
            duplicateLiteralMaybe.ifPresent(prev -> addEqualityConstraint(prev.getConstantId(), literal.getConstantId(), literalValue.getResultType()));
            constantObjectValues.add(ConstantObjectValue.of(Quantifier.constant(), literal.getConstantId(), literalValue.getResultType()));
        }
    }

    @Nonnull
    public Value processNamedPreparedParam(@Nonnull String param, int tokenIndex) {
        final var value = preparedParams.namedParamValue(param);
        //TODO type should probably be Type.any() instead of null
        return processPreparedStatementParameter(value, getObjectType(value), null, param, tokenIndex);
    }

    @Nonnull
    public Value processUnnamedPreparedParam(int tokenIndex) {
        // TODO (Make prepared statement parameters stateless)
        final int currentUnnamedParameterIndex = preparedParams.currentUnnamedParamIndex();
        final Object param;
        if (!shouldProcessLiteral()) {
            param = PreparedParams.copyOf(preparedParams, true).nextUnnamedParamValue();
        } else {
            param = preparedParams.nextUnnamedParamValue();
        }
        return processPreparedStatementParameter(param, getObjectType(param), currentUnnamedParameterIndex, null, tokenIndex);
    }

    @Nonnull
    private static Type getObjectType(@Nullable final Object object) {
        if (object instanceof WithMetadata) {
            try {
                return DataTypeUtils.toRecordLayerType(((WithMetadata)object).getRelationalMetaData());
            } catch (SQLException e) {
                throw new RelationalException(e).toUncheckedWrappedException();
            }
        }
        return Type.fromObject(object);
    }

    /**
     * Registers or validates a dynamic struct definition created within the query.
     * If this is the first time seeing this struct name, registers it.
     * If the struct name was already registered, validates that the new definition matches the previous one.
     *
     * @param structName The name of the struct type
     * @param structType The struct type definition
     * @throws com.apple.foundationdb.relational.api.exceptions.RelationalException if a struct with this name
     *         already exists with an incompatible signature
     */
    public void registerOrValidateDynamicStruct(@Nonnull String structName, @Nonnull DataType.StructType structType) {
        final var normalizedName = structName.toUpperCase(Locale.ROOT);
        final var existing = dynamicStructDefinitions.get(normalizedName);

        if (existing == null) {
            // First time seeing this struct name, register it
            dynamicStructDefinitions.put(normalizedName, structType);
        } else {
            // Struct name already exists, validate compatibility using centralized validator
            // This now correctly ignores nullability and recursively validates nested structs
            StructTypeValidator.validateStructTypesCompatible(existing, structType, structName, true);
        }
    }

    /**
     * Gets a previously registered dynamic struct type by name.
     *
     * @param structName The name of the struct type
     * @return The struct type if it was previously registered, empty otherwise
     */
    @Nonnull
    public Optional<DataType.StructType> getDynamicStructType(@Nonnull String structName) {
        final var normalizedName = structName.toUpperCase(Locale.ROOT);
        return Optional.ofNullable(dynamicStructDefinitions.get(normalizedName));
    }
}
