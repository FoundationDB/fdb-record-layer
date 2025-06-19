/*
 * EvaluatesToValue.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PEvaluatesToValue;
import com.apple.foundationdb.record.planprotos.PEvaluation;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Supplier;

public class EvaluatesToValue extends AbstractValue implements Value.RangeMatchableValue, ValueWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Evaluates-To-Value");

    public enum Evaluation {
        IS_TRUE,
        IS_FALSE,
        IS_NULL,
        IS_NOT_NULL
    }

    @Nonnull
    private final Value child;
    @Nonnull
    private final Evaluation evaluation;

    private EvaluatesToValue(@Nonnull final Value child, @Nonnull final Evaluation evaluation) {
        this.child = child;
        this.evaluation = evaluation;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, evaluation, child);
    }

    @Nonnull
    @Override
    public Value getChild() {
        return child;
    }

    @Nonnull
    public Evaluation getEvaluation() {
        return evaluation;
    }

    @Nonnull
    @Override
    public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
        return of(rebasedChild, evaluation);
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nullable final FDBRecordStoreBase<M> store,
                                            @Nonnull final EvaluationContext context) {
        final var value = child.eval(store, context);
        switch (evaluation) {
            case IS_TRUE:
                return value instanceof Boolean && ((Boolean)value);
            case IS_FALSE:
                return value instanceof Boolean && !((Boolean)value);
            case IS_NULL:
                return value == null;
            case IS_NOT_NULL:
                return value != null;
            default:
                throw new RecordCoreException("unexpected evaluation " + evaluation);
        }
    }

    @Override
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object o) {
        return semanticEquals(o, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> evaluation.equals(((EvaluatesToValue)other).getEvaluation()));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, evaluation);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final var child = Iterables.getOnlyElement(explainSuppliers).get().getExplainTokens();
        return ExplainTokensWithPrecedence.of(ExplainTokensWithPrecedence.Precedence.ALWAYS_PARENS,
                child.addWhitespace().addIdentifier("EVALUATES").addWhitespace().addKeyword("TO")
                        .addWhitespace().addIdentifier(evaluation.toString()));
    }

    @Nonnull
    @Override
    public PEvaluatesToValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PEvaluation evaluationProto;
        switch (evaluation) {
            case IS_TRUE:
                evaluationProto = PEvaluation.IS_TRUE;
                break;
            case IS_FALSE:
                evaluationProto = PEvaluation.IS_FALSE;
                break;
            case IS_NULL:
                evaluationProto = PEvaluation.IS_NULL;
                break;
            case IS_NOT_NULL:
                evaluationProto = PEvaluation.IS_NOT_NULL;
                break;
            default:
                throw new RecordCoreException("unexpected evaluation " + evaluation);
        }

        return PEvaluatesToValue.newBuilder()
                .setChild(child.toValueProto(serializationContext))
                .setEvaluation(evaluationProto)
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setEvaluatesToValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static EvaluatesToValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PEvaluatesToValue ofTypeValueProto) {
        final Evaluation evaluation;
        final PEvaluation evaluationProto = ofTypeValueProto.getEvaluation();
        switch (evaluationProto) {
            case IS_TRUE:
                evaluation = Evaluation.IS_TRUE;
                break;
            case IS_FALSE:
                evaluation = Evaluation.IS_FALSE;
                break;
            case IS_NULL:
                evaluation = Evaluation.IS_NULL;
                break;
            case IS_NOT_NULL:
                evaluation = Evaluation.IS_NOT_NULL;
                break;
            default:
                throw new RecordCoreException("unexpected evaluation " + evaluationProto);
        }
        return of(Value.fromValueProto(serializationContext, Objects.requireNonNull(ofTypeValueProto.getChild())), evaluation);
    }

    @Nonnull
    private static EvaluatesToValue of(@Nonnull final Value value, @Nonnull final Evaluation evaluation) {
        return new EvaluatesToValue(value, evaluation);
    }

    @Nonnull
    public static EvaluatesToValue isTrue(@Nonnull final Value value) {
        return of(value, Evaluation.IS_TRUE);
    }

    @Nonnull
    public static EvaluatesToValue isFalse(@Nonnull final Value value) {
        return of(value, Evaluation.IS_FALSE);
    }

    @Nonnull
    public static EvaluatesToValue isNull(@Nonnull final Value value) {
        return of(value, Evaluation.IS_NULL);
    }

    @Nonnull
    public static EvaluatesToValue isNotNull(@Nonnull final Value value) {
        return of(value, Evaluation.IS_NOT_NULL);
    }

    @Nonnull
    public static EvaluatesToValue of(@Nonnull final ConstantObjectValue constantObjectValue,
                                      @Nonnull final EvaluationContext evaluationContext) {
        final var plainValue = constantObjectValue.evalWithoutStore(evaluationContext);
        if (plainValue == null) {
            return isNull(constantObjectValue);
        }
        if (plainValue instanceof Boolean) {
            Boolean booleanPlainValue = (Boolean)plainValue;
            return booleanPlainValue ? isTrue(constantObjectValue) : isFalse(constantObjectValue);
        } else {
            return isNotNull(constantObjectValue);
        }
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(getChild());
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PEvaluatesToValue, EvaluatesToValue> {
        @Nonnull
        @Override
        public Class<PEvaluatesToValue> getProtoMessageClass() {
            return PEvaluatesToValue.class;
        }

        @Nonnull
        @Override
        public EvaluatesToValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                     @Nonnull final PEvaluatesToValue ofTypeValueProto) {
            return EvaluatesToValue.fromProto(serializationContext, ofTypeValueProto);
        }
    }
}
