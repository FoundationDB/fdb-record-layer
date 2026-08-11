/*
 * PatternForLikeValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PPatternForLikeValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence.Precedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A {@link Value} that constructs the pattern for a {@link LikeOperatorValue}. This extracts two fields, one
 * of which represents a pattern and the other an escape value. In some ways, this operates like a
 * {@link RecordConstructorValue}, but it offers two advantages:
 *
 * <ul>
 *     <li><em>It can perform some semantic checks.</em> For example, it can validate that the escape sequence
 *     is not multi-character.</li>
 *     <li><em>It is backwards compatible.</em> Previous versions of this value would return a single regex pattern,
 *     which would then be used to match candidate strings within {@link LikeOperatorValue}. That required a special
 *     value to do the string manipulation, and so to allow for those older plans to be deserialized, we need
 *     a special value here, even if it did the same job as a {@link RecordConstructorValue} with a fixed return type.</li>
 * </ul>
 */
@API(API.Status.EXPERIMENTAL)
public class PatternForLikeValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Like-Operator-Value");

    /**
     * Field in the returned {@link #TYPE} corresponding to the {@code LIKE}'s pattern.
     * This should be a SQL pattern as specified by the {@code LIKE} documentation.
     */
    public static final int PATTERN_FIELD_NUMBER = 1;

    /**
     * Field in the returned {@link #TYPE} corresponding to the {@code LIKE}'s escape character.
     * This can be {@code null} to indicate no escape character should be applied.
     */
    public static final int ESCAPE_FIELD_NUMBER = 2;

    /**
     * Return type of this {@link Value}. This extracts the pattern and escape and creates a
     * simple struct with that information. Fields should be accessed by number using the
     * {@link #PATTERN_FIELD_NUMBER} and {@link #ESCAPE_FIELD_NUMBER} constants.
     */
    @Nonnull
    public static final Type TYPE = Type.Record.fromFields(false, ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(TypeCode.STRING, true), Optional.of("pattern"), Optional.of(PATTERN_FIELD_NUMBER)),
            Type.Record.Field.of(Type.primitiveType(TypeCode.STRING, true), Optional.of("escape"), Optional.of(ESCAPE_FIELD_NUMBER)))
    );

    @Nonnull
    private final Value patternChild;
    @Nonnull
    private final Value escapeChild;

    /**
     * Constructs a new instance of {@link PatternForLikeValue}.
     * @param patternChild the pattern
     * @param escapeChild the escape character
     */
    public PatternForLikeValue(@Nonnull final Value patternChild, @Nonnull final Value escapeChild) {
        this.patternChild = patternChild;
        this.escapeChild = escapeChild;
    }

    @Nullable
    @Override
    @SuppressWarnings("java:S6213")
    public <M extends Message> Message eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final Descriptors.Descriptor typeDescriptor = Objects.requireNonNull(context.getTypeRepository().getMessageDescriptor(TYPE));
        String patternStr = (String)patternChild.eval(store, context);
        final DynamicMessage.Builder resultBuilder = DynamicMessage.newBuilder(typeDescriptor);
        if (patternStr != null) {
            resultBuilder.setField(typeDescriptor.findFieldByNumber(PATTERN_FIELD_NUMBER), patternStr);
        }
        String escapeChar = (String)escapeChild.eval(store, context);
        if (escapeChar != null) {
            SemanticException.check(escapeChar.length() == 1, SemanticException.ErrorCode.ESCAPE_CHAR_OF_LIKE_OPERATOR_IS_NOT_SINGLE_CHAR);
            resultBuilder.setField(typeDescriptor.findFieldByNumber(ESCAPE_FIELD_NUMBER), escapeChar);
        }
        return resultBuilder.build();
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return  ImmutableList.of(patternChild, escapeChild);
    }

    @Nonnull
    @Override
    public PatternForLikeValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 2);
        return new PatternForLikeValue(
                Iterables.get(newChildren, 0),
                Iterables.get(newChildren, 1));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, patternChild, escapeChild);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final var pattern = Iterables.get(explainSuppliers, 0).get();
        final var escape = Iterables.get(explainSuppliers, 1).get();

        return ExplainTokensWithPrecedence.of(Precedence.BETWEEN.parenthesizeChild(pattern).addWhitespace()
                .addKeyword("ESCAPE").addWhitespace().addNested(Precedence.BETWEEN.parenthesizeChild(escape)));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return TYPE;
    }

    @Nonnull
    @Override
    public PPatternForLikeValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PPatternForLikeValue.newBuilder()
                .setPatternChild(patternChild.toValueProto(serializationContext))
                .setEscapeChild(escapeChild.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setPatternForLikeValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static PatternForLikeValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PPatternForLikeValue patternForLikeValueProto) {
        return new PatternForLikeValue(Value.fromValueProto(serializationContext, Objects.requireNonNull(patternForLikeValueProto.getPatternChild())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(patternForLikeValueProto.getEscapeChild())));
    }

    @Nonnull
    private static Value encapsulate(@Nonnull final List<? extends Typed> arguments) {
        Verify.verify(arguments.size() == 2);
        Type patternType = arguments.get(0).getResultType();
        Type escapeType = arguments.get(1).getResultType();
        SemanticException.check(patternType.isNull() || patternType.getTypeCode().equals(TypeCode.STRING), SemanticException.ErrorCode.OPERAND_OF_LIKE_OPERATOR_IS_NOT_STRING);
        SemanticException.check(escapeType.isNull() || escapeType.getTypeCode().equals(TypeCode.STRING), SemanticException.ErrorCode.OPERAND_OF_LIKE_OPERATOR_IS_NOT_STRING);

        return new PatternForLikeValue((Value) arguments.get(0), (Value) arguments.get(1));
    }

    /**
     * The {@code patternForLike} operator.
     */
    @AutoService(BuiltInFunction.class)
    public static class PatternForLikeFn extends BuiltInFunction<Value> {
        public PatternForLikeFn() {
            super("patternForLike",
                    ImmutableList.of(Type.primitiveType(TypeCode.STRING), Type.primitiveType(TypeCode.STRING)),
                    (ignored, args) -> PatternForLikeValue.encapsulate(args.getArgumentsList()));
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PPatternForLikeValue, PatternForLikeValue> {
        @Nonnull
        @Override
        public Class<PPatternForLikeValue> getProtoMessageClass() {
            return PPatternForLikeValue.class;
        }

        @Nonnull
        @Override
        public PatternForLikeValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PPatternForLikeValue patternForLikeValueProto) {
            return PatternForLikeValue.fromProto(serializationContext, patternForLikeValueProto);
        }
    }
}
