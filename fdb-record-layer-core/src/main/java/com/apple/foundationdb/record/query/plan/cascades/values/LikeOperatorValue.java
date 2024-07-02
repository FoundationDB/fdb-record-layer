/*
 * LikeOperatorValue.java
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
import com.apple.foundationdb.record.planprotos.PLikeOperatorValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * A {@link Value} that applies a like operator on its child expressions.
 */
@API(API.Status.EXPERIMENTAL)
public class LikeOperatorValue extends AbstractValue implements BooleanValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Like-Operator-Value");

    @Nonnull
    private final Value srcChild;
    @Nonnull
    private final Value patternChild;

    /**
     * Constructs a new instance of {@link LikeOperatorValue}.
     * @param srcChild the string
     * @param patternChild the pattern
     */
    public LikeOperatorValue(@Nonnull final Value srcChild, @Nonnull final Value patternChild) {
        this.srcChild = srcChild;
        this.patternChild = patternChild;
    }

    @Nullable
    @Override
    @SuppressWarnings("java:S6213")
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        String lhs = (String)srcChild.eval(store, context);
        String rhs = (String)patternChild.eval(store, context);
        return likeOperation(lhs, rhs);
    }

    @Nullable
    public static Boolean likeOperation(final String lhs, final String rhs) {
        if (lhs == null || rhs == null) {
            return null;
        }
        Pattern pattern = Pattern.compile(rhs);
        return pattern.matcher(lhs).find();
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return srcChild.explain(formatter) + " LIKE " + patternChild.explain(formatter);
    }

    @Override
    public Optional<QueryPredicate> toQueryPredicate(@Nullable final TypeRepository typeRepository, @Nonnull final CorrelationIdentifier innermostAlias) {
        return Optional.of(new ValuePredicate(srcChild, new Comparisons.ValueComparison(Comparisons.Type.LIKE, patternChild)));
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(srcChild, patternChild);
    }

    @Nonnull
    @Override
    public LikeOperatorValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 2);
        return new LikeOperatorValue(
                Iterables.get(newChildren, 0),
                Iterables.get(newChildren, 1));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, srcChild, patternChild);
    }

    @Override
    public String toString() {
        return srcChild + " LIKE " + patternChild;
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
    public PLikeOperatorValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PLikeOperatorValue.newBuilder()
                .setSrcChild(srcChild.toValueProto(serializationContext))
                .setPatternChild(patternChild.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setLikeOperatorValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static LikeOperatorValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PLikeOperatorValue likeOperatorValueProto) {
        return new LikeOperatorValue(Value.fromValueProto(serializationContext, Objects.requireNonNull(likeOperatorValueProto.getSrcChild())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(likeOperatorValueProto.getPatternChild())));
    }

    @Nonnull
    private static Value encapsulate(@Nonnull final List<? extends Typed> arguments) {
        Verify.verify(arguments.size() == 2);
        Type srcType = arguments.get(0).getResultType();
        Type patternType = arguments.get(1).getResultType();
        SemanticException.check(srcType.getTypeCode().equals(TypeCode.STRING), SemanticException.ErrorCode.OPERAND_OF_LIKE_OPERATOR_IS_NOT_STRING);
        SemanticException.check(patternType.getTypeCode().equals(TypeCode.STRING), SemanticException.ErrorCode.OPERAND_OF_LIKE_OPERATOR_IS_NOT_STRING);

        return new LikeOperatorValue((Value) arguments.get(0), (Value) arguments.get(1));
    }

    /**
     * The {@code like} operator.
     */
    @AutoService(BuiltInFunction.class)
    public static class LikeFn extends BuiltInFunction<Value> {
        public LikeFn() {
            super("like",
                    ImmutableList.of(Type.primitiveType(TypeCode.STRING), Type.primitiveType(TypeCode.STRING)),
                    (ignored, args) -> LikeOperatorValue.encapsulate(args));
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PLikeOperatorValue, LikeOperatorValue> {
        @Nonnull
        @Override
        public Class<PLikeOperatorValue> getProtoMessageClass() {
            return PLikeOperatorValue.class;
        }

        @Nonnull
        @Override
        public LikeOperatorValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                           @Nonnull final PLikeOperatorValue likeOperatorValueProto) {
            return LikeOperatorValue.fromProto(serializationContext, likeOperatorValueProto);
        }
    }
}
