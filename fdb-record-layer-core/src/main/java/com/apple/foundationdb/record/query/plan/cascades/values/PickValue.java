/*
 * PickValue.java
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
import com.apple.foundationdb.record.planprotos.PPickValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A value representing multiple "alternative" values.
 * This is useful to element SQL-values-like behavior, e.g.
 * <pre>
 * {@code
 * VALUES (1, "Hello World", 3.0),
 *        (2, "Lazy Dag", 6.5),
 *        (10, "Brown Fow", -2.3)
 * }
 * can be rewritten as
 * {@code
 * SELECT PICK(range.index, (1, "Hello World", 3.0), RCV(2, "Lazy Dag", 6.5), (10, "Brown Fow", -2.3))
 * FROM RANGE(3) range
 * }
 * </pre>
 * of their {@link Value}s
 */
@API(API.Status.EXPERIMENTAL)
public class PickValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Pick-Value");
    private final Value selectorValue;
    private final List<? extends Value> alternativeValues;
    private final Iterable<? extends Value> children;
    private final Type resultType;

    public PickValue(final Value selectorValue, final Iterable<? extends Value> alternativeValues) {
        this(selectorValue, alternativeValues, resolveTypesFromAlternatives(alternativeValues));
    }

    private PickValue(final Value selectorValue, final Iterable<? extends Value> alternativeValues,
                      final Type resultType) {
        this.selectorValue = selectorValue;
        this.alternativeValues = ImmutableList.copyOf(alternativeValues);
        this.children = Iterables.concat(ImmutableList.of(selectorValue), alternativeValues);
        this.resultType = resultType;
    }

    @Override
    protected Iterable<? extends Value> computeChildren() {
        return children;
    }

    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, final EvaluationContext context) {
        final var boxedSelectedIndex = (Integer)selectorValue.eval(store, context);
        if (boxedSelectedIndex == null) {
            return null;
        }

        final var selectedIndex = (int)boxedSelectedIndex;
        return alternativeValues.get(selectedIndex).eval(store, context);
    }

    @Override
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        final var newChildrenIterator = newChildren.iterator();
        final var newSelectorValue = newChildrenIterator.next(); // must exist
        // this skips the very first child for the alternatives as that one is the selector
        return new PickValue(newSelectorValue, ImmutableList.copyOf(newChildrenIterator));
    }

    @Override
    public boolean isFunctionallyDependentOn(final Value otherValue) {
        return alternativeValues.stream()
                .allMatch(alternativeValue -> alternativeValue.isFunctionallyDependentOn(otherValue));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChildren());
    }

    @Override
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("pick",
                Value.explainFunctionArguments(explainSuppliers)));
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

    @Override
    public PPickValue toProto(final PlanSerializationContext serializationContext) {
        final var builder = PPickValue.newBuilder();
        builder.setSelectorValue(selectorValue.toValueProto(serializationContext));
        for (final Value alternativeValue : alternativeValues) {
            builder.addAlternativeValues(alternativeValue.toValueProto(serializationContext));
        }
        builder.setResultType(resultType.toTypeProto(serializationContext));
        return builder.build();
    }

    @Override
    public PValue toValueProto(final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setPickValue(toProto(serializationContext)).build();
    }

    public static PickValue fromProto(final PlanSerializationContext serializationContext,
                                      final PPickValue pickValueProto) {
        final ImmutableList.Builder<Value> alternativeValuesBuilder = ImmutableList.builder();
        for (int i = 0; i < pickValueProto.getAlternativeValuesCount(); i ++) {
            alternativeValuesBuilder.add(Value.fromValueProto(serializationContext, pickValueProto.getAlternativeValues(i)));
        }
        return new PickValue(Value.fromValueProto(serializationContext, Objects.requireNonNull(pickValueProto.getSelectorValue())),
                alternativeValuesBuilder.build(),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(pickValueProto.getResultType())));
    }

    private static Type resolveTypesFromAlternatives(final Iterable<? extends Value> alternativeValues) {
        Type commonType = null;
        for (final var alternativeValue : alternativeValues) {
            final var resultType = alternativeValue.getResultType();
            if (commonType == null) {
                commonType = resultType;
            } else {
                SemanticException.check(commonType.equals(resultType), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            }
        }
        return Verify.verifyNotNull(commonType).withNullability(true); // throws if there are no alternatives
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PPickValue, PickValue> {
        @Override
        public Class<PPickValue> getProtoMessageClass() {
            return PPickValue.class;
        }

        @Override
        public PickValue fromProto(final PlanSerializationContext serializationContext,
                                   final PPickValue pickValueProto) {
            return PickValue.fromProto(serializationContext, pickValueProto);
        }
    }

    /**
     * The {@code pick} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class PickValueFn extends BuiltInFunction<Value> {
        public PickValueFn() {
            super("pick", List.of(Type.primitiveType(Type.TypeCode.INT), Type.any()), Type.any(), PickValueFn::encapsulate);
        }

        @SuppressWarnings("PMD.UnusedFormalParameter")
        private static Value encapsulate(BuiltInFunction<Value> ignored,
                                         final CallSiteArguments callSiteArguments) {
            final List<? extends Typed> arguments = callSiteArguments.getArgumentsList();
            Verify.verify(arguments.size() > 1);
            var selectorValue = (Value)arguments.get(0);
            final var selectorMaxType = Type.maximumType(selectorValue.getResultType(), Type.primitiveType(Type.TypeCode.INT));
            SemanticException.check(selectorMaxType != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            selectorValue = PromoteValue.inject(selectorValue, selectorMaxType);

            final var firstAlternative = (Value)arguments.get(1);
            var alternativesMaxType = firstAlternative.getResultType();
            for (int i = 2; i < arguments.size(); i++) {
                alternativesMaxType = Type.maximumType(alternativesMaxType, arguments.get(i).getResultType());
                SemanticException.check(alternativesMaxType != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            }

            final var alternativesList = ImmutableList.<Value>builder();
            for (int i = 1; i < arguments.size(); i++) {
                alternativesList.add(PromoteValue.inject((Value)arguments.get(i), alternativesMaxType));
            }
            return new PickValue(selectorValue, alternativesList.build());
        }
    }
}
