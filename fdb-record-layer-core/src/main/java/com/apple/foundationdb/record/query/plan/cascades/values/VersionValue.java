/*
 * VersionValue.java
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
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.planprotos.PVersionValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.PseudoField;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
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
import java.util.function.Supplier;

/**
 * A value representing a version stamp derived from a quantifier.
 */
@API(API.Status.EXPERIMENTAL)
public class VersionValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Version-Value");

    @Nonnull
    private final Value childValue;

    public VersionValue(@Nonnull final Value childValue) {
        this.childValue = childValue;
    }

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var child = (FDBQueriedRecord<M>)Objects.requireNonNull(childValue.eval(store, context));
        return child.getVersion();
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(Type.TypeCode.VERSION);
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(childValue);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public VersionValue withChildren(@Nonnull final Iterable<? extends Value> newChildren) {
        final var newChild = Iterables.getOnlyElement(newChildren);
        if (newChild == getChildQuantifiedRecordValue()) {
            return this;
        }
        if (newChild instanceof QuantifiedObjectValue) {
            //
            // This is not a bona fide hack, but it is certainly in a gray area. It is okay to insist on the children to
            // be of a certain form, like for instance in this case the child must be either a qrv or a qov, HOWEVER,
            // we imply here that the qov also flows the hidden version information which needs to be ensured by the
            // caller.
            //
            final var newChildQuantifiedObjectValue = (QuantifiedObjectValue)newChild;
            return new VersionValue(QuantifiedRecordValue.of(newChildQuantifiedObjectValue.getAlias(),
                    newChildQuantifiedObjectValue.getResultType()));
        }
        return new VersionValue(newChild);
    }

    @Nonnull
    public QuantifiedRecordValue getChildQuantifiedRecordValue() {
        return (QuantifiedRecordValue)childValue;
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("version",
                Iterables.getOnlyElement(explainSuppliers).get().getExplainTokens()));
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.planHash(mode, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
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
    public PVersionValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PVersionValue.newBuilder()
                .setChild(childValue.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setVersionValue(toProto(serializationContext)).build();
    }

    @Nonnull
    @SuppressWarnings("unused")
    public static VersionValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PVersionValue versionValueProto) {
        if (versionValueProto.hasChild()) {
            return new VersionValue(Value.fromValueProto(serializationContext,
                    Objects.requireNonNull(versionValueProto).getChild()));
        } else {
            // deprecated version -- we will have to fake the input type to be any record which should be ok
            Verify.verify(versionValueProto.hasBaseAlias());
            return new VersionValue(QuantifiedRecordValue.of(
                    CorrelationIdentifier.of(Objects.requireNonNull(versionValueProto.getBaseAlias())),
                    new Type.AnyRecord(false)));
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PVersionValue, VersionValue> {
        @Nonnull
        @Override
        public Class<PVersionValue> getProtoMessageClass() {
            return PVersionValue.class;
        }

        @Nonnull
        @Override
        public VersionValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PVersionValue versionValueProto) {
            return VersionValue.fromProto(serializationContext, versionValueProto);
        }
    }

    /**
     * The {@code and} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class VersionFn extends BuiltInFunction<Value> {
        public VersionFn() {
            super("version",
                    List.of(Type.any()),
                    (ignored, arguments) -> VersionValue.encapsulate(arguments));
        }
    }

    @Nonnull
    private static Value encapsulate(@Nonnull final List<? extends Typed> arguments) {
        final var childRecordValue = Iterables.getOnlyElement(arguments);
        return FieldValue.ofFieldNameAndFuseIfPossible((Value) childRecordValue, PseudoField.ROW_VERSION.getFieldName());
    }
}
