/*
 * CollateValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.CollateFunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.CollateFunctionKeyExpressionFactoryJRE;
import com.apple.foundationdb.record.planprotos.PCollateValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.common.text.TextCollator;
import com.apple.foundationdb.record.provider.common.text.TextCollatorRegistry;
import com.apple.foundationdb.record.provider.common.text.TextCollatorRegistryJRE;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A {@link Value} that turns a string into a locale-specific sort key.
 */
@API(API.Status.EXPERIMENTAL)
public class CollateValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Collate-Value");

    @Nonnull
    private final TextCollatorRegistry collatorRegistry;
    @Nonnull
    private final Value stringChild;
    @Nullable
    private final Value localeChild;
    @Nullable
    private final Value strengthChild;
    @Nullable
    private final TextCollator invariableCollator;

    public CollateValue(@Nonnull final TextCollatorRegistry collatorRegistry,
                        @Nonnull final Value stringChild, @Nullable final Value localeChild, @Nullable final Value strengthChild) {
        this.collatorRegistry = collatorRegistry;
        this.stringChild = stringChild;
        this.localeChild = localeChild;
        this.strengthChild = strengthChild;
        this.invariableCollator = getInvariableCollator(collatorRegistry, localeChild, strengthChild);
    }

    @Nonnull
    public TextCollatorRegistry getCollatorRegistry() {
        return collatorRegistry;
    }
    
    @Nullable
    @Override
    public <M extends Message> ByteString eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final String str = (String)stringChild.eval(store, context);
        final TextCollator collator = getTextCollator(store, context);
        return collator.getKey(str); //TODO str may be null?
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final var stringExplainTokens =
                Iterables.get(explainSuppliers, 0).get().getExplainTokens();
        final var localeExplainTokens =
                localeChild == null
                ? new ExplainTokens().addKeyword("DEFAULT")
                : Iterables.get(explainSuppliers, 1).get().getExplainTokens();
        final var strengthExplainTokens =
                strengthChild == null
                ? new ExplainTokens().addKeyword("DEFAULT")
                : Iterables.get(explainSuppliers, localeChild == null ? 1 : 2).get().getExplainTokens();

        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("collate",
                new ExplainTokens().addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(),
                        stringExplainTokens,
                        localeExplainTokens,
                        new ExplainTokens().addKeyword("STRENGTH").addWhitespace().addNested(strengthExplainTokens))));
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(TypeCode.BYTES);
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        ImmutableList.Builder<Value> list = ImmutableList.builder();
        list.add(stringChild);
        if (localeChild != null) {
            list.add(localeChild);
        }
        if (strengthChild != null) {
            list.add(strengthChild);
        }
        return list.build();
    }

    @Nonnull
    @Override
    public CollateValue withChildren(final Iterable<? extends Value> newChildren) {
        final Iterator<? extends Value> iter = newChildren.iterator();
        Verify.verify(iter.hasNext());
        final Value stringChild = iter.next();
        final Value localeChild;
        final Value strengthChild;
        if (iter.hasNext()) {
            localeChild = iter.next();
            if (iter.hasNext()) {
                strengthChild = iter.next();
            } else {
                strengthChild = null;
            }
        } else {
            strengthChild = localeChild = null;
        }
        Verify.verify(!iter.hasNext());
        return new CollateValue(collatorRegistry, stringChild, localeChild, strengthChild);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, collatorRegistry.getName());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, collatorRegistry.getName(),
                                            stringChild, localeChild, strengthChild);
    }

    @Nonnull
    @Override
    public BooleanWithConstraint equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other).filter(ignored -> {
            CollateValue otherCollate = (CollateValue)other;
            return collatorRegistry.equals(otherCollate.collatorRegistry);
        });
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
    public PCollateValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        PCollateValue.Builder builder = PCollateValue.newBuilder();
        builder.setCollatorRegistry(collatorRegistry.getName());
        builder.setStringChild(stringChild.toValueProto(serializationContext));
        if (localeChild != null) {
            builder.setLocaleChild(localeChild.toValueProto(serializationContext));
        }
        if (strengthChild != null) {
            builder.setStrengthChild(strengthChild.toValueProto(serializationContext));
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setCollateValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static CollateValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PCollateValue collateValueProto) {
        final TextCollatorRegistry collatorRegistry = getCollatorRegistryFromProto(collateValueProto.getCollatorRegistry());
        final Value stringChild = Value.fromValueProto(serializationContext, collateValueProto.getStringChild());
        final Value localeChild;
        final Value strengthChild;
        if (collateValueProto.hasLocaleChild()) {
            localeChild = Value.fromValueProto(serializationContext, collateValueProto.getLocaleChild());
        } else {
            localeChild = null;
        }
        if (collateValueProto.hasStrengthChild()) {
            strengthChild = Value.fromValueProto(serializationContext, collateValueProto.getStrengthChild());
        } else {
            strengthChild = null;
        }
        return new CollateValue(collatorRegistry, stringChild, localeChild, strengthChild);
    }

    /**
     * Deserializer for {@link PCollateValue}.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PCollateValue, CollateValue> {
        @Nonnull
        @Override
        public Class<PCollateValue> getProtoMessageClass() {
            return PCollateValue.class;
        }

        @Nonnull
        @Override
        public CollateValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PCollateValue collateValueProto) {
            return CollateValue.fromProto(serializationContext, collateValueProto);
        }
    }

    @Nullable
    @SuppressWarnings("unchecked")
    protected static TextCollator getInvariableCollator(@Nonnull final TextCollatorRegistry collatorRegistry,
                                                        @Nullable final Value localeChild, @Nullable final Value strengthChild) {
        if (localeChild == null) {
            if (strengthChild == null) {
                return collatorRegistry.getTextCollator();
            }
            if (strengthChild instanceof LiteralValue) {
                final Integer strength = ((LiteralValue<Integer>)strengthChild).getLiteralValue();
                return collatorRegistry.getTextCollator(strength == null ? 0 : strength);
            }
        } else if (localeChild instanceof LiteralValue) {
            final String locale = ((LiteralValue<String>)localeChild).getLiteralValue();
            if (locale == null) {
                if (strengthChild == null) {
                    return collatorRegistry.getTextCollator();
                }
                if (strengthChild instanceof LiteralValue) {
                    final Integer strength = ((LiteralValue<Integer>)strengthChild).getLiteralValue();
                    return collatorRegistry.getTextCollator(strength == null ? 0 : strength);
                }
            } else {
                if (strengthChild == null) {
                    return collatorRegistry.getTextCollator(locale);
                }
                if (strengthChild instanceof LiteralValue) {
                    final Integer strength = ((LiteralValue<Integer>)strengthChild).getLiteralValue();
                    return collatorRegistry.getTextCollator(locale, strength == null ? 0 : strength);
                }
            }
        }
        return null;
    }

    @Nonnull
    private <M extends Message> TextCollator getTextCollator(@Nullable final FDBRecordStoreBase<M> store,
                                                             @Nonnull final EvaluationContext context) {
        if (invariableCollator != null) {
            return invariableCollator;
        }
        if (localeChild != null) {
            final String locale = (String)localeChild.eval(store, context);
            if (strengthChild != null) {
                final int strength = (Integer)strengthChild.eval(store, context);
                return collatorRegistry.getTextCollator(locale, strength);
            }
            return collatorRegistry.getTextCollator(locale);
        } else if (strengthChild != null) {
            final int strength = (Integer)strengthChild.eval(store, context);
            return collatorRegistry.getTextCollator(strength);
        } else {
            return collatorRegistry.getTextCollator();
        }
    }

    private static TextCollatorRegistry getCollatorRegistryFromProto(@Nonnull final String name) {
        CollateFunctionKeyExpression keyExpression = (CollateFunctionKeyExpression)
                Key.Expressions.function("collate_" + name,
                        Key.Expressions.concatenateFields("_string", "_locale", "_strength"));
        return keyExpression.getCollatorRegistry();
    }

    /**
     * Base class for defining collation built-in function.
     */
    public static class CollateFunction extends BuiltInFunction<Value> {
        public CollateFunction(@Nonnull final String functionName,
                               @Nonnull final TextCollatorRegistry collatorRegistry) {
            super(functionName,
                    ImmutableList.of(Type.primitiveType(Type.TypeCode.STRING)), Type.any(),
                    (builtInFunction, arguments) -> CollateValue.encapsulate(collatorRegistry, arguments));
        }

        @Nonnull
        @Override
        public Optional<BuiltInFunction<Value>> validateCall(@Nonnull final List<Type> argumentTypes) {
            // We claimed to be string + variadic any.
            return super.validateCall(argumentTypes).filter(ignoreThis -> {
                final int nargs = argumentTypes.size();
                if (nargs < 2) {
                    return true;
                }
                if (nargs > 3) {
                    return false;
                }
                if (argumentTypes.get(1).getTypeCode() != TypeCode.STRING) {
                    return false;
                }
                if (nargs < 3) {
                    return true;
                }
                return argumentTypes.get(2).getTypeCode() == TypeCode.INT;
            });
        }
    }

    @Nonnull
    private static Value encapsulate(@Nonnull final TextCollatorRegistry collatorRegistry,
                                     @Nonnull final List<? extends Typed> arguments) {
        final int nargs = arguments.size();
        Verify.verify(nargs >= 1 && nargs <= 3);
        final Typed stringArg = arguments.get(0);
        SemanticException.check(stringArg.getResultType().isPrimitive(), SemanticException.ErrorCode.ARGUMENT_TO_COLLATE_IS_OF_COMPLEX_TYPE);
        final Typed localeArg;
        if (nargs > 1) {
            localeArg = arguments.get(1);
            SemanticException.check(localeArg.getResultType().isPrimitive(), SemanticException.ErrorCode.ARGUMENT_TO_COLLATE_IS_OF_COMPLEX_TYPE);
        } else {
            localeArg = null;
        }
        final Typed strengthArg;
        if (nargs > 2) {
            strengthArg = arguments.get(2);
            SemanticException.check(strengthArg.getResultType().isPrimitive(), SemanticException.ErrorCode.ARGUMENT_TO_COLLATE_IS_OF_COMPLEX_TYPE);
        } else {
            strengthArg = null;
        }
        return new CollateValue(collatorRegistry, (Value)stringArg, (Value)localeArg, (Value)strengthArg);
    }

    /**
     * Define {@code collate_jre} built-in function.
     */
    @AutoService(BuiltInFunction.class)
    @SuppressWarnings("checkstyle:abbreviationaswordinname") // Allow JRE here.
    public static class CollateValueJRE extends CollateFunction {
        public CollateValueJRE() {
            super(CollateFunctionKeyExpressionFactoryJRE.FUNCTION_NAME, TextCollatorRegistryJRE.instance());
        }
    }
}
