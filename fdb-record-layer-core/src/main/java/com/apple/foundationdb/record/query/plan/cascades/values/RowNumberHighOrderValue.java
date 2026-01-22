/*
 * RowNumberValue.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRowNumberHighOrderValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Locale;
import java.util.function.Supplier;

public class RowNumberHighOrderValue extends AbstractValue implements Value.HighOrderValue, LeafValue  {

    @Nonnull
    private static final String NAME = "ROW_NUMBER_HIGH_ORDER";

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash(NAME + "-Value");

    @Nullable
    private final Integer efSearch;

    @Nullable
    private final Boolean isReturningVectors;

    private final Supplier<BuiltInFunction<RowNumberValue>> rowNumberFunctionSupplier;

    public RowNumberHighOrderValue(@Nonnull final PRowNumberHighOrderValue rowNumberHighOrderValueProto) {
        this.efSearch = rowNumberHighOrderValueProto.hasEfSearch() ? rowNumberHighOrderValueProto.getEfSearch() : null;
        this.isReturningVectors = rowNumberHighOrderValueProto.hasIsReturningVectors() ? rowNumberHighOrderValueProto.getIsReturningVectors() : null;
        this.rowNumberFunctionSupplier = Suppliers.memoize(() -> new CurriedRowNumberFn(efSearch, isReturningVectors));
    }

    public RowNumberHighOrderValue(@Nullable final Integer efSearch,
                                   @Nullable final Boolean isReturningVectors) {
        this.efSearch = efSearch;
        this.isReturningVectors = isReturningVectors;
        this.rowNumberFunctionSupplier = Suppliers.memoize(() -> new CurriedRowNumberFn(efSearch, isReturningVectors));
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, efSearch, isReturningVectors);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall(NAME.toLowerCase(Locale.ROOT),
                Iterables.getOnlyElement(explainSuppliers).get().getExplainTokens()));
    }

    @Nullable
    @Override
    public BuiltInFunction<? extends Value> evalWithoutStore(@Nonnull final EvaluationContext context) {
        return rowNumberFunctionSupplier.get();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, efSearch, isReturningVectors);
    }

    @Nonnull
    @Override
    public PRowNumberHighOrderValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var rowNumberHighOrderValueProtoBuilder = PRowNumberHighOrderValue.newBuilder();
        if (efSearch != null) {
            rowNumberHighOrderValueProtoBuilder.setEfSearch(efSearch);
        }
        if (isReturningVectors != null) {
            rowNumberHighOrderValueProtoBuilder.setIsReturningVectors(isReturningVectors);
        }
        return rowNumberHighOrderValueProtoBuilder.build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setRowNumberHighOrderValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RowNumberHighOrderValue fromProto(@Nonnull final PRowNumberHighOrderValue rowNumberHighOrderValue) {
        return new RowNumberHighOrderValue(rowNumberHighOrderValue);
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRowNumberHighOrderValue, RowNumberHighOrderValue> {
        @Nonnull
        @Override
        public Class<PRowNumberHighOrderValue> getProtoMessageClass() {
            return PRowNumberHighOrderValue.class;
        }

        @Nonnull
        @Override
        public RowNumberHighOrderValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PRowNumberHighOrderValue rowNumberHighOrderValueProto) {
            return RowNumberHighOrderValue.fromProto(rowNumberHighOrderValueProto);
        }
    }

    public static final class CurriedRowNumberFn extends BuiltInFunction<RowNumberValue> {
        CurriedRowNumberFn(@Nullable final Integer efSearch, @Nullable final Boolean isReturningVectors) {
            super("row_number", ImmutableList.of(Type.any(), Type.any()), (builtInFunction, arguments) -> {
                SemanticException.check(arguments.size() >= 2,
                        SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
                final var partitioningValuesList = (AbstractArrayConstructorValue)arguments.get(0);
                final var argumentValuesList = (AbstractArrayConstructorValue)arguments.get(1);
                return new RowNumberValue(partitioningValuesList.getChildren(), argumentValuesList.getChildren(),
                        efSearch, isReturningVectors);
            });
        }
    }
}
