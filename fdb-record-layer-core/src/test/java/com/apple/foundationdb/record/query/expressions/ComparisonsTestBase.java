/*
 * ComparisonsTestBase.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PComparison;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.values.LeafValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.common.collect.Iterables;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class ComparisonsTestBase {
    protected ComparisonsTestBase() {
        // nothing
    }

    @Nonnull
    protected static CorrelationIdentifier q1() {
        return CorrelationIdentifier.of("q1");
    }

    @Nonnull
    protected static CorrelationIdentifier q2() {
        return CorrelationIdentifier.of("q2");
    }

    @Nonnull
    protected static CorrelationIdentifier q3() {
        return CorrelationIdentifier.of("q3");
    }

    @SuppressWarnings("unchecked")
    protected static <T extends Comparisons.Comparison> void protoRoundTripComparison(@Nonnull final T original) {
        final PComparison comparisonProto = original.toComparisonProto(PlanSerializationContext.newForCurrentMode());
        final Map<Descriptors.FieldDescriptor, Object> allFields = comparisonProto.getAllFields();
        Assertions.assertThat(allFields).hasSize(1);
        final Message specificComparison = (Message)Iterables.getOnlyElement(allFields.values());
        assertThat(original.toProto(PlanSerializationContext.newForCurrentMode())).isEqualTo(specificComparison);

        final T roundTripped =
                (T)PlanSerialization.dispatchFromProtoContainer(PlanSerializationContext.newForCurrentMode(),
                        comparisonProto);
        assertThat(roundTripped.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isEqualTo(original.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        assertThat(roundTripped.hashCode()).isEqualTo(original.hashCode());
        assertThat(roundTripped).isEqualTo(original);
    }

    @Nonnull
    protected static Function<Value, Optional<Value>> replacementFunctionFromTranslationMap(@Nonnull final TranslationMap translationMap) {
        return value -> {
            if (value instanceof QuantifiedObjectValue) {
                final CorrelationIdentifier alias = ((QuantifiedObjectValue)value).getAlias();
                if (translationMap.containsSourceAlias(alias)) {
                    return Optional.of(translationMap.applyTranslationFunction(alias, (LeafValue)value));
                }
            }
            return Optional.empty();
        };
    }

    @Nonnull
    protected static String renderExplain(@Nonnull final Comparisons.Comparison comparison) {
        return comparison.explain()
                .getExplainTokens()
                .render(DefaultExplainFormatter.forDebugging())
                .toString();
    }
}
