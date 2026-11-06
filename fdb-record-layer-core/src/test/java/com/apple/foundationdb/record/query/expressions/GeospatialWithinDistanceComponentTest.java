/*
 * GeospatialWithinDistanceComponentTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.HaversineDistanceValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ParameterObjectValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behavioral checks for {@link GeospatialWithinDistanceComponent#expand} — the Cascades bridge that turns a
 * {@code WITHIN_DISTANCE} query filter into the reduced two-argument {@link HaversineDistanceValue} placeholder
 * paired with a {@link Comparisons.WithinDistanceComparison} the R-tree match candidate binds against.
 */
class GeospatialWithinDistanceComponentTest {

    @BeforeEach
    void setUpDebugger() {
        Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
        Debugger.setup();
    }

    @Nonnull
    private static Type.Record cityRecordType() {
        final Type.Record location = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.DOUBLE, false), Optional.of("latitude")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.DOUBLE, false), Optional.of("longitude"))));
        return Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(location, Optional.of("location"))));
    }

    @Nonnull
    private static Quantifier.ForEach baseQuantifier() {
        final Reference baseRef = Reference.initialOf(
                new FullUnorderedScanExpression(ImmutableSet.of("City"), cityRecordType(), new AccessHints()));
        return Quantifier.forEach(baseRef);
    }

    @Nonnull
    private static KeyExpression cityCoordinates() {
        return field("location").nest(concat(field("latitude"), field("longitude")));
    }

    @Test
    void expandProducesTwoArgumentHaversinePlaceholderWithWithinDistanceComparison() {
        final GeospatialWithinDistanceComponent component = new GeospatialWithinDistanceComponent(
                DoubleValueOrParameter.value(37.0),
                DoubleValueOrParameter.value(-122.0),
                DoubleValueOrParameter.value(1000.0),
                cityCoordinates());

        final Quantifier.ForEach base = baseQuantifier();
        final GraphExpansion expansion = component.expand(base, () -> base, ImmutableList.of());

        assertThat(expansion.getPredicates()).hasSize(1);
        final QueryPredicate predicate = expansion.getPredicates().get(0);
        assertThat(predicate).isInstanceOf(ValuePredicate.class);
        final ValuePredicate valuePredicate = (ValuePredicate)predicate;
        assertThat(valuePredicate.getValue()).isInstanceOf(HaversineDistanceValue.class);
        // expand() invokes HaversineDistanceValue.transformComparisonMaybe directly so the R-tree candidate's
        // coordinates placeholder can bind against a query-independent (lat, lon) value.
        assertThat(ImmutableList.copyOf(valuePredicate.getValue().getChildren())).hasSize(2);
        assertThat(valuePredicate.getComparison()).isInstanceOf(Comparisons.WithinDistanceComparison.class);
        assertThat(valuePredicate.getComparison().getType()).isEqualTo(Comparisons.Type.WITHIN_DISTANCE);
    }

    @Test
    void literalCenterAndRadiusProduceLiteralValueComparand() {
        final GeospatialWithinDistanceComponent component = new GeospatialWithinDistanceComponent(
                DoubleValueOrParameter.value(0.0),
                DoubleValueOrParameter.value(0.0),
                DoubleValueOrParameter.value(500.0),
                cityCoordinates());

        final Quantifier.ForEach base = baseQuantifier();
        final ValuePredicate predicate = (ValuePredicate)component.expand(base, () -> base, ImmutableList.of())
                .getPredicates().get(0);

        final Comparisons.ValueComparison comparison = (Comparisons.ValueComparison)predicate.getComparison();
        assertThat(comparison.getComparandValue()).isInstanceOf(LiteralValue.class);
    }

    @Test
    void parameterizedRadiusProducesParameterObjectValueComparand() {
        final GeospatialWithinDistanceComponent component = new GeospatialWithinDistanceComponent(
                DoubleValueOrParameter.value(0.0),
                DoubleValueOrParameter.value(0.0),
                DoubleValueOrParameter.parameter("radius"),
                cityCoordinates());

        final Quantifier.ForEach base = baseQuantifier();
        final ValuePredicate predicate = (ValuePredicate)component.expand(base, () -> base, ImmutableList.of())
                .getPredicates().get(0);

        // Radius surfaces via WithinDistanceComparison.getRadiusMetersValue(); getComparandValue() carries the
        // center latitude (the inherited ValueComparison comparand), not the radius.
        final Comparisons.WithinDistanceComparison comparison =
                (Comparisons.WithinDistanceComparison)predicate.getComparison();
        assertThat(comparison.getRadiusMetersValue()).isInstanceOf(ParameterObjectValue.class);
        assertThat(((ParameterObjectValue)comparison.getRadiusMetersValue()).getParameterName()).isEqualTo("radius");
    }

    @Test
    void expandWithSingleColumnCoordinatesExpressionThrowsRecordCoreException() {
        // Only latitude — normalizeKeyForPositions() yields one column, tripping the coordinate-arity guard.
        final KeyExpression singleColumnCoordinates = field("location").nest(field("latitude"));
        final GeospatialWithinDistanceComponent component = new GeospatialWithinDistanceComponent(
                DoubleValueOrParameter.value(37.0),
                DoubleValueOrParameter.value(-122.0),
                DoubleValueOrParameter.value(1000.0),
                singleColumnCoordinates);

        final Quantifier.ForEach base = baseQuantifier();

        assertThatThrownBy(() -> component.expand(base, () -> base, ImmutableList.of()))
                .isInstanceOf(RecordCoreException.class)
                .hasMessageContaining("exactly two columns");
    }
}
