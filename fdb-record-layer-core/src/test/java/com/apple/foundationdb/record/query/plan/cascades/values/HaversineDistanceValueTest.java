/*
 * HaversineDistanceValueTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanBounds;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HaversineDistanceValue}.
 * <p>
 * Two contracts anchor the coverage: the fully-populated form must evaluate the great-circle distance by delegating to
 * {@link GeospatialRTreeScanBounds#haversineMeters}, and {@link HaversineDistanceValue#transformComparisonMaybe} must
 * rewrite {@code haversine(lat, lon, centerLat, centerLon) <= radius} into a query-independent placeholder over
 * {@code (lat, lon)} plus a {@link Comparisons.WithinDistanceComparison} — the shape the geospatial R-tree match
 * candidate binds against.
 */
class HaversineDistanceValueTest {

    // Apple HQ (approx.) and the Golden Gate Bridge midspan (approx.); pair chosen for a well-known non-zero distance.
    private static final double APPLE_LAT = 37.3346;
    private static final double APPLE_LON = -122.0090;
    private static final double GG_LAT = 37.8199;
    private static final double GG_LON = -122.4783;

    @Test
    void fourArgFormEvalDelegatesToHaversineMeters() {
        final var value = new HaversineDistanceValue(
                LiteralValue.ofScalar(APPLE_LAT),
                LiteralValue.ofScalar(APPLE_LON),
                LiteralValue.ofScalar(GG_LAT),
                LiteralValue.ofScalar(GG_LON));

        final Object result = value.evalWithoutStore(EvaluationContext.empty());

        Assertions.assertInstanceOf(Double.class, result);
        Assertions.assertEquals(
                GeospatialRTreeScanBounds.haversineMeters(APPLE_LAT, APPLE_LON, GG_LAT, GG_LON),
                (Double) result,
                1.0e-9);
    }

    @Test
    void fourArgFormEvalReturnsZeroForSamePoint() {
        final var value = new HaversineDistanceValue(
                LiteralValue.ofScalar(APPLE_LAT),
                LiteralValue.ofScalar(APPLE_LON),
                LiteralValue.ofScalar(APPLE_LAT),
                LiteralValue.ofScalar(APPLE_LON));

        Assertions.assertEquals(0.0, (Double) value.evalWithoutStore(EvaluationContext.empty()), 1.0e-9);
    }

    @Test
    void twoArgPlaceholderFormEvalThrows() {
        final var placeholder = new HaversineDistanceValue(
                LiteralValue.ofScalar(APPLE_LAT),
                LiteralValue.ofScalar(APPLE_LON));

        Assertions.assertThrows(RuntimeException.class,
                () -> placeholder.evalWithoutStore(EvaluationContext.empty()),
                "the placeholder form is not directly evaluable");
    }

    @Test
    void resultTypeIsDouble() {
        final var value = new HaversineDistanceValue(
                LiteralValue.ofScalar(0.0),
                LiteralValue.ofScalar(0.0),
                LiteralValue.ofScalar(0.0),
                LiteralValue.ofScalar(0.0));
        Assertions.assertEquals(Type.primitiveType(Type.TypeCode.DOUBLE), value.getResultType());
    }

    @Test
    void transformComparisonWithLessThanOrEqualsProducesWithinDistancePredicate() {
        final var latColumn = LiteralValue.ofScalar(0.0);
        final var lonColumn = LiteralValue.ofScalar(1.0);
        final var centerLat = LiteralValue.ofScalar(APPLE_LAT);
        final var centerLon = LiteralValue.ofScalar(APPLE_LON);
        final var radius = LiteralValue.ofScalar(1_000.0);
        final var query = new HaversineDistanceValue(latColumn, lonColumn, centerLat, centerLon);

        final var transformed = query.transformComparisonMaybe(Comparisons.Type.LESS_THAN_OR_EQUALS, radius);

        Assertions.assertTrue(transformed.isPresent(), "<= transform should fire");
        Assertions.assertInstanceOf(ValuePredicate.class, transformed.get());
        final var valuePredicate = (ValuePredicate) transformed.get();

        Assertions.assertInstanceOf(HaversineDistanceValue.class, valuePredicate.getValue());
        final var reduced = (HaversineDistanceValue) valuePredicate.getValue();
        Assertions.assertEquals(
                ImmutableList.of(latColumn, lonColumn),
                ImmutableList.copyOf(reduced.getChildren()),
                "the reduced placeholder retains only the index-side columns");

        Assertions.assertInstanceOf(Comparisons.WithinDistanceComparison.class, valuePredicate.getComparison());
        final var withinDistance = (Comparisons.WithinDistanceComparison) valuePredicate.getComparison();
        Assertions.assertSame(centerLat, withinDistance.getCenterLatitudeValue());
        Assertions.assertSame(centerLon, withinDistance.getCenterLongitudeValue());
        Assertions.assertSame(radius, withinDistance.getRadiusMetersValue());
    }

    @Test
    void transformComparisonWithEqualsDoesNotFire() {
        final var query = new HaversineDistanceValue(
                LiteralValue.ofScalar(0.0),
                LiteralValue.ofScalar(1.0),
                LiteralValue.ofScalar(APPLE_LAT),
                LiteralValue.ofScalar(APPLE_LON));

        Assertions.assertTrue(query.transformComparisonMaybe(Comparisons.Type.EQUALS, LiteralValue.ofScalar(500.0))
                .isEmpty());
    }

    @Test
    void transformComparisonOnReducedPlaceholderDoesNotFire() {
        // The placeholder form has no query-side coordinates to factor out; a second transform pass must be a no-op or
        // repeated rewrites would loop and produce a malformed comparison.
        final var placeholder = new HaversineDistanceValue(LiteralValue.ofScalar(0.0), LiteralValue.ofScalar(1.0));

        Assertions.assertTrue(placeholder.transformComparisonMaybe(
                        Comparisons.Type.LESS_THAN_OR_EQUALS, LiteralValue.ofScalar(500.0))
                .isEmpty());
    }

    @Test
    void withChildrenPreservesArity() {
        final var fourArg = new HaversineDistanceValue(
                LiteralValue.ofScalar(0.0), LiteralValue.ofScalar(1.0),
                LiteralValue.ofScalar(2.0), LiteralValue.ofScalar(3.0));
        final var newFour = fourArg.withChildren(ImmutableList.of(
                LiteralValue.ofScalar(10.0), LiteralValue.ofScalar(11.0),
                LiteralValue.ofScalar(12.0), LiteralValue.ofScalar(13.0)));
        Assertions.assertInstanceOf(HaversineDistanceValue.class, newFour);
        Assertions.assertEquals(4, ImmutableList.copyOf(newFour.getChildren()).size());

        final var twoArg = new HaversineDistanceValue(LiteralValue.ofScalar(0.0), LiteralValue.ofScalar(1.0));
        final var newTwo = twoArg.withChildren(ImmutableList.of(
                LiteralValue.ofScalar(20.0), LiteralValue.ofScalar(21.0)));
        Assertions.assertInstanceOf(HaversineDistanceValue.class, newTwo);
        Assertions.assertEquals(2, ImmutableList.copyOf(newTwo.getChildren()).size());
    }

    @Test
    void withChildrenRejectsUnsupportedArity() {
        final var value = new HaversineDistanceValue(
                LiteralValue.ofScalar(0.0), LiteralValue.ofScalar(1.0),
                LiteralValue.ofScalar(2.0), LiteralValue.ofScalar(3.0));

        Assertions.assertThrows(RuntimeException.class,
                () -> value.withChildren(ImmutableList.of(LiteralValue.ofScalar(0.0))),
                "only the placeholder and full forms are legal");
    }

    @Test
    void protoRoundTripPreservesFourArgumentForm() {
        final var original = new HaversineDistanceValue(
                LiteralValue.ofScalar(APPLE_LAT),
                LiteralValue.ofScalar(APPLE_LON),
                LiteralValue.ofScalar(GG_LAT),
                LiteralValue.ofScalar(GG_LON));
        final var context = new PlanSerializationContext(
                DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);

        final var proto = original.toProto(context);
        final var restored = HaversineDistanceValue.fromProto(context, proto);

        Assertions.assertEquals(
                original.planHash(PlanHashable.PlanHashMode.VC0),
                restored.planHash(PlanHashable.PlanHashMode.VC0));
        Assertions.assertEquals(4, ImmutableList.copyOf(restored.getChildren()).size());
    }

    @Test
    void protoRoundTripPreservesReducedPlaceholderForm() {
        final var original = new HaversineDistanceValue(
                LiteralValue.ofScalar(APPLE_LAT),
                LiteralValue.ofScalar(APPLE_LON));
        final var context = new PlanSerializationContext(
                DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);

        final var proto = original.toProto(context);
        final var restored = HaversineDistanceValue.fromProto(context, proto);

        Assertions.assertEquals(2, ImmutableList.copyOf(restored.getChildren()).size());
        Assertions.assertEquals(
                original.planHash(PlanHashable.PlanHashMode.VC0),
                restored.planHash(PlanHashable.PlanHashMode.VC0));
    }

    @Test
    void geoDistanceFnResolvesAndEncapsulatesToFullFormValue() {
        final var resolved = BuiltInFunctionCatalog.resolve("geo_distance", 4);
        Assertions.assertTrue(resolved.isPresent(), "geo_distance/4 must resolve via the built-in catalog");
        Assertions.assertInstanceOf(HaversineDistanceValue.GeoDistanceFn.class, resolved.get());

        final var encapsulated = resolved.get().encapsulate(CallSiteArguments.ofPositional(
                LiteralValue.ofScalar(APPLE_LAT),
                LiteralValue.ofScalar(APPLE_LON),
                LiteralValue.ofScalar(GG_LAT),
                LiteralValue.ofScalar(GG_LON)));

        Assertions.assertInstanceOf(HaversineDistanceValue.class, encapsulated);
        final var haversine = (HaversineDistanceValue) encapsulated;
        final var children = ImmutableList.copyOf(haversine.getChildren());
        Assertions.assertEquals(4, children.size(), "the four-argument form carries lat, lon, centerLat, centerLon");
        for (final Value child : children) {
            Assertions.assertEquals(Type.primitiveType(Type.TypeCode.DOUBLE), child.getResultType(),
                    "geo_distance arguments are promoted to DOUBLE so downstream planning sees a single numeric type");
        }
    }

    @Test
    void geoDistanceFnPromotesIntegralArgumentsToDouble() {
        // Integer literals are legal SQL — the encapsulation must promote them, otherwise the R-tree pushdown path
        // (which is DOUBLE-typed on both sides) would refuse the rewrite.
        final var geoDistance = BuiltInFunctionCatalog.resolve("geo_distance", 4).orElseThrow();

        final var encapsulated = (HaversineDistanceValue) geoDistance.encapsulate(CallSiteArguments.ofPositional(
                LiteralValue.ofScalar(0),
                LiteralValue.ofScalar(0),
                LiteralValue.ofScalar(1),
                LiteralValue.ofScalar(1)));

        for (final Value child : encapsulated.getChildren()) {
            Assertions.assertEquals(Type.primitiveType(Type.TypeCode.DOUBLE), child.getResultType());
        }
    }

    @Test
    void geoDistanceFnRejectsNonNumericArgument() {
        final var geoDistance = BuiltInFunctionCatalog.resolve("geo_distance", 4).orElseThrow();

        Assertions.assertThrows(SemanticException.class,
                () -> geoDistance.encapsulate(CallSiteArguments.ofPositional(
                        LiteralValue.ofScalar(APPLE_LAT),
                        LiteralValue.ofScalar(APPLE_LON),
                        LiteralValue.ofScalar("not-a-number"),
                        LiteralValue.ofScalar(GG_LON))));
    }

    @Test
    void geoDistanceFnDoesNotResolveForWrongArity() {
        Assertions.assertTrue(BuiltInFunctionCatalog.resolve("geo_distance", 3).isEmpty(),
                "geo_distance is not defined for 3 arguments");
        Assertions.assertTrue(BuiltInFunctionCatalog.resolve("geo_distance", 5).isEmpty(),
                "geo_distance is not defined for 5 arguments");
    }
}
