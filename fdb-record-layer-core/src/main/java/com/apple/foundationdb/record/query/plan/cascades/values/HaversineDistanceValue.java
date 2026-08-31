/*
 * HaversineDistanceValue.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PHaversineDistanceValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanBounds;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence.Precedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Great-circle distance in meters between an index-side {@code (lat, lon)} pair and a query-side
 * {@code (centerLat, centerLon)} pair, delegating the numeric computation to
 * {@link GeospatialRTreeScanBounds#haversineMeters(double, double, double, double)}.
 * <p>
 * The class carries either arity of children:
 * <ul>
 *   <li><b>Full form (four children)</b> — {@code (lat, lon, centerLat, centerLon)}. Built at query-construction time
 *       from {@code GeospatialWithinDistanceComponent.expand(...)}; {@link #eval} computes the great-circle distance
 *       so the predicate can be evaluated without an index.</li>
 *   <li><b>Reduced placeholder form (two children)</b> — {@code (lat, lon)}. Produced by
 *       {@link #transformComparisonMaybe(Comparisons.Type, Value)} from the full form and used as the R-tree
 *       candidate's placeholder value. The query-side coordinates and radius travel with the paired
 *       {@link Comparisons.WithinDistanceComparison}; {@link #eval} on this shape is unsupported because the
 *       coordinates it would need have been factored out into the comparison.</li>
 * </ul>
 *
 * @see Comparisons.WithinDistanceComparison
 * @see RowNumberValue#transformComparisonMaybe(Comparisons.Type, Value) for the analogous vector-index transform
 */
@API(API.Status.EXPERIMENTAL)
public class HaversineDistanceValue extends AbstractValue {
    private static final String NAME = "HaversineDistance";
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash(NAME + "-Value");

    @Nonnull
    private final List<? extends Value> children;

    public HaversineDistanceValue(@Nonnull final Value latitudeValue,
                                  @Nonnull final Value longitudeValue) {
        this(ImmutableList.of(latitudeValue, longitudeValue));
    }

    public HaversineDistanceValue(@Nonnull final Value latitudeValue,
                                  @Nonnull final Value longitudeValue,
                                  @Nonnull final Value centerLatitudeValue,
                                  @Nonnull final Value centerLongitudeValue) {
        this(ImmutableList.of(latitudeValue, longitudeValue, centerLatitudeValue, centerLongitudeValue));
    }

    private HaversineDistanceValue(@Nonnull final List<? extends Value> children) {
        if (children.size() != 2 && children.size() != 4) {
            throw new RecordCoreException("HaversineDistanceValue requires 2 or 4 arguments");
        }
        this.children = children;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        if (children.size() != 4) {
            // The reduced placeholder form (2 children) has had the query-side coordinates factored out into the
            // paired WithinDistanceComparison; evaluating the value on its own is meaningless.
            throw new RecordCoreException("HaversineDistanceValue placeholder form cannot be evaluated directly");
        }
        final Object latitude = children.get(0).eval(store, context);
        final Object longitude = children.get(1).eval(store, context);
        final Object centerLatitude = children.get(2).eval(store, context);
        final Object centerLongitude = children.get(3).eval(store, context);
        if (latitude == null || longitude == null || centerLatitude == null || centerLongitude == null) {
            return null;
        }
        return GeospatialRTreeScanBounds.haversineMeters(
                ((Number) latitude).doubleValue(),
                ((Number) longitude).doubleValue(),
                ((Number) centerLatitude).doubleValue(),
                ((Number) centerLongitude).doubleValue());
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(Type.TypeCode.DOUBLE);
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return children;
    }

    @Nonnull
    @Override
    public HaversineDistanceValue withChildren(final Iterable<? extends Value> newChildren) {
        return new HaversineDistanceValue(ImmutableList.copyOf(newChildren));
    }

    /**
     * Factor the query-side {@code (centerLat, centerLon)} out of the full form and pair the resulting placeholder
     * with a {@link Comparisons.WithinDistanceComparison} carrying {@code (centerLat, centerLon, radius)}.
     * <p>
     * Fires only on the full four-argument form and only for {@link Comparisons.Type#LESS_THAN_OR_EQUALS}: the
     * R-tree geospatial scan implements an inclusive within-radius predicate, so strict-inequality and equality
     * comparisons cannot participate in a scan-prefix binding.
     */
    @Nonnull
    @Override
    public Optional<QueryPredicate> transformComparisonMaybe(@Nonnull final Comparisons.Type comparisonType,
                                                             @Nonnull final Value comparand) {
        if (comparisonType != Comparisons.Type.LESS_THAN_OR_EQUALS) {
            return Optional.empty();
        }
        if (children.size() != 4) {
            return Optional.empty();
        }
        final var reducedPlaceholder = new HaversineDistanceValue(children.get(0), children.get(1));
        final var withinDistance = new Comparisons.WithinDistanceComparison(
                children.get(2), children.get(3), comparand);
        return Optional.of(new ValuePredicate(reducedPlaceholder, withinDistance));
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final var tokens = new ExplainTokens().addKeyword(NAME).addOpeningParen();
        boolean first = true;
        for (final var supplier : explainSuppliers) {
            if (!first) {
                tokens.addKeyword(",").addWhitespace();
            }
            first = false;
            tokens.addNested(supplier.get().getExplainTokens());
        }
        return ExplainTokensWithPrecedence.of(Precedence.NEVER_PARENS, tokens.addClosingParen());
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, children.size());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, children);
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
    public PHaversineDistanceValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var builder = PHaversineDistanceValue.newBuilder()
                .setLatitudeValue(children.get(0).toValueProto(serializationContext))
                .setLongitudeValue(children.get(1).toValueProto(serializationContext));
        if (children.size() == 4) {
            builder.setCenterLatitudeValue(children.get(2).toValueProto(serializationContext));
            builder.setCenterLongitudeValue(children.get(3).toValueProto(serializationContext));
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setHaversineDistanceValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static HaversineDistanceValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                   @Nonnull final PHaversineDistanceValue proto) {
        final var latitude = Value.fromValueProto(serializationContext, proto.getLatitudeValue());
        final var longitude = Value.fromValueProto(serializationContext, proto.getLongitudeValue());
        if (proto.hasCenterLatitudeValue() && proto.hasCenterLongitudeValue()) {
            return new HaversineDistanceValue(latitude, longitude,
                    Value.fromValueProto(serializationContext, proto.getCenterLatitudeValue()),
                    Value.fromValueProto(serializationContext, proto.getCenterLongitudeValue()));
        }
        return new HaversineDistanceValue(latitude, longitude);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PHaversineDistanceValue, HaversineDistanceValue> {
        @Nonnull
        @Override
        public Class<PHaversineDistanceValue> getProtoMessageClass() {
            return PHaversineDistanceValue.class;
        }

        @Nonnull
        @Override
        public HaversineDistanceValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PHaversineDistanceValue proto) {
            return HaversineDistanceValue.fromProto(serializationContext, proto);
        }
    }

    @Nonnull
    private static Value encapsulateInternal(@Nonnull final BuiltInFunction<Value> ignored,
                                             @Nonnull final CallSiteArguments callSiteArguments) {
        return encapsulate(callSiteArguments.getArgumentsList());
    }

    @Nonnull
    private static Value encapsulate(@Nonnull final List<? extends Typed> arguments) {
        Verify.verify(arguments.size() == 4, "geo_distance requires exactly 4 arguments");
        final var promoted = arguments.stream()
                .map(arg -> {
                    SemanticException.check(arg.getResultType().isNumeric(),
                            SemanticException.ErrorCode.INCOMPATIBLE_TYPE,
                            "geo_distance arguments must be numeric");
                    return PromoteValue.inject((Value) arg, Type.primitiveType(Type.TypeCode.DOUBLE));
                })
                .collect(ImmutableList.toImmutableList());
        return new HaversineDistanceValue(promoted.get(0), promoted.get(1), promoted.get(2), promoted.get(3));
    }

    /**
     * The {@code geo_distance(lat, lon, centerLat, centerLon)} scalar SQL function. Numeric arguments are promoted to
     * {@code DOUBLE} so the produced {@link HaversineDistanceValue} is a single-typed comparand that
     * {@link HaversineDistanceValue#transformComparisonMaybe} can rewrite into a
     * {@link Comparisons.WithinDistanceComparison} for R-tree index pushdown.
     */
    @AutoService(BuiltInFunction.class)
    public static class GeoDistanceFn extends BuiltInFunction<Value> {
        public GeoDistanceFn() {
            super("geo_distance",
                    ImmutableList.of(Type.any(), Type.any(), Type.any(), Type.any()),
                    HaversineDistanceValue::encapsulateInternal);
        }
    }
}
