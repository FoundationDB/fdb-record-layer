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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.TestRecordsGeoProto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeospatialWithinDistanceComponentTest {
    @Test
    void equalsAndHashCodeTest() {
        final GeospatialWithinDistanceComponent original = component(1.0, 2.0, 3.0, coordinates());
        final GeospatialWithinDistanceComponent same = component(1.0, 2.0, 3.0, coordinates());
        assertThat(same).isEqualTo(original);
        assertThat(same).hasSameHashCodeAs(original);
        assertThat(original).isEqualTo(original);

        final List<GeospatialWithinDistanceComponent> different = ImmutableList.of(
                component(9.0, 2.0, 3.0, coordinates()),
                component(1.0, 9.0, 3.0, coordinates()),
                component(1.0, 2.0, 9.0, coordinates()),
                component(1.0, 2.0, 3.0, concat(field("location").nest("longitude"), field("location").nest("latitude"))));
        for (final GeospatialWithinDistanceComponent other : different) {
            assertThat(other).isNotEqualTo(original);
        }
        assertThat(original).isNotEqualTo(null);
        assertThat(original).isNotEqualTo("not a component");
    }

    @Test
    void toStringTest() {
        final GeospatialWithinDistanceComponent original = component(1.0, 2.0, 3.0, coordinates());
        assertThat(original.toString())
                .contains(coordinates().toString())
                .contains("WITHIN 3.0 METERS OF (1.0, 2.0)");
        assertThat(original).doesNotHaveToString(component(1.0, 2.0, 4.0, coordinates()).toString());
    }

    @Test
    void planHashTest() {
        final GeospatialWithinDistanceComponent original = component(1.0, 2.0, 3.0, coordinates());
        final GeospatialWithinDistanceComponent same = component(1.0, 2.0, 3.0, coordinates());
        final GeospatialWithinDistanceComponent different = component(1.0, 2.0, 4.0, coordinates());
        for (final PlanHashable.PlanHashMode mode : ImmutableList.of(PlanHashable.CURRENT_LEGACY, PlanHashable.CURRENT_FOR_CONTINUATION)) {
            assertThat(same.planHash(mode)).isEqualTo(original.planHash(mode));
            assertThat(different.planHash(mode)).isNotEqualTo(original.planHash(mode));
        }
        // The two hash kinds are separate, as the for-continuation hash mixes in a fixed base hash.
        assertThat(original.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isNotEqualTo(original.planHash(PlanHashable.CURRENT_LEGACY));
    }

    @Test
    void expandNotSupportedTest() {
        final GeospatialWithinDistanceComponent original = component(1.0, 2.0, 3.0, coordinates());
        assertThatThrownBy(() -> original.expand(null, null, ImmutableList.of()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void validateRequiresTwoColumnsTest() {
        final GeospatialWithinDistanceComponent valid = component(1.0, 2.0, 3.0, coordinates());
        valid.validate(TestRecordsGeoProto.City.getDescriptor());

        final GeospatialWithinDistanceComponent oneColumn =
                component(1.0, 2.0, 3.0, field("location").nest("latitude"));
        assertThatThrownBy(() -> oneColumn.validate(TestRecordsGeoProto.City.getDescriptor()))
                .isInstanceOf(Query.InvalidExpressionException.class);
    }

    @Nonnull
    private static KeyExpression coordinates() {
        return field("location").nest(concat(field("latitude"), field("longitude")));
    }

    @Nonnull
    private static GeospatialWithinDistanceComponent component(final double latitude, final double longitude,
                                                               final double radiusMeters,
                                                               @Nonnull final KeyExpression coordinatesExpression) {
        return new GeospatialWithinDistanceComponent(DoubleValueOrParameter.value(latitude),
                DoubleValueOrParameter.value(longitude), DoubleValueOrParameter.value(radiusMeters),
                coordinatesExpression);
    }
}
