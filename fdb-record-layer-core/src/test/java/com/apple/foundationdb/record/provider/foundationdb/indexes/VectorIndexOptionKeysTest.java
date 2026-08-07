/*
 * VectorIndexOptionKeysTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.MetaDataEvolutionValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the index-time vector option catalog and its guards, plus the {@link VectorOptionKey} primitive they are
 * built from (its typed read/write and plan-hash behavior). These are plain unit tests — no FDB required.
 */
class VectorIndexOptionKeysTest {
    @Test
    void indexSpecifyingBothAliasNamesIsRejected() {
        // Both the current and the legacy name of the metric option are set (to the SAME value) — this must still be
        // rejected, since which one wins would otherwise be silent.
        final Index index =
                new Index("v", field("vector_data"), IndexTypes.VECTOR,
                        ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                                IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name(),
                                IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name()));

        assertThatThrownBy(() -> VectorIndexHelper.validate(index))
                .isInstanceOf(MetaDataException.class);
    }

    @Test
    void indexSpecifyingSingleNameIsAccepted() {
        // The same option under exactly one name (the legacy one here) is fine.
        final Index index =
                new Index("v", field("vector_data"), IndexTypes.VECTOR,
                        ImmutableMap.of(IndexOptions.HNSW_NUM_DIMENSIONS, "128",
                                IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name()));

        VectorIndexHelper.validate(index);
    }

    @Test
    void metaDataBuildRejectsIndexWithBothAliasNames() {
        // Prove the guard is reached through the real infrastructure, not just the helper: building metadata runs
        // MetaDataValidator -> VectorIndexValidator.validate -> VectorIndexHelper.validate, which rejects an option
        // specified under both its current and legacy name.
        final RecordMetaDataBuilder builder =
                RecordMetaData.newBuilder().setRecords(TestRecordsVectorsProto.getDescriptor());
        builder.getRecordType("VectorRecord").setPrimaryKey(concatenateFields("group_id", "rec_no"));
        builder.addIndex("VectorRecord",
                new Index("DualNameVectorIndex", new KeyWithValueExpression(field("vector_data"), 0),
                        IndexTypes.VECTOR,
                        ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                                IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name(),
                                IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name())));

        assertThatThrownBy(builder::build).isInstanceOf(MetaDataException.class);
    }

    @Test
    void metaDataEvolutionRejectsAddingAConflictingAlias() {
        // Evolve a valid v1 index (metric under one name) to a v2 that also adds the legacy alias for the same option.
        // Metadata evolution builds AND validates the new metadata (buildMetaData(newProto, /*validate*/ true)) before
        // checking the transition, so the dual-name conflict is caught the same way as at creation.
        final RecordMetaData v1 = vectorMetaData(IndexOptions.VECTOR_METRIC);

        final RecordMetaDataProto.MetaData.Builder v2Proto = v1.toProto().toBuilder()
                .setVersion(v1.getVersion() + 1);
        boolean mutated = false;
        for (final RecordMetaDataProto.Index.Builder indexBuilder : v2Proto.getIndexesBuilderList()) {
            if ("MetricIndex".equals(indexBuilder.getName())) {
                indexBuilder.addOptions(RecordMetaDataProto.Index.Option.newBuilder()
                        .setKey(IndexOptions.HNSW_METRIC)
                        .setValue(Metric.EUCLIDEAN_METRIC.name()));
                mutated = true;
            }
        }
        assertThat(mutated).isTrue();

        // A validating build of the evolved metadata (what the store performs during evolution) must reject it.
        assertThatThrownBy(() -> RecordMetaData.newBuilder().setRecords(v2Proto.build()).build())
                .isInstanceOf(MetaDataException.class);
    }

    @Test
    void metaDataEvolutionAllowsMigratingFromLegacyToCanonicalName() {
        // The intended migration: the old index specifies the metric under its LEGACY name; the new index moves to the
        // canonical name with the SAME effective value. Evolution must accept this — the effective-value comparison in
        // validateChangedOptions treats it as no change even though the option name differs.
        final RecordMetaData v1 = vectorMetaData(IndexOptions.HNSW_METRIC);

        final RecordMetaDataProto.MetaData.Builder v2Proto = v1.toProto().toBuilder()
                .setVersion(v1.getVersion() + 1);
        boolean mutated = false;
        for (final RecordMetaDataProto.Index.Builder indexBuilder : v2Proto.getIndexesBuilderList()) {
            if ("MetricIndex".equals(indexBuilder.getName())) {
                for (final RecordMetaDataProto.Index.Option.Builder optionBuilder : indexBuilder.getOptionsBuilderList()) {
                    if (IndexOptions.HNSW_METRIC.equals(optionBuilder.getKey())) {
                        optionBuilder.setKey(IndexOptions.VECTOR_METRIC);   // same value, moved to the canonical name
                        mutated = true;
                    }
                }
            }
        }
        assertThat(mutated).isTrue();

        // The evolved metadata is itself valid, and evolution from v1 to v2 is accepted.
        final RecordMetaData v2 = RecordMetaData.newBuilder().setRecords(v2Proto.build()).build();
        MetaDataEvolutionValidator.getDefaultInstance().validate(v1, v2);
    }

    @Test
    void metaDataEvolutionAllowsMigratingFromCanonicalToLegacyName() {
        // The reverse migration: the old index specifies the metric under its CANONICAL name; the new index moves back
        // to the LEGACY alias with the SAME effective value. Evolution must accept this too — the effective-value
        // comparison in validateChangedOptions is symmetric, so which of a key's names each side uses does not matter as
        // long as the value agrees.
        final RecordMetaData v1 = vectorMetaData(IndexOptions.VECTOR_METRIC);

        final RecordMetaDataProto.MetaData.Builder v2Proto = v1.toProto().toBuilder()
                .setVersion(v1.getVersion() + 1);
        boolean mutated = false;
        for (final RecordMetaDataProto.Index.Builder indexBuilder : v2Proto.getIndexesBuilderList()) {
            if ("MetricIndex".equals(indexBuilder.getName())) {
                for (final RecordMetaDataProto.Index.Option.Builder optionBuilder : indexBuilder.getOptionsBuilderList()) {
                    if (IndexOptions.VECTOR_METRIC.equals(optionBuilder.getKey())) {
                        optionBuilder.setKey(IndexOptions.HNSW_METRIC);   // same value, moved to the legacy alias
                        mutated = true;
                    }
                }
            }
        }
        assertThat(mutated).isTrue();

        // The evolved metadata is itself valid, and evolution from v1 to v2 is accepted.
        final RecordMetaData v2 = RecordMetaData.newBuilder().setRecords(v2Proto.build()).build();
        MetaDataEvolutionValidator.getDefaultInstance().validate(v1, v2);
    }

    @Nonnull
    private static RecordMetaData vectorMetaData(@Nonnull final String metricOptionName) {
        final RecordMetaDataBuilder builder =
                RecordMetaData.newBuilder().setRecords(TestRecordsVectorsProto.getDescriptor());
        builder.getRecordType("VectorRecord").setPrimaryKey(concatenateFields("group_id", "rec_no"));
        builder.addIndex("VectorRecord",
                new Index("MetricIndex", new KeyWithValueExpression(field("vector_data"), 0),
                        IndexTypes.VECTOR,
                        ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                                metricOptionName, Metric.EUCLIDEAN_METRIC.name())));
        return builder.build();
    }

    @Test
    void allContainsEveryDeclaredKey() throws IllegalAccessException {
        // Guards VectorIndexOptionKeys.ALL against drift: it must list exactly the VectorOptionKey fields declared on
        // the catalog, so the alias-conflict validation never silently skips a key someone forgot to add.
        final Set<VectorOptionKey<?>> declared = new HashSet<>();
        for (final Field declaredField : VectorIndexOptionKeys.class.getDeclaredFields()) {
            if (Modifier.isStatic(declaredField.getModifiers())
                    && VectorOptionKey.class.isAssignableFrom(declaredField.getType())) {
                declared.add((VectorOptionKey<?>)declaredField.get(null));
            }
        }

        assertThat(VectorIndexOptionKeys.ALL).containsExactlyInAnyOrderElementsOf(declared);
    }

    @Test
    void toStringIsTheCanonicalName() {
        // A key renders as its canonical name, never a non-canonical alias. (The shared keys' canonical name is
        // temporarily the legacy hnsw* name for rolling-upgrade wire compatibility; see VectorIndexOptionKeys.)
        assertThat(VectorIndexOptionKeys.METRIC).hasToString(IndexOptions.HNSW_METRIC);
        assertThat(VectorIndexOptionKeys.HNSW_M).hasToString(IndexOptions.HNSW_M);
        assertThat(VectorOptionKey.ofMetric("vectorMetric", "hnswMetric")).hasToString("vectorMetric");
    }

    @Test
    void readWithDefaultReturnsDefaultOnlyWhenUnset() {
        final Index unset =
                new Index("v", field("vector_data"), IndexTypes.VECTOR,
                        ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128"));
        assertThat(VectorIndexOptionKeys.METRIC.read(unset, Metric.COSINE_METRIC)).isEqualTo(Metric.COSINE_METRIC);

        final Index set =
                new Index("v", field("vector_data"), IndexTypes.VECTOR,
                        ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                                IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name()));
        assertThat(VectorIndexOptionKeys.METRIC.read(set, Metric.COSINE_METRIC)).isEqualTo(Metric.EUCLIDEAN_METRIC);
    }

    @Test
    void putWritesTheCanonicalNameAndNeverALegacyAlias() {
        // Put writes the canonical name only. For the shared keys that canonical is temporarily the legacy hnsw* name
        // (rolling-upgrade wire compatibility; see VectorIndexOptionKeys), so METRIC writes hnswMetric, not vectorMetric.
        final Map<String, String> written = new HashMap<>();
        VectorIndexOptionKeys.METRIC.put(written::put, Metric.EUCLIDEAN_METRIC);
        assertThat(written)
                .hasSize(1)
                .containsEntry(IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name())
                .doesNotContainKey(IndexOptions.VECTOR_METRIC);
    }

    @Test
    void putThenReadRoundTripsEveryValueType() {
        // Exercises each factory's serializer against its parser. The metric case in particular guards the
        // serializer-vs-toString distinction: Metric serializes via name() (not toString()), so it parses back;
        // had put used toString(), the read below would fail Metric.valueOf(...).
        final Map<String, String> written = new HashMap<>();
        VectorIndexOptionKeys.METRIC.put(written::put, Metric.COSINE_METRIC);
        VectorIndexOptionKeys.HNSW_M.put(written::put, 24);
        VectorIndexOptionKeys.MAINTAIN_STATS_PROBABILITY.put(written::put, 0.25);
        VectorIndexOptionKeys.USE_RABITQ.put(written::put, true);

        final Index index =
                new Index("v", field("vector_data"), IndexTypes.VECTOR, ImmutableMap.copyOf(written));
        assertThat(VectorIndexOptionKeys.METRIC.read(index)).isEqualTo(Metric.COSINE_METRIC);
        assertThat(VectorIndexOptionKeys.HNSW_M.read(index)).isEqualTo(24);
        assertThat(VectorIndexOptionKeys.MAINTAIN_STATS_PROBABILITY.read(index)).isEqualTo(0.25);
        assertThat(VectorIndexOptionKeys.USE_RABITQ.read(index)).isEqualTo(true);
    }

    @Test
    void equalsHashCodeAndPlanHashKeyOnlyTheCanonicalName() {
        // Two keys with the same canonical name (regardless of aliases) are interchangeable — equal, same hashCode,
        // and same planHash — so a scan-options map keyed by them is stable; a different canonical name differs.
        final VectorOptionKey<Metric> withAlias = VectorOptionKey.ofMetric("vectorMetric", "hnswMetric");
        final VectorOptionKey<Metric> withoutAlias = VectorOptionKey.ofMetric("vectorMetric");
        final VectorOptionKey<Integer> other = VectorOptionKey.ofInteger("hnswM");

        assertThat(withAlias).isEqualTo(withoutAlias).hasSameHashCodeAs(withoutAlias);
        assertThat(withAlias.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isEqualTo(withoutAlias.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

        assertThat(withAlias).isNotEqualTo(other);
        assertThat(withAlias.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isNotEqualTo(other.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }
}
