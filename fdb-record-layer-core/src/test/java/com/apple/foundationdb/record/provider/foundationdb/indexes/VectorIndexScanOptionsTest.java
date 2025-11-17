/*
 * VectorIndexScanOptionsTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PVectorIndexScanOptions;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions.Builder;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorIndexScanOptionsTest {
    @Test
    void builderRoundTripTest() {
        final Builder builder = VectorIndexScanOptions.builder();
        final VectorIndexScanOptions optionsOriginal = builder.build();
        assertThat(optionsOriginal.containsOption(VectorIndexScanOptions.HNSW_EF_SEARCH)).isFalse();

        Builder resultBuilder = builder.putOption(VectorIndexScanOptions.HNSW_EF_SEARCH, 10);
        assertThat(resultBuilder).isSameAs(builder);

        final VectorIndexScanOptions optionsAfterPut = builder.build();
        assertThat(optionsAfterPut.containsOption(VectorIndexScanOptions.HNSW_EF_SEARCH)).isTrue();
        assertThat(optionsAfterPut).isNotEqualTo(optionsOriginal);

        assertThat(optionsAfterPut.toBuilder().build()).isEqualTo(optionsAfterPut);

        resultBuilder = builder.removeOption(VectorIndexScanOptions.HNSW_EF_SEARCH);
        assertThat(resultBuilder).isSameAs(builder);

        final VectorIndexScanOptions optionsAfterRemove = builder.build();
        assertThat(optionsOriginal.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isEqualTo(optionsAfterRemove.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        assertThat(optionsAfterRemove).isEqualTo(optionsOriginal);
    }

    @Test
    void builderEqualityTest() {
        final Builder builder1 = VectorIndexScanOptions.builder();
        final Builder builder2 = VectorIndexScanOptions.builder();

        assertThat(builder1).isEqualTo(builder2);
        builder1.putOption(VectorIndexScanOptions.HNSW_EF_SEARCH, 10);
        assertThat(builder1).isNotEqualTo(builder2);
        builder2.putOption(VectorIndexScanOptions.HNSW_EF_SEARCH, 10);
        assertThat(builder1).hasSameHashCodeAs(builder2);
        assertThat(builder1).isEqualTo(builder2);
        builder1.removeOption(VectorIndexScanOptions.HNSW_EF_SEARCH);
        builder2.removeOption(VectorIndexScanOptions.HNSW_EF_SEARCH);
        assertThat(builder1).hasSameHashCodeAs(builder2);
        assertThat(builder1).isEqualTo(builder2);

        builder1.putOption(VectorIndexScanOptions.HNSW_EF_SEARCH, 10);
        builder2.putOption(VectorIndexScanOptions.HNSW_EF_SEARCH, 20);
        assertThat(builder1).isNotEqualTo(builder2);

        assertThat((VectorIndexScanOptions.OptionKey<?>)VectorIndexScanOptions.HNSW_EF_SEARCH)
                .isNotEqualTo(VectorIndexScanOptions.HNSW_RETURN_VECTORS);
    }

    @Test
    void protoRoundTripTest() {
        final Builder builder = VectorIndexScanOptions.builder();

        final VectorIndexScanOptions options =
                builder.putOption(VectorIndexScanOptions.HNSW_EF_SEARCH, 10)
                        .build();

        final PVectorIndexScanOptions proto =
                options.toProto(PlanSerializationContext.newForCurrentMode());

        final VectorIndexScanOptions.Deserializer deserializer = new VectorIndexScanOptions.Deserializer();
        final VectorIndexScanOptions optionsAfterRoundTrip =
                deserializer.fromProto(PlanSerializationContext.newForCurrentMode(), proto);

        assertThat(optionsAfterRoundTrip.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isEqualTo(options.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        assertThat(optionsAfterRoundTrip).hasSameHashCodeAs(options);
        assertThat(optionsAfterRoundTrip).isEqualTo(options);
    }

    @Test
    void explainTest() {
        final VectorIndexScanOptions options1 =
                VectorIndexScanOptions.builder()
                        .putOption(VectorIndexScanOptions.HNSW_EF_SEARCH, 100)
                        .build();
        final String explain1 = renderExplain(options1);

        final VectorIndexScanOptions options2 =
                VectorIndexScanOptions.builder()
                        .putOption(VectorIndexScanOptions.HNSW_EF_SEARCH, 200)
                        .build();
        assertThat(options1).doesNotHaveToString(options2.toString());
        final String explain2 = renderExplain(options2);
        assertThat(explain1).isNotEqualTo(explain2);

        final VectorIndexScanOptions options3 =
                options2.toBuilder()
                        .putOption(VectorIndexScanOptions.HNSW_EF_SEARCH, 100)
                        .build();
        assertThat(options3).hasToString(options1.toString());
        final String explain3 = renderExplain(options3);
        assertThat(explain3).isEqualTo(explain1);
    }

    @Nonnull
    private static String renderExplain(@Nonnull final VectorIndexScanOptions options) {
        return options.explain()
                .getExplainTokens()
                .render(DefaultExplainFormatter.forDebugging())
                .toString();
    }
}
