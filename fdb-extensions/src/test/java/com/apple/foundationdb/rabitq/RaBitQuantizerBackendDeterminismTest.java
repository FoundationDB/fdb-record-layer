/*
 * RaBitQuantizerBackendDeterminismTest.java
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

package com.apple.foundationdb.rabitq;

import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.test.Tags;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HexFormat;
import java.util.Random;
import java.util.stream.Stream;

import static com.apple.foundationdb.linear.RealVectorTest.createRandomDoubleVector;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the RaBitQ wire format to a golden, machine-independent byte string, proving that encoding
 * does not depend on which vector backend ({@link com.apple.foundationdb.linear.Backend}, i.e.
 * scalar vs SIMD) happens to be active.
 * <p>
 * This is the end-to-end guard behind the {@code …Exact} reductions in {@link RaBitQuantizer}: the
 * three calibration constants ({@code fAddEx}/{@code fRescaleEx}/{@code fErrorEx}) and the
 * per-dimension quantized codes are serialized verbatim and hashed into a content signature used
 * for duplicate detection. If any of the underlying reductions leaked through the ambient SIMD
 * backend, the bytes here would differ between a SIMD host and a scalar host. Because
 * {@link Tag @Tag}({@link Tags#DualScalarSIMD}) runs this class under both the SIMD {@code test}
 * task and the scalar {@code scalarFallbackTest} task, both runs must reproduce the <em>same</em>
 * golden — which is exactly the cross-backend invariant we want.
 * <p>
 * The golden hexes were captured once from an encoder run; the exact reductions use plain IEEE-754
 * arithmetic with a fixed accumulation order, so they are stable across CPUs and JVMs. If a
 * deliberate change to the encoding algorithm breaks these, re-capture the expected value from the
 * assertion failure's reported {@code actual} hex — but a break that appears only under one backend
 * is the regression this test is here to catch.
 */
@Tag(Tags.DualScalarSIMD)
class RaBitQuantizerBackendDeterminismTest {
    private static Stream<Arguments> goldenCases() {
        return Stream.of(
                // seed, numDimensions, numExBits, goldenHex
                Arguments.of(0xA5A5A5A5L, 3, 4,
                        "033feacbe8c6a7d71cbfb8456669ba316e3fa59d6e022f73e9cfd2"),
                Arguments.of(0xBEEFL, 128, 6,
                        "034043f02ef98efb1fbfa134736459560b3f908c0d8bd9e6a146d4f144d28fcd569afdd9b10f5c5c"
                                + "c54eee3e8c21e2d17a667671afca2e629593d84968f77a7daaaf2c8241aae0fb8a776e58715bf5b0"
                                + "2c6ddcda58cb65731e1e335d641ee922f49eaea607e4c6ebd54257733d18c9db465aa6232f505a6d"
                                + "0fccb555a05f8599856ab14e14bb533d49"),
                Arguments.of(0xC0FFEEL, 768, 8,
                        "03406e990a77337e1ebf80372611b6ee7c3f713dafab9c99eee9bd173bd53a544268461c353938a5"
                                + "1db70bfbf745306333950792be0817e91802369d88efd96d9f06e74dde0d0a44395eeacd1c61cc6f"
                                + "4e3ece3eaf7720b8397f39fbd2ec31c6a48d607049f4efb6cf05079e74c9c892959947b4db1474b2"
                                + "72fefb0106e5c9cd2de879b694d4bfac4b3b446185ad570e4593b2acf168d518ce38a48c3f2d2e06"
                                + "ee9635ab581248e4d8344f42a20f866e1e280ca214cd89e4a04dbae040ea7a54210c1a34c13f4841"
                                + "f9391912fe746bb2c9931e6d7afb758f3552ceb2fc0f9eb7e71ed288948aa5c49c70682957416dea"
                                + "15600ad48a609da3c352eaa091d701558629d7a5d290d89802a2297575c20cc0526b5b8030d44fd1"
                                + "a084b054336859b351947c87486d8e71e721a21f4509723b3cd13b95d132b6b889b2b7107c09d9ac"
                                + "e45be6ed2a0f080a6d570b626b16545aff213ccc41774346b8c5882b7915442d749f7ae2593eb669"
                                + "5429f10d974d74b749bc542d4b82e3a840a0928703f61b2250f7b3852e9e274a344e75282a10adbc"
                                + "3e4e330d990c414f9af62773a4dff16b654712d99457ea64fd9025c640de85869e348aa8e251a615"
                                + "0d623285f5535c2fbbce154ee22e1db92b2360613a8e6ef429f289d2170159c8a428ee9d46eb26dd"
                                + "3373225e2ed4c76b6685133dde4116ba882b28edd577f57aa81a945046f94e6902d1a524761e684d"
                                + "c37e782b6650a2e7ea98e118031491edd249c026ea54f3f34c263e4962b3ba53f4054928d04a8f24"
                                + "cd63e53ba9a86e7ad7e25e03e0b2523675ec8b8f88d59c3ed2515493c3c70885116b5da0ead1f320"
                                + "8aa0d4dffb2af75877851390c51946901b3fba05109f3adc70821a5722dc60d2c8dd502b7b9cc872"
                                + "ad371a9c34a32b7e972f578024deae5f544f4f4558ff53e74958b0eb254e75af11436adc08b84c85"
                                + "cbdb021c057f7bd19598391f31268d1f89076472c416cd458748f025fcbcb3cbb8ec550d7ea07c9a"
                                + "520b511ae1369eb9dc36acc4e4acbf7ad82555f8fa93ac80ab49cce0a781cfa9cae16c6b14831d3f"
                                + "5d4b0d2e720ac2aa12dd2fbd22418786ff8e6bd0d4d133233706b634e458be77ba8e42a59788fd4e"
                                + "3cf6d03458a2203befeb4aba5fc155880fac791b2abb9e5c0c4377c9d44f2be549466886af7a0097"
                                + "38a1c857b0e134911b2dd069aeac1d18f8ef8ca5e3eeaa7118a4b68286b3d64b8460441633d2691e"
                                + "60cfad3229dd44f040"));
    }

    @ParameterizedTest(name = "seed={0}, dims={1}, numExBits={2}")
    @MethodSource("goldenCases")
    @DisplayName("RaBitQ encoded bytes are identical across scalar and SIMD backends")
    void encodedBytesAreBackendIndependent(final long seed, final int numDimensions, final int numExBits,
                                           final String goldenHex) {
        final RealVector v = createRandomDoubleVector(new Random(seed), numDimensions);
        final RaBitQuantizer quantizer = new RaBitQuantizer(Metric.EUCLIDEAN_SQUARE_METRIC, numExBits);

        final byte[] rawData = quantizer.encode(v).getRawData();
        final String actualHex = HexFormat.of().formatHex(rawData);

        assertThat(actualHex)
                .as("seed=%d, dims=%d, numExBits=%d", seed, numDimensions, numExBits)
                .isEqualTo(goldenHex);
    }
}
