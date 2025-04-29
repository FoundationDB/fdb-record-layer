/*
 * EventSamplerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.util;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class EventSamplerTest {

    @Test
    void correctlyAllowsFirstCountThrough() {
        Sampler sampler = new EventSampler(1, 1);

        Assertions.assertThat(sampler.canSample()).isTrue();
    }

    @Test
    void correctlyForbidsWithinTimeFrame() {
        Sampler sampler = new EventSampler(1, 3);

        //if two come in simultaneously, then one should fail
        Assertions.assertThat(sampler.canSample()).isTrue();
        Assertions.assertThat(sampler.canSample()).isFalse();
    }

    @Test
    void conformsToAverageRate() {
        //over time, the number of samples should be returned at a rate of maxTokens/refreshIntervalNanos
        //this test checks that it allows through about 1/second. It will allow a little bit more
        //because the initial burstiness allowed by the tokens

        Sampler sampler = new EventSampler(1, 10);

        //advance at the rate of about 10/second
        int numTasks = 100; //do about 10 cycles
        int numTrues = 0;
        for (int i = 0; i < numTasks; i++) {
            if (sampler.canSample()) {
                numTrues++;
            }
        }

        //we should see 11 total trues here--2 at the beginning, and 1 for the 9 subsequent refreshes
        Assertions.assertThat(numTrues).isEqualTo(11);
    }
}
