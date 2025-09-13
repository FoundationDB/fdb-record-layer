/*
 * NoOpMetricRegistry.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.metrics;

import com.apple.foundationdb.annotation.API;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.NoopMetricRegistry;

import javax.annotation.Nonnull;

/**
 * A utility for accessing a no-op {@link MetricRegistry}.
 * <p>
 * NOTE(stack): Consider NOT using codahale but prometheus metrics. If server is exporting metrics
 * on a prometheus endpoint, codahale will require translation (there are translators but better not
 * to translate at all). What is the story for clients? RL is codahale?
 */
@API(API.Status.EXPERIMENTAL)
public final class NoOpMetricRegistry {

    @Nonnull
    public static final MetricRegistry INSTANCE = new NoopMetricRegistry();

    private NoOpMetricRegistry() {
    }
}
