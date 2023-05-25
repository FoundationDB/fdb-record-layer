/*
 * QueryLogger.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.util.DistinctSampler;
import com.apple.foundationdb.relational.util.Sampler;
import com.apple.foundationdb.relational.util.Sampling;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

public final class QueryLogger {
    private static final Logger logger = LogManager.getLogger(QueryLogger.class);
    private static volatile QueryLogger instance;

    @Nullable
    private final DistinctSampler<Plan<?>> planSampler;

    @Nonnull
    private final Sampler totalSampler;

    private QueryLogger(int maxSampleCacheSize, int maxSamplesPerUnit, TimeUnit timeUnit) {
        this.totalSampler = Sampling.newSampler(maxSamplesPerUnit, timeUnit);
        if (maxSampleCacheSize <= 0) {
            //distinct sampling is disabled, so defer instead to global sampling.
            // warning: Global query sampling has a tendency of losing rare, but potentially expensive,
            // queries!
            this.planSampler = null;
        } else {
            this.planSampler = new DistinctSampler<>(maxSampleCacheSize, () -> Sampling.newSampler(maxSamplesPerUnit, timeUnit));
        }
    }

    public static QueryLogger instance() {
        QueryLogger instance = QueryLogger.instance;
        if (instance == null) {
            throw new IllegalStateException("QueryLogger has not been configured!");
        }
        return instance;
    }

    public static void configure(Options options) {
        if (instance != null) {
            return; //don't configure it twice
        }
        //double-checked locking here, just for safety
        synchronized (QueryLogger.class) {
            if (instance != null) {
                throw new IllegalStateException("Query Logger has already been configured!");
            }
            int maxSamplesPerUnit = options.getOption(Options.Name.MAX_QUERY_LOGGING_THROUGHPUT);
            TimeUnit timeUnit = options.getOption(Options.Name.SAMPLING_TIME_UNIT);
            int maxCacheSize = options.getOption(Options.Name.MAX_QUERY_LOGGING_CACHE_SIZE);
            instance = new QueryLogger(maxCacheSize, maxSamplesPerUnit, timeUnit);
        }
    }

    void logPlan(@Nullable Plan<?> plan, String queryString, long queryPlanTime) {
        if (plan != null && shouldLog(plan)) {
            //TODO(bfines) add more information to this
            KeyValueLogMessage message = KeyValueLogMessage.build("Planned Query")
                    .addKeyAndValue("plan", plan.toString());
            if (plan instanceof QueryPlan.LogicalQueryPlan) {
                message.addKeyAndValue("queryString", queryString);
            }
            message.addKeyAndValue("queryPlanTime", Long.toString(queryPlanTime));
            logger.info(message.toString());
        }
    }

    private boolean shouldLog(@Nonnull Plan<?> plan) {
        if (planSampler == null) {
            //defer to the global sampler
            return totalSampler.canSample();
        } else {
            return planSampler.test(plan);
        }
    }

    public void logPlanError(String query, Throwable t) {
        logger.error(query, t);
    }
}
