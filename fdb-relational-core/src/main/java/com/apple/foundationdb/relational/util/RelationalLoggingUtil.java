/*
 * RelationalLoggingUtil.java
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

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;

import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;

public class RelationalLoggingUtil {
    public static void publishPlanGenerationLogs(Logger logger, KeyValueLogMessage message, @Nullable Plan<?> plan,
                                                 @Nullable RelationalException e, long totalTime, Options options) {
        final boolean logQuery = options.getOption(Options.Name.LOG_QUERY);
        final boolean isSlow = totalTime > (long) options.getOption(Options.Name.LOG_SLOW_QUERY_THRESHOLD_MICROS);
        message.addKeyAndValue("totalPlanTimeMicros", totalTime);
        if (plan != null) {
            if (plan instanceof QueryPlan.PhysicalQueryPlan) {
                final var planHash = ((QueryPlan.PhysicalQueryPlan) plan).planHash();
                message.addKeyAndValue("planHash", planHash);
            }
            message.addKeyAndValue("plan", plan.explain());
        }
        if (e != null) {
            logger.error(message, e);
        } else if (logQuery || isSlow) {
            logger.info(message);
        } else if (logger.isDebugEnabled()) {
            logger.debug(message);
        }
    }

    public static void publishNormalizeQueryLogs(KeyValueLogMessage message, long stepTime, int queryHash, String query) {
        message.addKeyAndValue("queryHash", queryHash);
        message.addKeyAndValue("query", query.trim());
        message.addKeyAndValue("normalizeQueryTimeMicros", stepTime);
    }

    public static void publishPlanCacheLogs(KeyValueLogMessage message, PlanCacheEvent event, long stepTime, long primaryCacheNumEntries) {
        switch (event) {
            case SKIP:
                message.addKeyAndValue("planCache", "skip");
                message.addKeyAndValue("generatePhysicalPlanTimeMicros", stepTime);
                break;
            case HIT:
                message.addKeyAndValue("planCache", "hit");
                message.addKeyAndValue("primaryCacheNumEntries", primaryCacheNumEntries);
                break;
            case MISS:
                message.addKeyAndValue("planCache", "miss");
                message.addKeyAndValue("generatePhysicalPlanTimeMicros", stepTime);
                message.addKeyAndValue("primaryCacheNumEntries", primaryCacheNumEntries);
                break;
            default:
                break;
        }
    }

    public enum PlanCacheEvent {
        SKIP,
        HIT,
        MISS,
    }
}
