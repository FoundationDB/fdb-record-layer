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
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;

import org.apache.logging.log4j.Logger;

public class RelationalLoggingUtil {
    public static void publishPlanGenerationLogs(Logger logger, KeyValueLogMessage message, Plan<?> plan, long totalTime, Options options) {
        final boolean logQuery = options.getOption(Options.Name.LOG_QUERY);
        final boolean isSlow = totalTime > (long) options.getOption(Options.Name.LOG_SLOW_QUERY_THRESHOLD_MICROS);

        if (logQuery || logger.isDebugEnabled() || isSlow) {
            if (plan instanceof QueryPlan.PhysicalQueryPlan) {
                final var planHash = ((QueryPlan.PhysicalQueryPlan) plan).planHash();
                message.addKeyAndValue("planHash", planHash);
            }
            message.addKeyAndValue("plan", plan.explain());
            message.addKeyAndValue("totalPlanTime", totalTime);
            message.addKeyAndValue("query", plan.getQuery().trim());
        }
        if (logQuery || isSlow) {
            logger.info(message);
        } else if (logger.isDebugEnabled()) {
            logger.debug(message);
        }
    }

    public static void publishNormalizeQueryLogs(Logger logger, KeyValueLogMessage message, long stepTime, int queryHash) {
        message.addKeyAndValue("normalizeQueryTime", stepTime);
        message.addKeyAndValue("queryHash", queryHash);
    }

    public static void publishPlanCacheLogs(Logger logger, KeyValueLogMessage message, PlanCacheEvent event, long stepTime, boolean logQuery) {
        switch (event) {
            case SKIP:
                if (logQuery || logger.isDebugEnabled()) {
                    message.addKeyAndValue("planCache", "skip");
                    message.addKeyAndValue("generatePhysicalPlanTime", stepTime);
                }
                break;
            case HIT:
                if (logQuery || logger.isDebugEnabled()) {
                    message.addKeyAndValue("planCache", "hit");
                }
                break;
            case MISS:
                message.addKeyAndValue("planCache", "miss");
                message.addKeyAndValue("generatePhysicalPlanTime", stepTime);
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
