/*
 * CascadesPlannerConfiguration.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

/**
 * Configuration class for {@link CascadesPlanner}.
 */
public class CascadesPlannerConfiguration {
    private int maxTaskQueueSize;
    private int maxTotalTaskCount;

    public CascadesPlannerConfiguration(final int maxTaskQueueSize, final int maxTotalTaskCount) {
        this.maxTaskQueueSize = maxTaskQueueSize;
        this.maxTotalTaskCount = maxTotalTaskCount;
    }

    public int getMaxTaskQueueSize() {
        return maxTaskQueueSize;
    }

    public int getMaxTotalTaskCount() {
        return maxTotalTaskCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for {@link CascadesPlannerConfiguration}.
     */
    public static class Builder {
        private int maxTaskQueueSize = 0;
        private int maxTotalTaskCount = 0;

        /**
         * Set the maximum size of the cascades planner task queue.
         * @param maxTaskQueueSize The maximum size of the queue. 0 means "unbound" (the default). Trying to add a task beyond the
         * maximum size will fail the planning.
         * @return the builder
         */
        public Builder withMaxTaskQueueSize(int maxTaskQueueSize) {
            this.maxTaskQueueSize = maxTaskQueueSize;
            return this;
        }

        /**
         * Set the maximum number of tasks that can be executed as part of the cascades planner planning.
         * @param maxTotalTaskCount The maximum number of tasks. 0 means "unbound" (the default). Trying to execute a task after
         * the maximum number was exceeded will fail the planning.
         * @return the builder
         */
        public Builder withMaxTotalTaskCount(int maxTotalTaskCount) {
            this.maxTotalTaskCount = maxTotalTaskCount;
            return this;
        }

        /**
         * Build the configuration with the given parameters.
         * @return teh constructed configuration
         */
        public CascadesPlannerConfiguration build() {
            return new CascadesPlannerConfiguration(maxTaskQueueSize, maxTotalTaskCount);
        }
    }
}
