/*
 * ExplainLevel.java
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

package com.apple.foundationdb.record.query.plan.explain;

/**
 * Explain level. Adjusts the granularity of explain output.
 */
public final class ExplainLevel {
    /**
     * Everything we can render is rendered.
     */
    public static final int ALL_DETAILS = 0;
    /**
     * Nice to have details are rendered, other details not absolutely needed for the understanding of the plan
     * are omitted.
     */
    public static final int SOME_DETAILS = 1;
    /**
     * Only the structure of the plan, i.e. the plan operators and their relationship is rendered.
     */
    public static final int STRUCTURE = 2;

    private ExplainLevel() {
        // nothing
    }
}
