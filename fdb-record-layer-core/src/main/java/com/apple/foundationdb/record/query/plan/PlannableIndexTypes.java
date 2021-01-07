/*
 * PlannableIndexTypes.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A wrapper class to identify groups of that behave the same way from the perspective of the planner.
 * For example, index types should generally support implementing the same types of filters, sort, and aggregates.
 * A {@code PlannableIndexTypes} object can be passed to a {@link RecordQueryPlanner} when it is initialized so that
 * users of the Record Layer can define their own index types.
 *
 * This is a bit of a hack, and we should really fix it in the future.
 * TODO: Define index "behaviors" with defined contracts, for use in planning and index maintenance (https://github.com/FoundationDB/fdb-record-layer/issues/16)
 *
 * @see IndexTypes
 */
@API(API.Status.EXPERIMENTAL)
public class PlannableIndexTypes {
    public static final PlannableIndexTypes DEFAULT = new PlannableIndexTypes(
            Sets.newHashSet(IndexTypes.VALUE, IndexTypes.VERSION),
            Sets.newHashSet(IndexTypes.RANK, IndexTypes.TIME_WINDOW_LEADERBOARD),
            Sets.newHashSet(IndexTypes.TEXT),
            Sets.newHashSet(IndexTypes.FULL_TEXT)
            );

    @Nonnull
    private final Set<String> valueTypes;
    @Nonnull
    private final Set<String> rankTypes;
    @Nonnull
    private final Set<String> textTypes;
    @Nonnull
    private final Set<String> luceneTypes;


    // TODO extend with more in the future?

    public PlannableIndexTypes(@Nonnull Set<String> valueTypes,
                               @Nonnull Set<String> rankTypes,
                               @Nonnull Set<String> textTypes,
                               @Nonnull Set<String> luceneTypes) {
        this.valueTypes = valueTypes;
        this.rankTypes = rankTypes;
        this.textTypes = textTypes;
        this.luceneTypes = luceneTypes;
    }

    @Nonnull
    public Set<String> getValueTypes() {
        return valueTypes;
    }

    @Nonnull
    public Set<String> getRankTypes() {
        return rankTypes;
    }

    @Nonnull
    public Set<String> getTextTypes() {
        return textTypes;
    }

    @Nonnull
    public Set<String> getLuceneTypes() {
        return luceneTypes;
    }

}
