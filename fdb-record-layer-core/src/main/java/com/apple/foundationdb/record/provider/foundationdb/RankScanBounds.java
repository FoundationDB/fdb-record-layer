/*
 * RankScanBounds.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.TupleRange;

import javax.annotation.Nonnull;

/**
 * {@link IndexScanBounds} for a scan of a rank index.
 * <p>
 * Only {@link IndexScanType#BY_VALUE} and {@link IndexScanType#BY_RANK} are accepted, those being the two traversals a
 * rank index supports: by value the range is over scores, by rank it is over ranks and is converted to a score range
 * before the underlying scan happens. Either way the entries are the same, so {@code includeRankAsValue} is what asks
 * for the rank to be reported rather than merely used as a bound. A rank index leaves the entry's value empty, so that
 * is where the rank can go.
 * </p>
 *
 * @param indexScanType whether the range is over scores ({@code BY_VALUE}) or over ranks ({@code BY_RANK})
 * @param rankRange the bounds of the scan, prefixed by the group
 * @param includeRankAsValue whether the rank of each entry should be reported in the entry's value. This can be
 * expensive: a rank is not stored alongside the entry but looked up, so each entry returned costs a traversal of the
 * ranked-set skip list held in the index's secondary subspace. The cost is therefore borne per entry and grows with the
 * number of entries the scan returns, not with the size of the range asked for
 */
@API(API.Status.UNSTABLE)
public record RankScanBounds(@Nonnull IndexScanType indexScanType,
                             @Nonnull TupleRange rankRange,
                             boolean includeRankAsValue) implements IndexScanBounds {

    public RankScanBounds {
        if (!IndexScanType.BY_VALUE.equals(indexScanType) && !IndexScanType.BY_RANK.equals(indexScanType)) {
            throw new RecordCoreArgumentException("a rank index can only be scanned by value or by rank",
                    "scanType", indexScanType);
        }
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return indexScanType;
    }
}
