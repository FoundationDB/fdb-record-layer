/*
 * OrderedBytesHelpers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PDirection;
import com.apple.foundationdb.tuple.TupleOrdering.Direction;

import javax.annotation.Nonnull;

/**
 * A value that produces a binary encoding that is comparable according to certain modes gives by
 * {@link Direction}.
 */
@API(API.Status.EXPERIMENTAL)
public class OrderedBytesHelpers {
    private OrderedBytesHelpers() {
        // do nothing and prevent instantiation
    }

    @Nonnull
    public static PDirection toDirectionProto(@Nonnull final Direction direction) {
        switch (direction) {
            case ASC_NULLS_FIRST:
                return PDirection.ASC_NULLS_FIRST;
            case ASC_NULLS_LAST:
                return PDirection.ASC_NULLS_LAST;
            case DESC_NULLS_FIRST:
                return PDirection.DESC_NULLS_FIRST;
            case DESC_NULLS_LAST:
                return PDirection.DESC_NULLS_LAST;
            default:
                throw new RecordCoreException("unable to find direction mapping. did you forgot to add it here?");
        }
    }

    @Nonnull
    public static Direction fromDirectionProto(@Nonnull final PDirection directionProto) {
        switch (directionProto) {
            case ASC_NULLS_FIRST:
                return Direction.ASC_NULLS_FIRST;
            case ASC_NULLS_LAST:
                return Direction.ASC_NULLS_LAST;
            case DESC_NULLS_FIRST:
                return Direction.DESC_NULLS_FIRST;
            case DESC_NULLS_LAST:
                return Direction.DESC_NULLS_LAST;

            default:
                throw new RecordCoreException("unable to find direction proto mapping");
        }
    }
}
