/*
 * IndexScanType.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * The way in which an index should be scanned.
 *
 * The set of allowed scan types varies by the type of the index.
 * This isn't an enum so clients can define more of them for their own index maintainers.
 *
 * @see com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer#scan
 */
@API(API.Status.MAINTAINED)
public class IndexScanType implements PlanHashable {
    @Nonnull
    public static final IndexScanType BY_VALUE = new IndexScanType("BY_VALUE");
    @Nonnull
    public static final IndexScanType BY_RANK = new IndexScanType("BY_RANK");
    @Nonnull
    public static final IndexScanType BY_GROUP = new IndexScanType("BY_GROUP");
    @Nonnull
    public static final IndexScanType BY_TIME_WINDOW = new IndexScanType("BY_TIME_WINDOW");
    @Nonnull
    public static final IndexScanType BY_TEXT_TOKEN = new IndexScanType("BY_TEXT_TOKEN");
    @Nonnull
    public static final IndexScanType BY_LUCENE = new IndexScanType("BY_LUCENE");

    private final String name;

    public IndexScanType(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexScanType that = (IndexScanType) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return hashCode();
    }
}
