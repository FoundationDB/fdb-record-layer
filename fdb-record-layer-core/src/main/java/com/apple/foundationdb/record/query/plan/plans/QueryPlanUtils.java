/*
 * QueryPlanUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * Utility class for query planning.
 */
public class QueryPlanUtils {
    private QueryPlanUtils() {
    }

    /**
     * Some plans require serializable isolation (such as DML); this should be enforced by the parser, but as a backup
     * this method can be used to additionally protect.
     * <p>
     *     For example, data-modification plans must run at serializable isolation: they read existing records (to
     *     maintain indexes, enforce uniqueness, etc.) and those reads must participate in conflict detection to remain
     *     correct. Executing at SNAPSHOT isolation would, at a minimum, require adding any records read to the conflict
     *     range to ensure index consistency. It also requires determining and documenting the exact semantics. Because
     *     of this complexity and a lack of immediate requests, this is not supported.
     * </p>
     * @param executeProperties the execute properties used for executing
     * @param planClass the class of the plan being protected
     */
    @API(API.Status.INTERNAL)
    static void enforceSerializable(@Nonnull final ExecuteProperties executeProperties,
                                    final Class<?> planClass) {
        if (executeProperties.getIsolationLevel() != IsolationLevel.SERIALIZABLE) {
            throw new RecordCoreArgumentException("Cannot execute plan at SNAPSHOT isolation level")
                    .addLogInfo(LogMessageKeys.PLAN, planClass.getSimpleName());
        }
    }
}
