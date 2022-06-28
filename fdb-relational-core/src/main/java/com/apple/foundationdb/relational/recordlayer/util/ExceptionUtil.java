/*
 * ExceptionUtil.java
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

package com.apple.foundationdb.relational.recordlayer.util;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.sql.SQLException;
import java.util.Map;

public final class ExceptionUtil {
    public static RelationalException toRelationalException(Throwable re) {
        if (re instanceof RelationalException) {
            return (RelationalException) re;
        } else if (re instanceof SQLException) {
            return new RelationalException(re.getMessage(), ErrorCode.get(((SQLException) re).getSQLState()), re);
        } else if (re instanceof RecordCoreException) {
            return recordCoreToRelationalException((RecordCoreException) re);
        }
        return new RelationalException(ErrorCode.UNKNOWN, re);
    }

    private static RelationalException recordCoreToRelationalException(RecordCoreException re) {
        ErrorCode code = ErrorCode.UNKNOWN;
        if (re instanceof RecordCoreStorageException) {
            code = ErrorCode.TRANSACTION_INACTIVE;
        }
        Map<String, Object> extraContext = re.getLogInfo();
        return new RelationalException(code, re).withContext(extraContext);
    }

    private ExceptionUtil() {
    }
}
