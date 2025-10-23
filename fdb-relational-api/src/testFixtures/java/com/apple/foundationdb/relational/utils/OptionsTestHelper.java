/*
 * RelationalAssert.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.utils;

import com.apple.foundationdb.relational.api.Options;
import org.assertj.core.api.Assertions;

import java.sql.SQLException;
import java.util.List;

/**
 * Test fixtures related to {@link Options}.
 */
public class OptionsTestHelper {

    public static Options nonDefaultOptions() throws SQLException {
        Options.Builder builder = Options.builder();
        // Cannot do CONTINUATION, since the Impl class is in -core and not -api.
        builder = builder.withOption(Options.Name.INDEX_HINT, "thisIndex");
        builder = builder.withOption(Options.Name.MAX_ROWS, 5);
        builder = builder.withOption(Options.Name.REQUIRED_METADATA_TABLE_VERSION, 123);
        builder = builder.withOption(Options.Name.TRANSACTION_TIMEOUT, 5000L);
        builder = builder.withOption(Options.Name.INDEX_FETCH_METHOD, Options.IndexFetchMethod.USE_REMOTE_FETCH);
        builder = builder.withOption(Options.Name.DISABLE_PLANNER_REWRITING, true);
        builder = builder.withOption(Options.Name.DISABLED_PLANNER_RULES, List.of("r1", "r2"));
        builder = builder.withOption(Options.Name.PLAN_CACHE_PRIMARY_MAX_ENTRIES, 512);
        builder = builder.withOption(Options.Name.PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS, 1000L);
        builder = builder.withOption(Options.Name.PLAN_CACHE_SECONDARY_MAX_ENTRIES, 128);
        builder = builder.withOption(Options.Name.PLAN_CACHE_SECONDARY_TIME_TO_LIVE_MILLIS, 3000L);
        builder = builder.withOption(Options.Name.PLAN_CACHE_TERTIARY_MAX_ENTRIES, 4);
        builder = builder.withOption(Options.Name.PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS, 2000L);
        builder = builder.withOption(Options.Name.REPLACE_ON_DUPLICATE_PK, true);
        builder = builder.withOption(Options.Name.LOG_QUERY, true);
        builder = builder.withOption(Options.Name.LOG_SLOW_QUERY_THRESHOLD_MICROS, 1000000L);
        builder = builder.withOption(Options.Name.EXECUTION_SCANNED_BYTES_LIMIT, 500000L);
        builder = builder.withOption(Options.Name.EXECUTION_TIME_LIMIT, 100L);
        builder = builder.withOption(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, 4000);
        builder = builder.withOption(Options.Name.DRY_RUN, true);
        builder = builder.withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true);
        builder = builder.withOption(Options.Name.CURRENT_PLAN_HASH_MODE, "m1");
        builder = builder.withOption(Options.Name.VALID_PLAN_HASH_MODES, "m1,m2");
        builder = builder.withOption(Options.Name.ASYNC_OPERATIONS_TIMEOUT_MILLIS, 5000L);
        builder = builder.withOption(Options.Name.ENCRYPT_WHEN_SERIALIZING, true);
        builder = builder.withOption(Options.Name.ENCRYPTION_KEY_STORE, "secrets.ks");
        builder = builder.withOption(Options.Name.ENCRYPTION_KEY_ENTRY, "mykey");
        builder = builder.withOption(Options.Name.ENCRYPTION_KEY_ENTRY_LIST, List.of("mykey", "anotherkey"));
        builder = builder.withOption(Options.Name.ENCRYPTION_KEY_PASSWORD, "mypass");
        builder = builder.withOption(Options.Name.COMPRESS_WHEN_SERIALIZING, false);
        Options options = builder.build();
        for (Options.Name name : Options.Name.values()) {
            if (name != Options.Name.CONTINUATION) {    // See above on why CONTINUATION was skipped.
                Object value = options.getOption(name);
                Assertions.assertThat(value).as(() -> "non default for " + name.name()).isNotEqualTo(Options.defaultOptions().get(name));
            }
        }
        return options;
    }
}
