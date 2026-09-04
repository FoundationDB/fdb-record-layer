/*
 * RecordMetaDataStoredQueryTest.java
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

package com.apple.foundationdb.record;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RecordMetaDataStoredQueryTest {

    @Test
    void twoArgConstructorDeclaresNoParameters() {
        final var storedQuery = new RecordMetaData.StoredQuery("select * from t1", List.of("f1", "f2"));
        assertThat(storedQuery.getQuery()).isEqualTo("select * from t1");
        assertThat(storedQuery.getTempFunctions()).containsExactly("f1", "f2");
        assertThat(storedQuery.getParameters()).isEmpty();
    }

    @Test
    void threeArgConstructorRetainsParameters() {
        final var storedQuery = new RecordMetaData.StoredQuery("SELECT id FROM f1(?PARAM_B)", List.of("f1 body"),
                Map.of("PARAM_A", "BIGINT", "PARAM_B", "STRING NOT NULL"));
        assertThat(storedQuery.getQuery()).isEqualTo("SELECT id FROM f1(?PARAM_B)");
        assertThat(storedQuery.getTempFunctions()).containsExactly("f1 body");
        assertThat(storedQuery.getParameters()).containsExactlyInAnyOrderEntriesOf(
                Map.of("PARAM_A", "BIGINT", "PARAM_B", "STRING NOT NULL"));
    }

    @Test
    void constructorCopiesItsInputs() {
        final var tempFunctions = new ArrayList<>(List.of("f1"));
        final var parameters = new HashMap<>(Map.of("PARAM_A", "BIGINT"));
        final var storedQuery = new RecordMetaData.StoredQuery("select 1", tempFunctions, parameters);

        // mutating the caller's collections after construction must not be visible through the stored query.
        tempFunctions.add("f2");
        parameters.put("PARAM_B", "STRING");

        assertThat(storedQuery.getTempFunctions()).containsExactly("f1");
        assertThat(storedQuery.getParameters()).containsOnlyKeys("PARAM_A");
    }
}
