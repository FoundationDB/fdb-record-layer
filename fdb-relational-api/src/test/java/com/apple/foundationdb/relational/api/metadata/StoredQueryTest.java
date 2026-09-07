/*
 * StoredQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.metadata;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StoredQueryTest {

    @Test
    void twoArgConstructorDeclaresNoParametersAndNoCases() {
        final var storedQuery = new StoredQuery("select * from t1", List.of("f1", "f2"));
        assertThat(storedQuery.getQuery()).isEqualTo("select * from t1");
        assertThat(storedQuery.getTempFunctions()).containsExactly("f1", "f2");
        assertThat(storedQuery.getParameters()).isEmpty();
        assertThat(storedQuery.getPreparedCases()).isEmpty();
    }

    @Test
    void fourArgConstructorRetainsParametersAndCases() {
        final var storedQuery = new StoredQuery("SELECT id FROM f1(?PARAM_B)", List.of("f1 body"),
                Map.of("PARAM_A", "BIGINT", "PARAM_B", "STRING NOT NULL"),
                List.of(Map.of("PARAM_A", StoredQuery.ParameterState.IS_NULL,
                                "PARAM_B", StoredQuery.ParameterState.IS_NOT_NULL),
                        Map.of("PARAM_A", StoredQuery.ParameterState.IS_NOT_NULL,
                                "PARAM_B", StoredQuery.ParameterState.IS_NOT_NULL)));
        assertThat(storedQuery.getQuery()).isEqualTo("SELECT id FROM f1(?PARAM_B)");
        assertThat(storedQuery.getTempFunctions()).containsExactly("f1 body");
        assertThat(storedQuery.getParameters()).containsExactlyInAnyOrderEntriesOf(
                Map.of("PARAM_A", "BIGINT", "PARAM_B", "STRING NOT NULL"));
        assertThat(storedQuery.getPreparedCases()).containsExactly(
                Map.of("PARAM_A", StoredQuery.ParameterState.IS_NULL,
                        "PARAM_B", StoredQuery.ParameterState.IS_NOT_NULL),
                Map.of("PARAM_A", StoredQuery.ParameterState.IS_NOT_NULL,
                        "PARAM_B", StoredQuery.ParameterState.IS_NOT_NULL));
    }

    @Test
    void constructorCopiesItsInputs() {
        final var tempFunctions = new ArrayList<>(List.of("f1"));
        final var parameters = new HashMap<>(Map.of("PARAM_A", "BIGINT"));
        final var firstCase = new HashMap<>(Map.of("PARAM_A", StoredQuery.ParameterState.IS_NULL));
        final var preparedCases = new ArrayList<Map<String, StoredQuery.ParameterState>>(List.of(firstCase));
        final var storedQuery = new StoredQuery("select 1", tempFunctions, parameters, preparedCases);

        // mutating the caller's collections after construction must not be visible through the stored query. The cases
        // are a list of maps, so both levels have to be copied, not just the outer one.
        tempFunctions.add("f2");
        parameters.put("PARAM_B", "STRING");
        firstCase.put("PARAM_B", StoredQuery.ParameterState.IS_TRUE);
        preparedCases.add(Map.of("PARAM_A", StoredQuery.ParameterState.IS_NOT_NULL));

        assertThat(storedQuery.getTempFunctions()).containsExactly("f1");
        assertThat(storedQuery.getParameters()).containsOnlyKeys("PARAM_A");
        assertThat(storedQuery.getPreparedCases()).containsExactly(
                Map.of("PARAM_A", StoredQuery.ParameterState.IS_NULL));
    }

    /**
     * The constant names are the tokens the wire format carries, so a rename would silently orphan every stored query
     * already persisted with the old spelling.
     */
    @Test
    void parameterStateNamesAreTheWireTokens() {
        assertThat(StoredQuery.ParameterState.values())
                .extracting(Enum::name)
                .containsExactly("IS_NULL", "IS_NOT_NULL", "IS_TRUE", "IS_FALSE");
    }
}
