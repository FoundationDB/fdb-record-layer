/*
 * PreparedCaseParamsTest.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.metadata.StoredQuery.ParameterState;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Turning one prepared case into the {@link PreparedParams} warm-up plans it with.
 *
 * <p>A state either binds a value or leaves the parameter without one, and that choice is the whole of what this class
 * decides. Resolving a declaration into a type happens later, on the planning path, and is covered by
 * {@code OfflineValueFreePlanGenerationTest}.</p>
 */
class PreparedCaseParamsTest {

    @Test
    void isNullBindsARealNull() {
        final var params = PreparedCaseParams.of(Map.of("P", "BIGINT"), Map.of("P", ParameterState.IS_NULL));

        // containsKey, not a non-null value: this is what makes the parameter value-bound rather than value-free, so the
        // planner folds the predicate instead of leaving a typed constant behind.
        assertThat(params.hasNamedParamValue("P")).isTrue();
        assertThat(params.namedParamValue("P")).isNull();
    }

    @Test
    void booleanStatesBindRealBooleans() {
        final var whenTrue = PreparedCaseParams.of(Map.of("B", "BOOLEAN"), Map.of("B", ParameterState.IS_TRUE));
        assertThat(whenTrue.namedParamValue("B")).isEqualTo(Boolean.TRUE);

        final var whenFalse = PreparedCaseParams.of(Map.of("B", "BOOLEAN"), Map.of("B", ParameterState.IS_FALSE));
        assertThat(whenFalse.namedParamValue("B")).isEqualTo(Boolean.FALSE);
    }

    @Test
    void isNotNullLeavesTheParameterWithoutAValue() {
        final var params = PreparedCaseParams.of(Map.of("P", "BIGINT"), Map.of("P", ParameterState.IS_NOT_NULL));

        assertThat(params.hasNamedParamValue("P")).isFalse();
        assertThat(params.declarationMaybe("P")).contains("BIGINT");
    }

    /**
     * Declarations are passed on for every parameter, including the ones a case binds a value to. Harmless because a
     * declaration is only read for a parameter with no value, and it means nothing has to be filtered per case.
     */
    @Test
    void everyDeclarationIsPassedOnWhateverTheStateIs() {
        final var params = PreparedCaseParams.of(
                Map.of("BOUND", "BIGINT", "FREE", "STRING"),
                Map.of("BOUND", ParameterState.IS_NULL, "FREE", ParameterState.IS_NOT_NULL));

        assertThat(params.declarationMaybe("BOUND")).contains("BIGINT");
        assertThat(params.declarationMaybe("FREE")).contains("STRING");
    }

    @Test
    void caseMixingStatesBindsEachAccordingly() {
        final var params = PreparedCaseParams.of(
                Map.of("ZONE", "BIGINT", "WIDE", "BOOLEAN", "ADOPTER", "INTEGER NOT NULL"),
                Map.of("ZONE", ParameterState.IS_NULL,
                        "WIDE", ParameterState.IS_FALSE,
                        "ADOPTER", ParameterState.IS_NOT_NULL));

        assertThat(params.namedParamValue("ZONE")).isNull();
        assertThat(params.namedParamValue("WIDE")).isEqualTo(Boolean.FALSE);
        assertThat(params.hasNamedParamValue("ADOPTER")).isFalse();
        assertThat(params.declarationMaybe("ADOPTER")).contains("INTEGER NOT NULL");
    }

    /**
     * A state with no declaration behind it is caught here, so the failure names the missing declaration rather than
     * surfacing later as an ordinary unbound parameter with a missing value.
     */
    @Test
    void valueFreeStateWithoutADeclarationIsRejected() {
        assertThatThrownBy(() -> PreparedCaseParams.of(Map.of(), Map.of("P", ParameterState.IS_NOT_NULL)))
                .hasMessageContaining("prepared case names 'P'")
                .hasMessageContaining("the parameter list does not declare");
    }

    @Test
    void caseWithNoParametersIsEmpty() {
        final var params = PreparedCaseParams.of(Map.of(), Map.of());

        assertThat(params.isEmpty()).isTrue();
    }
}
