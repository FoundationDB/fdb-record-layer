/*
 * FDBCollateICUQueryTest.java
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

package com.apple.foundationdb.record.icu;

import com.apple.foundationdb.record.provider.foundationdb.query.FDBCollateQueryTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for query execution using {@link CollateFunctionKeyExpressionFactoryICU}.
 */
@SuppressWarnings("checkstyle:abbreviationaswordinname") // Allow ICU here.
public class FDBCollateICUQueryTest extends FDBCollateQueryTest {

    public FDBCollateICUQueryTest() {
        super(CollateFunctionKeyExpressionFactoryICU.FUNCTION_NAME);
    }

    protected static final String[] NUMBERS = {
        "1.2.3", "12.0.0", "1.10.0"
    };

    @Test
    public void sortNumbersDefault() throws Exception {
        sortOnly(null, NUMBERS, "1.10.0", "1.2.3", "12.0.0");
    }

    @Test
    public void sortNumbersLocale() throws Exception {
        sortOnly("en", NUMBERS, "1.10.0", "1.2.3", "12.0.0");
    }

    @Test
    public void sortNumbersNumeric() throws Exception {
        sortOnly("en@colNumeric=yes", NUMBERS, "1.2.3", "1.10.0", "12.0.0");
    }

    @Test
    public void sortNumbersNonNumeric() throws Exception {
        sortOnly("en@colNumeric=no", NUMBERS, "1.10.0", "1.2.3", "12.0.0");
    }

}
