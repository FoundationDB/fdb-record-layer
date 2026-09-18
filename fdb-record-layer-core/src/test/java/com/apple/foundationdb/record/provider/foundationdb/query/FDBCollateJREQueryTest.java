/*
 * FDBCollateJREQueryTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.metadata.expressions.CollateFunctionKeyExpressionFactoryJRE;

/**
 * Concrete subclass that runs {@link FDBCollateQueryTestBase} against the JRE collator.
 * The abstract base lives in the testFixtures source set so that other modules
 * (e.g. fdb-record-layer-icu) can supply their own collator function names; this concrete
 * subclass lives in the test source set so JUnit Platform discovers and runs it.
 */
@SuppressWarnings("checkstyle:abbreviationaswordinname") // Allow JRE here.
public class FDBCollateJREQueryTest extends FDBCollateQueryTestBase {
    public FDBCollateJREQueryTest() {
        super(CollateFunctionKeyExpressionFactoryJRE.FUNCTION_NAME);
    }
}
