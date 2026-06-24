/*
 * package-info.java
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

/**
 * Test fixtures for planning and executing queries against an
 * {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore}: a query test
 * base ({@link com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase}),
 * a collation-specific extension
 * ({@link com.apple.foundationdb.record.provider.foundationdb.query.FDBCollateQueryTestBase}),
 * and the {@link com.apple.foundationdb.record.provider.foundationdb.query.DualPlannerTest}
 * annotation (backed by
 * {@link com.apple.foundationdb.record.provider.foundationdb.query.DualPlannerExtension}) that
 * runs an annotated method under both planners.
 *
 * <p>
 * There is no production package of this name. These classes were lifted out of
 * {@code src/test/java} when the {@code tests} configuration was retired in favor of
 * {@code java-test-fixtures}; the boundaries here may want a follow-up rationalization.
 * </p>
 */
package com.apple.foundationdb.record.provider.foundationdb.query;
