/*
 * package-info.java
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

/**
 * Classes implementing executable query plan elements.
 *
 * <p>
 * The result of planning a query is a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan}, which supports an {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan#execute execute} method
 * against an {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore} with an {@link com.apple.foundationdb.record.provider.foundationdb.FDBEvaluationContext} holding dynamic parameter values.
 * </p>
 *
 * <p>
 * The most basic plan elements read from the database. For example,
 * </p>
 * <ul>
 * <li>{@link com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan} scans the record store for all records.</li>
 * <li>{@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan} scans a secondary index.</li>
 * </ul>
 *
 * <p>
 * In most cases, a plan is actually a tree of {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan} elements,
 * which is executed by asynchronously streaming the results of child plan elements into the parent processor.
 * </p>
 *
 * <p>
 * For example,
 * </p>
  * <ul>
 * <li>A {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan} evaluates a boolean predicate expression against upstream records, only letting through those that satisfy it.</li>
 * <li>A {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan} merges two or more compatibly-ordered streams of records into one, eliminating duplicates.</li>
 * <li>A {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan} merges two or more compatibly-ordered streams of records into one, keeping only those that appear in all streams.</li>
 * </ul>
 */
package com.apple.foundationdb.record.query.plan.plans;
