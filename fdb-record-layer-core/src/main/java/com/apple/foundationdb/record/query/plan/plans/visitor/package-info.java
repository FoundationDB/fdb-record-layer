/*
 * package-info.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
 * A package of classes in the visitor pattern that perform substitutions on a tree of
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan}s. These visitors are used in the
 * {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner} to execute certain optimizations.
 */
package com.apple.foundationdb.record.query.plan.plans.visitor;
