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
 * Classes related to the Cascades matcher system and its implementation.
 *
 * <p>
 * The {@link com.apple.foundationdb.record.query.plan.temp Cascades} planner is based on a system of rules, each of which is
 * defined by a matcher expression and a method that is applied when an expression matches a given matcher expression.
 * A matcher is represented by an implementation of the {@link com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher
 * ExpressionMatcher} interface and is responsible for inspecting the structure of a planner expression, determining
 * whether it matches an expression, and providing bindings to access particular substructures of the expression during
 * the execution of the rule. These bindings are exposed by the {@link com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings}
 * class.
 * </p>
 */
package com.apple.foundationdb.record.query.plan.temp.matchers;
