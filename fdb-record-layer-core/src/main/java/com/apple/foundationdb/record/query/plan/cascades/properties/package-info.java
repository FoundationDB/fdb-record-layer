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
 * A package for planner properties (in the sense that Cascades uses them).
 *
 * <p>
 * In Cascades, a property is an attribute of {@link com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression}
 * that cannot be described from the structure of the expression tree alone. For example, the set of record types that
 * a relational planner expression produces is a property, but whether or not it has a filter that contains an AND
 * clause is not a property. Properties generally need to be evaluated against an expression, instead of arising from
 * the structural features of a planner expression (which could be matched using an
 * {@link com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher}.
 * </p>
 *
 * <p>
 * In the Record Layer, properties are implemented as hierarchical visitors on the {@code PlannerExpression} tree; this
 * approach prevents the accumulation of many methods on the {@code PlannerExpression} interface. Furthermore, some
 * properties (such as the set of possible record types returned) cannot be determined from the {@code PlannerExpression}
 * alone; they also need access to the {@link com.apple.foundationdb.record.query.plan.cascades.PlanContext}.
 * </p>
 *
 * <p>
 * This package contains various implementations of {@link com.apple.foundationdb.record.query.plan.cascades.SimpleExpressionVisitor}.
 * </p>
 */
package com.apple.foundationdb.record.query.plan.cascades.properties;
