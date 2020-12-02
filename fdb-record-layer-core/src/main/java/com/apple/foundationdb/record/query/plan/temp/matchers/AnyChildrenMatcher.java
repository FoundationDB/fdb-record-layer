/*
 * AnyChildrenMatcher.java
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.record.query.plan.temp.Bindable;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

/**
 * A children matcher that matches any set of children.
 */
public class AnyChildrenMatcher implements ExpressionChildrenMatcher {
    public static final ExpressionChildrenMatcher ANY = new AnyChildrenMatcher();

    private AnyChildrenMatcher() {
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> matches(@Nonnull final PlannerBindings outerBindings, @Nonnull List<? extends Bindable> children) {
        return Stream.of(PlannerBindings.empty());
    }
}
