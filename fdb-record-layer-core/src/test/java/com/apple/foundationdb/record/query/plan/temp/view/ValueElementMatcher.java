/*
 * ValueElementMatcher.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.view;

import org.hamcrest.Description;
import org.hamcrest.Matcher;

import javax.annotation.Nonnull;

/**
 * A Hamcrest matcher that checks whether an {@link Element} is a {@link ValueElement}.
 */
public class ValueElementMatcher extends ElementMatcher {
    @Nonnull
    private final Matcher<Source> sourceMatcher;

    public ValueElementMatcher(@Nonnull Matcher<Source> sourceMatcher) {
        this.sourceMatcher = sourceMatcher;
    }

    @Override
    protected boolean matchesSafely(Element element) {
        return element instanceof ValueElement && sourceMatcher.matches(((ValueElement)element).getSource());
    }

    @Override
    public void describeTo(Description description) {
        sourceMatcher.describeTo(description);
        description.appendText(" with value");
    }
}
