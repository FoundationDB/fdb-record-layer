/*
 * InvokedRoutinesMatcher.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.matchers;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.InvokedRoutine;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

import javax.annotation.Nonnull;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class SchemaTemplateMatchers {

    @Nonnull
    public static Matcher<InvokedRoutine> routine(@Nonnull final String name,
                                                  @Nonnull final String description) {
        return allOf(
                new FeatureMatcher<>(equalTo(name), "name", "name") {

                    @Override
                    protected String featureValueOf(final InvokedRoutine actual) {
                        return actual.getName();
                    }
                },
                new FeatureMatcher<>(equalTo(description), "description", "description") {
                    @Override
                    protected String featureValueOf(final InvokedRoutine actual) {
                        return actual.getDescription();
                    }
                });
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static Matcher<SchemaTemplate> containsRoutinesInAnyOrder(Matcher<? super InvokedRoutine>... routineMatchers) {
        return new FeatureMatcher<>(containsInAnyOrder(routineMatchers), "Routines defined in schema template", "routines") {
            @Override
            protected Iterable<? extends InvokedRoutine> featureValueOf(final SchemaTemplate actual) {
                try {
                    return actual.getInvokedRoutines();
                } catch (RelationalException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
