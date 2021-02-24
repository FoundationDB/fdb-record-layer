/*
 * TypeFilterExpression.java
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

package com.apple.foundationdb.record.query.plan.temp.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Objects;

/**
 * A relational planner expression that represents a type filter. This includes both logical and physical type filtering
 * expressions.
 */
@API(API.Status.EXPERIMENTAL)
public interface TypeFilterExpression extends RelationalExpressionWithChildren {
    @Nonnull
    Collection<String> getRecordTypes();

    @Nonnull
    @Override
    default TypeFilterExpression rebase(@Nonnull final AliasMap translationMap) {
        // we know the following is correct, just Java doesn't
        return (TypeFilterExpression)RelationalExpressionWithChildren.super.rebase(translationMap);
    }

    @Override
    default boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                          @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }

        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        return ((TypeFilterExpression)otherExpression).getRecordTypes().equals(getRecordTypes());
    }

    @Override
    default int hashCodeWithoutChildren() {
        return Objects.hash(getRecordTypes());
    }
}
