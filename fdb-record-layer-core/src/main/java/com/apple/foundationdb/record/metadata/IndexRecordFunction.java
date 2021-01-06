/*
 * IndexRecordFunction.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordFunction;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A function which is applied to a record with the help of an index.
 * @param <T> the result type of the function
 * @see com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore#evaluateRecordFunction
 */
@API(API.Status.MAINTAINED)
public class IndexRecordFunction<T> extends RecordFunction<T> {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Index-Record-Function");

    @Nonnull
    private final GroupingKeyExpression operand;
    @Nullable
    private final String index;

    public IndexRecordFunction(@Nonnull String name, @Nonnull GroupingKeyExpression operand, @Nullable String index) {
        super(name);
        this.operand = operand;
        this.index = index;
    }

    @Nonnull
    public GroupingKeyExpression getOperand() {
        return operand;
    }

    @Nullable
    public String getIndex() {
        return index;
    }

    @Nonnull
    public IndexRecordFunction<T> cloneWithOperand(@Nonnull GroupingKeyExpression operand) {
        return new IndexRecordFunction<>(getName(), operand, getIndex());
    }

    @Nonnull
    public IndexRecordFunction<T> cloneWithIndex(@Nonnull String index) {
        return new IndexRecordFunction<>(getName(), getOperand(), index);
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        operand.validate(descriptor);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        if (index != null) {
            str.append(index).append('.');
        }
        str.append(getName()).append('(').append(operand).append(')');
        return str.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        IndexRecordFunction<?> that = (IndexRecordFunction) o;

        return this.operand.equals(that.operand) && Objects.equals(this.index, that.index);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + operand.hashCode();
        if (index != null) {
            result = 31 * result + index.hashCode();
        }
        return result;
    }

    @Override
    public int planHash(@Nonnull final PlanHashable.PlanHashKind hashKind) {
        return super.basePlanHash(hashKind, BASE_HASH, operand, index);
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return super.baseQueryHash(hashKind, BASE_HASH, operand, index);
    }
}
