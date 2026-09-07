/*
 * ReferencedFieldsConstraint.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

/**
 * A constraint holding a set of referenced field values.
 */
public class ReferencedFieldsConstraint implements PlannerConstraint<ReferencedFieldsConstraint.ReferencedFields> {
    public static final PlannerConstraint<ReferencedFields> REFERENCED_FIELDS = new ReferencedFieldsConstraint();

    /**
     * A set of referenced field values.
     */
    public static class ReferencedFields {
        private final Set<FieldValue> referencedFieldValues;

        public ReferencedFields(final Set<FieldValue> referencedFieldValues) {
            this.referencedFieldValues = referencedFieldValues;
        }

        public Set<FieldValue> getReferencedFieldValues() {
            return referencedFieldValues;
        }
    }

    @Override
    public Optional<ReferencedFields> combine(final ReferencedFields currentConstraint, final ReferencedFields newConstraint) {
        final ImmutableSet<FieldValue> referencedFields =
                ImmutableSet.<FieldValue>builder()
                        .addAll(currentConstraint.getReferencedFieldValues())
                        .addAll(newConstraint.getReferencedFieldValues())
                        .build();

        if (referencedFields.size() > currentConstraint.getReferencedFieldValues().size()) {
            return Optional.of(new ReferencedFields(referencedFields));
        }

        return Optional.empty();
    }
}
