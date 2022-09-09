/*
 * QualifiedIdentifierValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import java.util.Arrays;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

public final class QualifiedIdentifierValue extends LiteralValue<String> {
    @Nonnull
    private final String[] parts;

    private QualifiedIdentifierValue(@Nonnull String... values) {
        super(Arrays.stream(values).collect(Collectors.joining(".")));
        parts = values;
    }

    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP", justification = "intentional")
    @Nonnull
    public String[] getParts() {
        return parts;
    }

    public boolean isQualified() {
        return parts.length > 1;
    }

    @Nonnull
    public static QualifiedIdentifierValue of(@Nonnull String... values) {
        Assert.thatUnchecked(values.length > 0, "QualifiedIdentifierValue should be created with at least one value");
        return new QualifiedIdentifierValue(values);
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (other == this) {
            return true;
        }
        if (!(other instanceof QualifiedIdentifierValue)) {
            return false;
        }
        final var otherQualifiedIdentifierValue = (QualifiedIdentifierValue) other;
        return Arrays.equals(parts, otherQualifiedIdentifierValue.parts) && super.equals(other);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }
}
