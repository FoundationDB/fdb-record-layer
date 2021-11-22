/*
 * BoundKeyPart.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * A key expression that can be bound by a comparison.
 */
public class KeyPart {
    @Nonnull
    private final KeyExpression normalizedKeyExpression;

    private final boolean isReverse;

    protected KeyPart(@Nonnull final KeyExpression normalizedKeyExpression, final boolean isReverse) {
        this.normalizedKeyExpression = normalizedKeyExpression;
        this.isReverse = isReverse;
    }

    @Nonnull
    public KeyExpression getNormalizedKeyExpression() {
        return normalizedKeyExpression;
    }

    public boolean isReverse() {
        return isReverse;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KeyPart)) {
            return false;
        }
        final var keyPart = (KeyPart)o;
        return getNormalizedKeyExpression().equals(keyPart.getNormalizedKeyExpression()) &&
               isReverse() == keyPart.isReverse();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNormalizedKeyExpression(), isReverse());
    }

    @Override
    public String toString() {
        return "(" + getNormalizedKeyExpression() + ", " + isReverse() + ')';
    }

    @Nonnull
    public static List<KeyPart> prefix(@Nonnull final List<? extends KeyPart> keyParts, final int endExclusive) {
        return ImmutableList.copyOf(keyParts.subList(0, endExclusive));
    }

    @Nonnull
    public static KeyPart of(@Nonnull final KeyExpression normalizedKeyExpression) {
        return KeyPart.of(normalizedKeyExpression, false);
    }

    @Nonnull
    public static KeyPart of(@Nonnull final KeyExpression normalizedKeyExpression,
                             final boolean isReverse) {
        return new KeyPart(normalizedKeyExpression, isReverse);
    }
}
