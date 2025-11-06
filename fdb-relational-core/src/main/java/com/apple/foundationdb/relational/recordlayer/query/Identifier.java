/*
 * Identifier.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
public class Identifier {
    @Nonnull
    private final String name;

    @Nonnull
    private final List<String> qualifier;

    protected Identifier(@Nonnull String name, @Nonnull Iterable<String> qualifier) {
        this.name = name;
        this.qualifier = ImmutableList.copyOf(qualifier);
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public List<String> getQualifier() {
        return qualifier;
    }

    public boolean isQualified() {
        return !qualifier.isEmpty();
    }

    @Override
    public String toString() {
        return String.join(".", qualifier) + (qualifier.isEmpty() ? "" : ".") + name;
    }

    @Nonnull
    public static Identifier of(@Nonnull String name) {
        return new Identifier(name, ImmutableList.of());
    }

    @Nonnull
    public static Identifier of(@Nonnull String name, @Nonnull Iterable<String> qualifier) {
        return new Identifier(name, qualifier);
    }

    @Nonnull
    public Identifier withQualifier(@Nonnull Collection<String> qualifier) {
        if (qualifier.isEmpty()) {
            return this;
        }
        final ImmutableList.Builder<String> newQualifierBuilder = ImmutableList.builder();
        newQualifierBuilder.addAll(qualifier);
        newQualifierBuilder.addAll(getQualifier());
        return new Identifier(name, newQualifierBuilder.build());
    }

    @Nonnull
    public Identifier replaceQualifier(@Nonnull Function<Collection<String>, Collection<String>> replaceFunc) {
        final var replacingQualifier = replaceFunc.apply(qualifier);
        if (replacingQualifier.equals(qualifier)) {
            return this;
        }
        return new Identifier(name, replacingQualifier);
    }

    @Nonnull
    public Identifier withQualifier(@Nonnull String qualifier) {
        return withQualifier(List.of(qualifier));
    }

    @Nonnull
    public Identifier withoutQualifier() {
        if (!isQualified()) {
            return this;
        }
        return new Identifier(getName(), ImmutableList.of());
    }

    @Nonnull
    public List<String> fullyQualifiedName() {
        // todo: should amortize
        if (!isQualified()) {
            return List.of(name);
        }
        return ImmutableList.<String>builder().addAll(getQualifier()).add(name).build();
    }

    public boolean prefixedWith(@Nonnull Identifier identifier) {
        final var identifierFullName = identifier.fullyQualifiedName();
        final var fullName = fullyQualifiedName();
        if (fullName.size() < identifierFullName.size()) {
            return false;
        }
        for (int i = 0; i < identifierFullName.size(); i++) {
            if (!fullName.get(i).equals(identifierFullName.get(i))) {
                return false;
            }
        }
        return true;
    }

    public boolean qualifiedWith(@Nonnull Identifier identifier) {
        final var identifierFullName = identifier.fullyQualifiedName();
        final var fullName = fullyQualifiedName();
        if (fullName.size() != identifierFullName.size() + 1) {
            return false;
        }
        for (int i = 0; i < identifierFullName.size(); i++) {
            if (!fullName.get(i).equals(identifierFullName.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Nonnull
    public static Identifier toProtobufCompliant(@Nonnull final Identifier identifier) {
        final var qualifier = identifier.getQualifier().stream().map(DataTypeUtils::toProtoBufCompliantName).collect(Collectors.toList());
        return Identifier.of(DataTypeUtils.toProtoBufCompliantName(identifier.getName()), qualifier);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Identifier)) {
            return false;
        }
        final var otherIdentifier = (Identifier) obj;
        return getName().equals(otherIdentifier.getName()) && getQualifier().equals(otherIdentifier.getQualifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getQualifier());
    }
}
