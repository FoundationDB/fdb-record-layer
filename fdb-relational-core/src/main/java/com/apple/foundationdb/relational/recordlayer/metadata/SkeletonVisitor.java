/*
 * SkeletonVisitor.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.metadata.Column;
import com.apple.foundationdb.relational.api.metadata.Index;
import com.apple.foundationdb.relational.api.metadata.InvokedRoutine;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.api.metadata.View;
import com.apple.foundationdb.relational.api.metadata.Visitor;

import javax.annotation.Nonnull;

/**
 * This is a no-op {@link Visitor} added to make convenient to implement other visitors
 * targeting specific {@link com.apple.foundationdb.relational.api.metadata.Metadata} artifacts.
 */
@API(API.Status.EXPERIMENTAL)
public class SkeletonVisitor implements Visitor {
    @Override
    public void visit(@Nonnull final Table table) {
        // no-op
    }

    @Override
    public void visit(@Nonnull final Column column) {
        // no-op
    }

    @Override
    public void startVisit(@Nonnull final SchemaTemplate schemaTemplate) {
        // no-op
    }

    @Override
    public void visit(@Nonnull final SchemaTemplate schemaTemplate) {
        // no-op
    }

    @Override
    public void finishVisit(@Nonnull final SchemaTemplate schemaTemplate) {
        // no-op
    }

    @Override
    public void visit(@Nonnull final Schema schema) {
        // no-op
    }

    @Override
    public void visit(@Nonnull final Index index) {
        // no-op
    }

    @Override
    public void visit(@Nonnull final InvokedRoutine invokedRoutine) {
        // no-op
    }

    @Override
    public void visit(@Nonnull final View view) {
        // no-op
    }
}
