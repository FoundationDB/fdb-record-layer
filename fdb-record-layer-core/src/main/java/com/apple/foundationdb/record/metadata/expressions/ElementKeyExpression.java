/*
 * ElementKeyExpression.java
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.temp.view.Element;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A key expression that derives its values from an {@link Element}.
 *
 * <p>
 * This key expression is only used in the "planner normalized" key expressions produced by
 * {@link KeyExpression#normalizeForPlanner(Source, List)} and is meant to make it easy for methods such as
 * {@link com.apple.foundationdb.record.query.plan.temp.view.ViewExpression#fromIndexDefinition}
 * to interpret a key expression without dealing with complex rules for nesting and fan-out.
 * @see KeyExpression#normalizeForPlanner(Source, List)
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class ElementKeyExpression extends BaseKeyExpression implements KeyExpressionWithoutChildren {
    @Nonnull
    private final Element element;

    public ElementKeyExpression(@Nonnull Element element) {
        this.element = element;
    }

    @Nonnull
    public Element getElement() {
        return element;
    }

    @Nonnull
    @Override
    public KeyExpression normalizeForPlanner(@Nonnull Source source, @Nonnull List<String> fieldNamePrefix) {
        throw new RecordCoreException("element key expression is already normalized");
    }

    @Nonnull
    @Override
    public List<Element> flattenForPlanner() {
        return Collections.singletonList(element);
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluate(@Nullable FDBRecord<M> record) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean createsDuplicates() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getColumnSize() {
        return 1;
    }

    @Nonnull
    @Override
    public Message toProto() throws SerializationException {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.KeyExpression toKeyExpression() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int planHash() {
        return 0;
    }
}
