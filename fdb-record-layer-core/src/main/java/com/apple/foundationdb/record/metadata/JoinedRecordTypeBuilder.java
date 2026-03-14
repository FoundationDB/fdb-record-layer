/*
 * JoinedRecordTypeBuilder.java
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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A builder for {@link JoinedRecordType}.
 */
@API(API.Status.EXPERIMENTAL)
public final class JoinedRecordTypeBuilder extends SyntheticRecordTypeBuilder<JoinedRecordTypeBuilder.JoinConstituent> {

    @Nonnull
    private final List<Join> joins = new ArrayList<>();

    /**
     * A constituent type within a joined record type.
     */
    public static class JoinConstituent extends SyntheticRecordTypeBuilder.Constituent {
        private final boolean outerJoined;

        public JoinConstituent(@Nonnull String name, @Nonnull RecordTypeBuilder recordType, boolean outerJoined) {
            super(name, recordType);
            this.outerJoined = outerJoined;
        }

        public boolean isOuterJoined() {
            return outerJoined;
        }

        @Nonnull
        JoinedRecordType.JoinConstituent build(@Nonnull RecordMetaData metaData) {
            return new JoinedRecordType.JoinConstituent(getName(), metaData.getRecordType(getRecordType().getName()), outerJoined);
        }
    }

    /**
     * An equi-join between constituent types.
     */
    public static class Join {
        @Nonnull
        private final String left;
        @Nonnull
        private final KeyExpression leftExpression;
        @Nonnull
        private final String right;
        @Nonnull
        private final KeyExpression rightExpression;

        public Join(@Nonnull String left, @Nonnull KeyExpression leftExpression, @Nonnull String right, @Nonnull KeyExpression rightExpression) {
            this.left = left;
            this.leftExpression = leftExpression;
            this.right = right;
            this.rightExpression = rightExpression;
        }

        @Nonnull
        public String getLeft() {
            return left;
        }

        @Nonnull
        public KeyExpression getLeftExpression() {
            return leftExpression;
        }

        @Nonnull
        public String getRight() {
            return right;
        }

        @Nonnull
        public KeyExpression getRightExpression() {
            return rightExpression;
        }

        @Nonnull
        protected JoinedRecordType.Join build(@Nonnull Map<String, JoinedRecordType.JoinConstituent> constituentsByName) {
            return new JoinedRecordType.Join(constituentsByName.get(left), leftExpression, constituentsByName.get(right), rightExpression);
        }
    }

    public JoinedRecordTypeBuilder(@Nonnull String name, @Nonnull Object recordTypeKey, @Nonnull RecordMetaDataBuilder metaDataBuilder) {
        super(name, recordTypeKey, metaDataBuilder);
    }

    public JoinedRecordTypeBuilder(@Nonnull RecordMetaDataProto.JoinedRecordType typeProto, @Nonnull RecordMetaDataBuilder metaDataBuilder) {
        super(typeProto.getName(), LiteralKeyExpression.fromProtoValue(typeProto.getRecordTypeKey()), metaDataBuilder);
        for (RecordMetaDataProto.JoinedRecordType.JoinConstituent joinConstituent : typeProto.getJoinConstituentsList()) {
            addConstituent(joinConstituent.getName(), metaDataBuilder.getRecordType(joinConstituent.getRecordType()), joinConstituent.getOuterJoined());
        }
        for (RecordMetaDataProto.JoinedRecordType.Join join : typeProto.getJoinsList()) {
            addJoin(join.getLeft(), KeyExpression.fromProto(join.getLeftExpression()), join.getRight(), KeyExpression.fromProto(join.getRightExpression()));
        }
    }

    @Override
    @Nonnull
    protected JoinConstituent newConstituent(@Nonnull String name, @Nonnull RecordTypeBuilder recordType) {
        return new JoinConstituent(name, recordType, false);
    }

    /**
     * Add a new constituent by name.
     * @param name the correlation name for the new constituent
     * @param recordType the record type for the new constituent
     * @param isOuterJoined whether constituent is outer-joined in joins in which it participates
     * @return the newly added constituent
     */
    @Nonnull
    public JoinConstituent addConstituent(@Nonnull String name, @Nonnull RecordTypeBuilder recordType, boolean isOuterJoined) {
        return addConstituent(new JoinConstituent(name, recordType, isOuterJoined));
    }

    /**
     * Get the list of joins for this joined record type.
     * @return the list of joins
     */
    @Nonnull
    public List<Join> getJoins() {
        return joins;
    }

    /**
     * Add a new join.
     * @param left the correlation name of the left constituent
     * @param leftExpression an expression to evaluate against the left constituent
     * @param right the correlation name of the right constituent
     * @param rightExpression an expression to evaluate against the right constituent
     * @return the newly added join
     */
    @Nonnull
    public Join addJoin(@Nonnull String left, @Nonnull KeyExpression leftExpression, @Nonnull String right, @Nonnull KeyExpression rightExpression) {
        if (leftExpression.getColumnSize() != rightExpression.getColumnSize()) {
            throw new RecordCoreArgumentException("Two sides of join are not the same size and will never match")
                    .addLogInfo("left", leftExpression, "right", rightExpression);
        }
        Join join = new Join(left, leftExpression, right, rightExpression);
        joins.add(join);
        return join;
    }

    /**
     * Add a new join.
     * @param left the correlation name of the left constituent
     * @param leftField a field to evaluate in the left constituent
     * @param right the correlation name of the right constituent
     * @param rightField a field to evaluate in the right constituent
     * @return the newly added join
     */
    @Nonnull
    public Join addJoin(@Nonnull String left, @Nonnull String leftField, @Nonnull String right, @Nonnull String rightField) {
        Join join = new Join(left, Key.Expressions.field(leftField), right, Key.Expressions.field(rightField));
        joins.add(join);
        return join;
    }

    @Nonnull
    @Override
    public JoinedRecordType build(@Nonnull RecordMetaData metaData, @Nonnull Descriptors.FileDescriptor fileDescriptor) {
        final List<JoinedRecordType.JoinConstituent> builtConstituents = getConstituents().stream()
                .map(constituent -> constituent.build(metaData))
                .collect(Collectors.toList());
        final Descriptors.Descriptor descriptor = fileDescriptor.findMessageTypeByName(name);
        final KeyExpression primaryKey = buildPrimaryKey();
        final Map<String, JoinedRecordType.JoinConstituent> constituentsByName = builtConstituents.stream()
                .collect(Collectors.toMap(JoinedRecordType.Constituent::getName, Function.identity()));
        final List<JoinedRecordType.Join> builtJoins = joins.stream()
                .map(join -> join.build(constituentsByName))
                .collect(Collectors.toList());
        return new JoinedRecordType(metaData, descriptor, primaryKey, recordTypeKey, indexes, multiTypeIndexes, builtConstituents, builtJoins);
    }

}
