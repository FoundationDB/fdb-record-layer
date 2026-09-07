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
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.google.protobuf.Descriptors;

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

    private final List<Join> joins = new ArrayList<>();

    /**
     * A constituent type within a joined record type.
     */
    public static class JoinConstituent extends SyntheticRecordTypeBuilder.Constituent {
        private final boolean outerJoined;

        public JoinConstituent(String name, RecordTypeBuilder recordType, boolean outerJoined) {
            super(name, recordType);
            this.outerJoined = outerJoined;
        }

        public boolean isOuterJoined() {
            return outerJoined;
        }

        JoinedRecordType.JoinConstituent build(RecordMetaData metaData) {
            return new JoinedRecordType.JoinConstituent(getName(), metaData.getRecordType(getRecordType().getName()), outerJoined);
        }
    }

    /**
     * An equi-join between constituent types.
     */
    public static class Join {
        private final String left;
        private final KeyExpression leftExpression;
        private final String right;
        private final KeyExpression rightExpression;

        public Join(String left, KeyExpression leftExpression, String right, KeyExpression rightExpression) {
            this.left = left;
            this.leftExpression = leftExpression;
            this.right = right;
            this.rightExpression = rightExpression;
        }

        public String getLeft() {
            return left;
        }

        public KeyExpression getLeftExpression() {
            return leftExpression;
        }

        public String getRight() {
            return right;
        }

        public KeyExpression getRightExpression() {
            return rightExpression;
        }

        protected JoinedRecordType.Join build(Map<String, JoinedRecordType.JoinConstituent> constituentsByName) {
            final JoinedRecordType.JoinConstituent leftConstituent = constituentsByName.get(left);
            if (leftConstituent == null) {
                throw new RecordCoreArgumentException("unknown constituent in join").addLogInfo("constituent", left);
            }
            final JoinedRecordType.JoinConstituent rightConstituent = constituentsByName.get(right);
            if (rightConstituent == null) {
                throw new RecordCoreArgumentException("unknown constituent in join").addLogInfo("constituent", right);
            }
            return new JoinedRecordType.Join(leftConstituent, leftExpression, rightConstituent, rightExpression);
        }
    }

    public JoinedRecordTypeBuilder(String name, Object recordTypeKey, RecordMetaDataBuilder metaDataBuilder) {
        super(name, recordTypeKey, metaDataBuilder);
    }

    public JoinedRecordTypeBuilder(RecordMetaDataProto.JoinedRecordType typeProto, RecordMetaDataBuilder metaDataBuilder) {
        super(typeProto.getName(), requireRecordTypeKey(typeProto), metaDataBuilder);
        for (RecordMetaDataProto.JoinedRecordType.JoinConstituent joinConstituent : typeProto.getJoinConstituentsList()) {
            addConstituent(joinConstituent.getName(), metaDataBuilder.getRecordType(joinConstituent.getRecordType()), joinConstituent.getOuterJoined());
        }
        for (RecordMetaDataProto.JoinedRecordType.Join join : typeProto.getJoinsList()) {
            addJoin(join.getLeft(), KeyExpression.fromProto(join.getLeftExpression()), join.getRight(), KeyExpression.fromProto(join.getRightExpression()));
        }
    }

    private static Object requireRecordTypeKey(RecordMetaDataProto.JoinedRecordType typeProto) {
        final Object recordTypeKey = LiteralKeyExpression.fromProtoValue(typeProto.getRecordTypeKey());
        if (recordTypeKey == null) {
            throw new RecordCoreArgumentException("joined record type must have a record type key")
                    .addLogInfo(LogMessageKeys.RECORD_TYPE, typeProto.getName());
        }
        return recordTypeKey;
    }

    @Override
    protected JoinConstituent newConstituent(String name, RecordTypeBuilder recordType) {
        return new JoinConstituent(name, recordType, false);
    }

    /**
     * Add a new constituent by name.
     * @param name the correlation name for the new constituent
     * @param recordType the record type for the new constituent
     * @param isOuterJoined whether constituent is outer-joined in joins in which it participates
     * @return the newly added constituent
     */
    public JoinConstituent addConstituent(String name, RecordTypeBuilder recordType, boolean isOuterJoined) {
        return addConstituent(new JoinConstituent(name, recordType, isOuterJoined));
    }

    /**
     * Get the list of joins for this joined record type.
     * @return the list of joins
     */
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
    public Join addJoin(String left, KeyExpression leftExpression, String right, KeyExpression rightExpression) {
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
    public Join addJoin(String left, String leftField, String right, String rightField) {
        Join join = new Join(left, Key.Expressions.field(leftField), right, Key.Expressions.field(rightField));
        joins.add(join);
        return join;
    }

    @Override
    public JoinedRecordType build(RecordMetaData metaData, Descriptors.FileDescriptor fileDescriptor) {
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
        if (recordTypeKey == null) {
            // Should not happen: both constructors guarantee a non-null record type key.
            throw new RecordCoreArgumentException("joined record type must have a record type key")
                    .addLogInfo(LogMessageKeys.RECORD_TYPE, name);
        }
        return new JoinedRecordType(metaData, descriptor, primaryKey, recordTypeKey, indexes, multiTypeIndexes, builtConstituents, builtJoins);
    }

}
