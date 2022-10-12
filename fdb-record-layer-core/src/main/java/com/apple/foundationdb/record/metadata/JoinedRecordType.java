/*
 * JoinedRecordType.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A <i>synthetic</i> record type representing the indexable result of <em>joining</em> stored records.
 */
@API(API.Status.EXPERIMENTAL)
public class JoinedRecordType extends SyntheticRecordType<JoinedRecordType.JoinConstituent> {

    @Nonnull
    private final List<Join> joins;

    /**
     * A constituent type within a joined record type.
     */
    public static class JoinConstituent extends SyntheticRecordType.Constituent {
        private final boolean outerJoined;

        protected JoinConstituent(String name, RecordType recordType, boolean outerJoined) {
            super(name, recordType);
            this.outerJoined = outerJoined;
        }

        public boolean isOuterJoined() {
            return outerJoined;
        }
    }

    /**
     * An equi-join between constituent types.
     */
    public static class Join {
        @Nonnull
        private final JoinConstituent left;
        @Nonnull
        private final KeyExpression leftSourceExpression;
        @Nonnull
        private final KeyExpression leftValueExpression;
        @Nonnull
        private final JoinConstituent right;
        @Nonnull
        private final KeyExpression rightSourceExpression;

        @Nonnull
        private final KeyExpression rightValueExpression;

        protected Join(@Nonnull JoinConstituent left, @Nonnull KeyExpression leftSourceExpression, @Nonnull KeyExpression leftValueExpression,
                       @Nonnull JoinConstituent right, @Nonnull KeyExpression rightSourceExpression, @Nonnull KeyExpression rightValueExpression) {
            this.left = left;
            this.leftSourceExpression = leftSourceExpression;
            this.leftValueExpression = leftValueExpression;
            this.right = right;
            this.rightSourceExpression = rightSourceExpression;
            this.rightValueExpression = rightValueExpression;
        }

        @Nonnull
        public JoinConstituent getLeft() {
            return left;
        }

        @Nonnull
        public KeyExpression getLeftSourceExpression() {
            return leftSourceExpression;
        }

        @Nonnull
        public KeyExpression getLeftValueExpression() {
            return leftValueExpression;
        }

        @Nonnull
        public JoinConstituent getRight() {
            return right;
        }

        @Nonnull
        public KeyExpression getRightSourceExpression() {
            return rightSourceExpression;
        }

        @Nonnull
        public KeyExpression getRightValueExpression() {
            return rightValueExpression;
        }
    }

    @SuppressWarnings("squid:S00107") // Comes from Builder.
    protected JoinedRecordType(@Nonnull RecordMetaData metaData, @Nonnull Descriptors.Descriptor descriptor,
                               @Nonnull KeyExpression primaryKey, @Nonnull Object recordTypeKey,
                               @Nonnull List<Index> indexes, @Nonnull List<Index> multiTypeIndexes,
                               @Nonnull List<JoinConstituent> constituents, @Nonnull List<Join> joins) {
        super(metaData, descriptor, primaryKey, recordTypeKey, indexes, multiTypeIndexes, constituents);
        this.joins = joins;
    }

    @Nonnull
    public List<Join> getJoins() {
        return joins;
    }

    @Nonnull
    public RecordMetaDataProto.JoinedRecordType toProto() {
        RecordMetaDataProto.JoinedRecordType.Builder typeBuilder = RecordMetaDataProto.JoinedRecordType.newBuilder()
                .setName(getName())
                .setRecordTypeKey(LiteralKeyExpression.toProtoValue(getRecordTypeKey()));
        for (JoinedRecordType.JoinConstituent joinConstituent : getConstituents()) {
            RecordMetaDataProto.JoinedRecordType.JoinConstituent.Builder constituentBuilder = typeBuilder.addJoinConstituentsBuilder()
                    .setName(joinConstituent.getName())
                    .setRecordType(joinConstituent.getRecordType().getName());
            if (joinConstituent.isOuterJoined()) {
                constituentBuilder.setOuterJoined(true);
            }
        }
        for (JoinedRecordType.Join join : getJoins()) {
            typeBuilder.addJoinsBuilder()
                    .setLeft(join.getLeft().getName())
                    .setLeftSourceExpression(join.getLeftSourceExpression().toKeyExpression())
                    .setLeftValueExpression(join.getLeftValueExpression().toKeyExpression())
                    .setRight(join.getRight().getName())
                    .setRightSourceExpression(join.getRightSourceExpression().toKeyExpression())
                    .setRightValueExpression(join.getRightValueExpression().toKeyExpression());
        }
        return typeBuilder.build();
    }

}
