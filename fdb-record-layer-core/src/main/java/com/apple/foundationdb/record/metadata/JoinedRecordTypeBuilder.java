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
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
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
public class JoinedRecordTypeBuilder extends SyntheticRecordTypeBuilder<JoinedRecordTypeBuilder.JoinConstituent> {

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

        @Override
        protected JoinedRecordType.JoinConstituent build(@Nonnull RecordMetaData metaData) {
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
        private final KeyExpression leftIndexExpression;
        @Nonnull
        private final KeyExpression leftJoinExpression;
        @Nonnull
        private final String right;
        @Nonnull
        private final KeyExpression rightIndexExpression;
        @Nonnull
        private final KeyExpression rightJoinExpression;

        /**
         * Create a new join from left to right.
         *
         * @param left the name of the type on the LHS of the join.
         * @param leftIndexExpression the KeyExpression to use when constructing indexes. The entries in this
         * KeyExpression
         * must line up correctly with how the join index will store records on disk.
         * @param leftJoinExpression the KeyExpression to use when applying the join condition on incoming records from
         * the left-hand-side. This
         * expression isn't stored, it's just used for join comparisons (For example, when applying a join condition
         * where
         * the join is between two projections).
         * @param right the name of the type on the right hand side of the join.
         * @param rightIndexExpression The KeyExpression to use when constructing indexes. The entries in this
         * KeyExpression
         * must line up correctly with how the join index will store RHS records on disk.
         * @param rightJoinExpression the KeyExpression to use when applying the join conditions on incoming records
         * from the right-hand-side.
         * This expression isn't stored, it's just used for join operations (For example, when applying a join condition
         * where
         * the join is between two projections).
         */
        public Join(@Nonnull String left, @Nonnull KeyExpression leftIndexExpression, @Nonnull KeyExpression leftJoinExpression,
                    @Nonnull String right, @Nonnull KeyExpression rightIndexExpression, @Nonnull KeyExpression rightJoinExpression) {
            this.left = left;
            this.leftIndexExpression = leftIndexExpression;
            this.leftJoinExpression = leftJoinExpression;
            this.right = right;
            this.rightIndexExpression = rightIndexExpression;
            this.rightJoinExpression = rightJoinExpression;
        }

        @Nonnull
        public String getLeft() {
            return left;
        }

        @Nonnull
        public KeyExpression getLeftIndexExpression() {
            return leftIndexExpression;
        }

        @Nonnull
        public KeyExpression getLeftJoinExpression() {
            return leftJoinExpression;
        }

        @Nonnull
        public String getRight() {
            return right;
        }

        @Nonnull
        public KeyExpression getRightIndexExpression() {
            return rightIndexExpression;
        }

        @Nonnull
        public KeyExpression getRightJoinExpression() {
            return rightJoinExpression;
        }

        @Nonnull
        protected JoinedRecordType.Join build(@Nonnull Map<String, JoinedRecordType.JoinConstituent> constituentsByName) {
            return new JoinedRecordType.Join(constituentsByName.get(left), leftIndexExpression, leftJoinExpression, constituentsByName.get(right), rightIndexExpression, rightJoinExpression);
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
            addJoin(join.getLeft(), KeyExpression.fromProto(join.getLeftExpression()), KeyExpression.fromProto(join.getLeftJoinExpression()),
                    join.getRight(), KeyExpression.fromProto(join.getRightExpression()), KeyExpression.fromProto(join.getRightJoinExpression()));
        }
    }

    @Override
    @Nonnull
    protected JoinConstituent newConstituent(@Nonnull String name, @Nonnull RecordTypeBuilder recordType) {
        return new JoinConstituent(name, recordType, false);
    }

    /**
     * Add a new constituent by name.
     *
     * @param name the correlation name for the new constituent
     * @param recordType the record type for the new constituent
     * @param isOuterJoined whether constituent is outer-joined in joins in which it participates
     *
     * @return the newly added constituent
     */
    @Nonnull
    public JoinConstituent addConstituent(@Nonnull String name, @Nonnull RecordTypeBuilder recordType, boolean isOuterJoined) {
        return addConstituent(new JoinConstituent(name, recordType, isOuterJoined));
    }

    /**
     * Get the list of joins for this joined record type.
     *
     * @return the list of joins
     */
    @Nonnull
    public List<Join> getJoins() {
        return joins;
    }

    /**
     * Add a new join.
     *
     * @param left the correlation name of the left constituent
     * @param leftExpression an expression to evaluate against the left constituent
     * @param right the correlation name of the right constituent
     * @param rightExpression an expression to evaluate against the right constituent
     *
     * @return the newly added join
     */
    @Nonnull
    public Join addJoin(@Nonnull String left, @Nonnull KeyExpression leftExpression, @Nonnull String right, @Nonnull KeyExpression rightExpression) {
        if (leftExpression.getColumnSize() != rightExpression.getColumnSize()) {
            throw new RecordCoreArgumentException("Two sides of join are not the same size and will never match")
                    .addLogInfo("left", leftExpression, "right", rightExpression);
        }
        Join join = new Join(left, leftExpression, leftExpression, right, rightExpression, rightExpression);
        joins.add(join);
        return join;
    }

    /**
     * Add a new equi-join between the specified left and right sides.
     *
     * The way joins are constructed is that they first take the index expression from their side (e.g. the left
     * hand side of the join takes leftIndexExpression), and then it finds what the equivalent expression on the
     * opposite side in order to match. The join expressions are the representations of that opposition, so the
     * leftJoinExpression is what is used to match <em>rightIndexExpression</em>. and {@code rightJoinExpression}
     * is used to match {@code leftJoinExpression}.
     *
     * For example, Consider the join condition {@code left.a == right.b}, where {@code left.a} is a String and {@code right.b}
     * is an int. In this case, {@code leftIndexExpression = left.nest(a)} and {@code rightIndexExpression = right.nest(b)},
     * but if we compared them directly the join would not match correctly (because strings aren't comparable with ints). To
     * Account for this, we make {@code leftJoinExpression = left.nest(function(toInt(a))} and
     * {@code rightJoinExpression = right.nest(function(toString(b))}. Then, when inserting the LHS table, we are
     * matching {@code left.nest(function(toInt(a)) == right.nest(b)}. When inserting the RHS of the join, we are
     * matching {@code left.nest(a) == right.nest(function(toString(b))}. This will correctly match the join
     * and allow the index to proceed.
     *
     * @param left the name of the type on the LHS of the join.
     * @param leftIndexExpression the KeyExpression to use when constructing indexes.
     * @param leftJoinExpression the KeyExpression to use when applying the join condition on incoming records from the
     * left-hand-side. This
     * expression isn't stored, it's just used for join comparisons (For example, when applying a join condition where
     * the join is between two projections).
     * @param right the name of the type on the right hand side of the join.
     * @param rightIndexExpression The KeyExpression to use when constructing indexes.
     * @param rightJoinExpression the KeyExpression to use when applying the join conditions on incoming records from
     * the right-hand-side.
     * This expression isn't stored, it's just used for join operations (For example, when applying a join condition
     * where
     * the join is between two projections).
     * @return a representation of the join.
     */
    @Nonnull
    public Join addJoin(@Nonnull String left, @Nonnull KeyExpression leftIndexExpression, @Nonnull KeyExpression leftJoinExpression,
                        @Nonnull String right, @Nonnull KeyExpression rightIndexExpression, @Nonnull KeyExpression rightJoinExpression) {
        if (leftIndexExpression.getColumnSize() != rightIndexExpression.getColumnSize()) {
            throw new RecordCoreArgumentException("Two sides of join are not the same size and will never match")
                    .addLogInfo("left", leftIndexExpression, "right", rightIndexExpression);
        }
        //TODO(bfines) validate column sizes for the value expressions also
        Join join = new Join(left, leftIndexExpression, leftJoinExpression, right, rightIndexExpression, rightJoinExpression);
        joins.add(join);
        return join;
    }

    /**
     * Add a new join.
     *
     * @param left the correlation name of the left constituent
     * @param leftField a field to evaluate in the left constituent
     * @param right the correlation name of the right constituent
     * @param rightField a field to evaluate in the right constituent
     *
     * @return the newly added join
     */
    @Nonnull
    public Join addJoin(@Nonnull String left, @Nonnull String leftField, @Nonnull String right, @Nonnull String rightField) {
        final FieldKeyExpression leftFieldExpression = Key.Expressions.field(leftField);
        final FieldKeyExpression rightFieldExpression = Key.Expressions.field(rightField);
        Join join = new Join(left, leftFieldExpression, leftFieldExpression, right, rightFieldExpression, rightFieldExpression);
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
