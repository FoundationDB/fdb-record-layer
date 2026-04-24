/*
 * RenameFieldsVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata.expressions.visitors;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionWithChild;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionWithChildren;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionWithValue;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.ListKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.SplitKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionVisitor;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Visitor that can be used to rewrite a {@link KeyExpression} in response to a field renaming. This
 * should generally be invoked via {@link #renameFields(KeyExpression, Descriptors.Descriptor, Descriptors.Descriptor)}.
 *
 * @see #renameFields(KeyExpression, Descriptors.Descriptor, Descriptors.Descriptor)
 */
public final class RenameFieldsVisitor implements KeyExpressionVisitor<RenameFieldsVisitor.RenameFieldsState, KeyExpression> {
    @Nonnull
    private final Deque<RenameFieldsState> stateStack;

    private RenameFieldsVisitor(@Nonnull Descriptors.Descriptor sourceDescriptor, @Nonnull Descriptors.Descriptor targetDescriptor) {
        this.stateStack = new ArrayDeque<>();
        stateStack.add(new RenameFieldsState(sourceDescriptor, targetDescriptor));
    }

    @Override
    @Nonnull
    public RenameFieldsState getCurrentState() {
        return stateStack.getLast();
    }

    @Nonnull
    @Override
    public EmptyKeyExpression visitExpression(@Nonnull final EmptyKeyExpression emptyKeyExpression) {
        return emptyKeyExpression;
    }

    @Nonnull
    @Override
    public FieldKeyExpression visitExpression(@Nonnull final FieldKeyExpression fieldKeyExpression) {
        final String originalName = fieldKeyExpression.getFieldName();
        final RenameFieldsState state = getCurrentState();
        final String newName = state.renameField(originalName);
        if (originalName.equals(newName)) {
            return fieldKeyExpression;
        } else {
            return new FieldKeyExpression(newName, fieldKeyExpression.getFanType(), fieldKeyExpression.getNullStandin());
        }
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    @Nonnull
    @Override
    public NestingKeyExpression visitExpression(@Nonnull final NestingKeyExpression nestingKeyExpression) {
        // Rewrite the parent field
        final FieldKeyExpression originalParent = nestingKeyExpression.getParent();
        final FieldKeyExpression newParent = visitExpression(originalParent);

        // Rewrite child field. To do this properly, we have to make sure to look up the new renaming map
        // that need to apply to the child's descriptor type
        final Descriptors.Descriptor childSource = getMessageTypeForField(getCurrentState().sourceDescriptor, originalParent);
        final Descriptors.Descriptor childTarget = getMessageTypeForField(getCurrentState().targetDescriptor, newParent);
        final KeyExpression newChild;
        if (childSource == childTarget) {
            // No renaming here. Skip the recursive exploration
            newChild = nestingKeyExpression.getChild();
        } else {
            stateStack.addLast(new RenameFieldsState(childSource, childTarget));
            newChild = nestingKeyExpression.getChild().expand(this);
            stateStack.removeLast();
        }

        if (originalParent == newParent && newChild == nestingKeyExpression.getChild()) {
            return nestingKeyExpression;
        } else {
            return newParent.nest(newChild);
        }
    }

    @Nonnull
    private static Descriptors.Descriptor getMessageTypeForField(@Nonnull Descriptors.Descriptor descriptor, @Nonnull FieldKeyExpression field) {
        final Descriptors.FieldDescriptor targetFieldDescriptor = descriptor.findFieldByName(field.getFieldName());
        if (targetFieldDescriptor == null) {
            throw new MetaDataException("parent field not found")
                    .addLogInfo(LogMessageKeys.FIELD_NAME, field.getFieldName())
                    .addLogInfo(LogMessageKeys.MESSAGE, descriptor.getFullName());
        }
        if (targetFieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            return targetFieldDescriptor.getMessageType();
        } else {
            throw new MetaDataException("parent field is not of message type")
                    .addLogInfo(LogMessageKeys.FIELD_NAME, field.getFieldName())
                    .addLogInfo(LogMessageKeys.MESSAGE, descriptor.getFullName());
        }
    }

    @Nonnull
    @Override
    public KeyExpressionWithValue visitExpression(@Nonnull final KeyExpressionWithValue keyExpressionWithValue) {
        if (keyExpressionWithValue instanceof LiteralKeyExpression<?>
                || keyExpressionWithValue instanceof VersionKeyExpression
                || keyExpressionWithValue instanceof RecordTypeKeyExpression) {
            // All of these types are invariant to field renamings and can be returned as-is
            return keyExpressionWithValue;
        } else {
            throw new RecordCoreArgumentException("field renaming not supported for expression")
                    .addLogInfo(LogMessageKeys.KEY_EXPRESSION, keyExpressionWithValue);
        }
    }

    @Nonnull
    @Override
    public FunctionKeyExpression visitExpression(@Nonnull final FunctionKeyExpression functionKeyExpression) {
        final KeyExpression newArguments = rewriteChild(functionKeyExpression);
        if (newArguments == null) {
            return functionKeyExpression;
        } else {
            return Key.Expressions.function(functionKeyExpression.getName(), newArguments);
        }
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    @Nonnull
    @Override
    public KeyWithValueExpression visitExpression(@Nonnull final KeyWithValueExpression keyWithValueExpression) {
        // The child of a KeyWithValueExpression refers just to the component in the key, so we can't use rewriteChild here
        final KeyExpression newWholeKey = keyWithValueExpression.getInnerKey().expand(this);
        if (newWholeKey == keyWithValueExpression.getInnerKey()) {
            return keyWithValueExpression;
        } else {
            return Key.Expressions.keyWithValue(newWholeKey, keyWithValueExpression.getSplitPoint());
        }
    }

    @Nonnull
    @Override
    public ThenKeyExpression visitExpression(@Nonnull final ThenKeyExpression thenKeyExpression) {
        final List<KeyExpression> newChildren = rewriteChildren(thenKeyExpression);
        if (newChildren == null) {
            return thenKeyExpression;
        } else {
            return Key.Expressions.concat(newChildren);
        }
    }

    @Nonnull
    @Override
    public ListKeyExpression visitExpression(@Nonnull final ListKeyExpression listKeyExpression) {
        final List<KeyExpression> newChildren = rewriteChildren(listKeyExpression);
        if (newChildren == null) {
            return listKeyExpression;
        } else {
            return Key.Expressions.list(newChildren);
        }
    }

    @Nonnull
    @Override
    public KeyExpression visitExpression(@Nonnull final KeyExpression keyExpression) {
        // These KeyExpression classes implementations fall through to the default KeyExpression within the visitor.
        // We should consider adding them to the top-level visitor so that we can follow the visitor
        // pattern more generally.
        // See: https://github.com/FoundationDB/fdb-record-layer/issues/4110
        if (keyExpression instanceof GroupingKeyExpression) {
            return visitExpression((GroupingKeyExpression) keyExpression);
        } else if (keyExpression instanceof SplitKeyExpression) {
            return visitExpression((SplitKeyExpression) keyExpression);
        } else if (keyExpression instanceof DimensionsKeyExpression) {
            return visitExpression((DimensionsKeyExpression) keyExpression);
        } else {
            throw new RecordCoreArgumentException("field renaming not supported for expression")
                    .addLogInfo(LogMessageKeys.KEY_EXPRESSION, keyExpression);
        }
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    @Nonnull
    public GroupingKeyExpression visitExpression(@Nonnull final GroupingKeyExpression groupingKeyExpression) {
        final KeyExpression newWholeKey = groupingKeyExpression.getWholeKey().expand(this);
        if (newWholeKey == groupingKeyExpression.getWholeKey()) {
            return groupingKeyExpression;
        } else {
            return new GroupingKeyExpression(newWholeKey, groupingKeyExpression.getGroupedCount());
        }
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    @Nonnull
    public SplitKeyExpression visitExpression(@Nonnull final SplitKeyExpression splitKeyExpression) {
        // Note: "JOINED" is not a child expression, so we can't use rewriteChild here
        final KeyExpression newJoined = splitKeyExpression.getJoined().expand(this);
        if (newJoined == splitKeyExpression.getJoined()) {
            return splitKeyExpression;
        } else {
            return new SplitKeyExpression(newJoined, splitKeyExpression.getColumnSize());
        }

    }

    @Nonnull
    public DimensionsKeyExpression visitExpression(@Nonnull final DimensionsKeyExpression dimensionsKeyExpression) {
        final KeyExpression newChild = rewriteChild(dimensionsKeyExpression);
        if (newChild == null) {
            return dimensionsKeyExpression;
        } else {
            return DimensionsKeyExpression.of(newChild, dimensionsKeyExpression.getPrefixSize(), dimensionsKeyExpression.getDimensionsSize());
        }
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    @Nullable
    private KeyExpression rewriteChild(@Nonnull final KeyExpressionWithChild keyExpressionWithChild) {
        final KeyExpression child = keyExpressionWithChild.getChild();
        final KeyExpression rewritten = child.expand(this);
        if (child == rewritten) {
            return null;
        } else {
            return rewritten;
        }
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    @Nullable
    private List<KeyExpression> rewriteChildren(@Nonnull final KeyExpressionWithChildren keyExpressionWithChildren) {
        boolean anyChanged = false;
        final List<KeyExpression> children = keyExpressionWithChildren.getChildren();
        final ImmutableList.Builder<KeyExpression> newChildren = ImmutableList.builderWithExpectedSize(children.size());
        for (KeyExpression child : children) {
            final KeyExpression rewrittenChild = child.expand(this);
            newChildren.add(rewrittenChild);
            anyChanged |= (child != rewrittenChild);
        }
        if (anyChanged) {
            return newChildren.build();
        } else {
            return null;
        }
    }

    public static final class RenameFieldsState implements KeyExpressionVisitor.State {
        @Nonnull
        private final Descriptors.Descriptor sourceDescriptor;
        @Nonnull
        private final Descriptors.Descriptor targetDescriptor;

        private RenameFieldsState(@Nonnull Descriptors.Descriptor sourceDescriptor, @Nonnull Descriptors.Descriptor targetDescriptor) {
            this.sourceDescriptor = sourceDescriptor;
            this.targetDescriptor = targetDescriptor;
        }

        @Nonnull
        public String renameField(@Nonnull String sourceFieldName) {
            // Grab the source field descriptor by name
            @Nullable Descriptors.FieldDescriptor sourceField = sourceDescriptor.findFieldByName(sourceFieldName);
            if (sourceField == null) {
                throw new MetaDataException("field not found in source descriptor")
                        .addLogInfo(LogMessageKeys.FIELD_NAME, sourceFieldName)
                        .addLogInfo(LogMessageKeys.MESSAGE, sourceDescriptor.getFullName());
            }
            // Use the field number to find the equivalent field in the target
            @Nullable Descriptors.FieldDescriptor targetField = targetDescriptor.findFieldByNumber(sourceField.getNumber());
            if (targetField == null) {
                throw new MetaDataException("field not found in target descriptor")
                        .addLogInfo(LogMessageKeys.OLD_FIELD_NAME, sourceFieldName)
                        .addLogInfo(LogMessageKeys.MESSAGE, targetDescriptor.getFullName());
            }
            return targetField.getName();
        }
    }

    /**
     * Rewrite an expression in response to modifying the descriptor. In particular, this looks at the fields referenced
     * by the original expression (by name), and then it finds the equivalent field (by number) in the new descriptor.
     * It will then create an updated expression which references the new field name. This updated expression can then
     * be used, for instance, to update an index in response to a field change in the meta-data, or to validate that
     * such a change has not resulted in any semantic differences.
     *
     * @param expression the original expression
     * @param sourceDescriptor a {@link Descriptors.Descriptor} for which the original {@code expression} was written
     * @param targetDescriptor a {@link Descriptors.Descriptor} on which to rewrite the {@code expression}
     * @return a new key expression with rewritten field information
     */
    @Nonnull
    public static KeyExpression renameFields(@Nonnull KeyExpression expression, @Nonnull Descriptors.Descriptor sourceDescriptor, @Nonnull Descriptors.Descriptor targetDescriptor) {
        if (sourceDescriptor == targetDescriptor) {
            return expression;
        }
        final RenameFieldsVisitor visitor = new RenameFieldsVisitor(sourceDescriptor, targetDescriptor);
        return expression.expand(visitor);
    }
}
