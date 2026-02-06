/*
 * BaseKeyExpression.java
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Key;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.ListIterator;

/**
 * Base class to provide some common logic shared between most expression implementations.
 */
@API(API.Status.UNSTABLE)
public abstract class BaseKeyExpression implements KeyExpression {
    @Override
    @Nonnull
    public final KeyExpression getSubKey(int start, int end) {
        final int columnSize = getColumnSize();
        if (start < 0 || end > columnSize || start > end) {
            throw new IllegalSubKeyException(start, end, columnSize);
        }
        if (start == end) {
            return EmptyKeyExpression.EMPTY;
        } else if ((start == 0) && (end == columnSize)) {
            return this;
        }
        return getSubKeyImpl(start, end);
    }

    @Nonnull
    protected KeyExpression getSubKeyImpl(int start, int end) {
        throw new UnsplittableKeyExpressionException(this);
    }

    void validateColumnCounts(List<Key.Evaluated> results) {
        int columnSize = getColumnSize();
        for (Key.Evaluated result : results) {
            if (result.size() != columnSize) {
                throw new InvalidResultException("Expression evaluated to unexpected number of columns").addLogInfo(
                        LogMessageKeys.KEY_EXPRESSION, toString(),
                        LogMessageKeys.KEY_EVALUATED, result.toTuple().toString(),
                        LogMessageKeys.EXPECTED_COLUMN_SIZE, getColumnSize(),
                        LogMessageKeys.ACTUAL_COLUMN_SIZE, result.size());
            }
        }
    }

    @Override
    public boolean isPrefixKey(@Nonnull KeyExpression key) {
        if (this instanceof EmptyKeyExpression || this.equals(key)) {
            return true;        // Fast check for common cases.
        }

        if (this instanceof GroupingKeyExpression && ((GroupingKeyExpression) this).getGroupedCount() > 0) {
            return false; // a grouping can never be a prefix of a different key
        }

        final Deque<KeyExpression> prefixes = new ArrayDeque<>();
        final Deque<KeyExpression> keys = new ArrayDeque<>();
        prefixes.push(this);
        keys.push(key);

        while (!prefixes.isEmpty()) {
            if (keys.isEmpty()) {
                // iif the rest of the remaining prefix is empty, we still match
                return prefixes.stream().allMatch(prefixPart -> prefixPart instanceof EmptyKeyExpression);
            }

            KeyExpression prefixPart = prefixes.pop();
            KeyExpression keyPart = keys.pop();

            // robustness check to ensure that proper interfaces are implemented
            // every key expression must implement either Key.ExpressionWithChildren or Key.ExpressionWithoutChildren
            if (!keyPart.hasProperInterfaces() || !prefixPart.hasProperInterfaces()) {
                throw new KeyExpression.InvalidExpressionException("Expression contained Key.Expression implementation that does " +
                                                                   "not implement Key.ExpressionWithChildren or Key.ExpressionWithoutChildren");
            }

            if (prefixPart instanceof AtomKeyExpression && keyPart instanceof AtomKeyExpression) {
                // there should be an atom comparison here
                if (((AtomKeyExpression) prefixPart).equalsAtomic((AtomKeyExpression) keyPart)) {
                    pushChildren(prefixes, prefixPart);
                    pushChildren(keys, keyPart);
                } else {
                    return false;
                }
            } else if (prefixPart instanceof AtomKeyExpression) {
                prefixes.push(prefixPart); // save prefixPart for comparison at next step
                pushChildren(keys, keyPart); // push children of keyPart, if it has any
            } else if (keyPart instanceof AtomKeyExpression) {
                keys.push(keyPart); // save keyPart for comparison at next step
                pushChildren(prefixes, prefixPart); // push children of prefixPart, if it has any
            } else {
                pushChildren(prefixes, prefixPart);
                pushChildren(keys, keyPart);
            }
        }
        return true;
    }

    private static void pushChildren(Deque<KeyExpression> stack, KeyExpression part) {
        if (part instanceof KeyExpressionWithChildren) {
            List<KeyExpression> children = ((KeyExpressionWithChildren) part).getChildren();
            ListIterator<KeyExpression> childrenIterator = children.listIterator(children.size());
            while (childrenIterator.hasPrevious()) {
                stack.push(childrenIterator.previous());
            }
        }
    }

    /**
     * An exception indicating that the key expression is not splittable.
     */
    public static class UnsplittableKeyExpressionException extends RecordCoreException {
        public static final long serialVersionUID = 1L;

        public UnsplittableKeyExpressionException(@Nonnull BaseKeyExpression keyExpression) {
            super("Cannot split " + keyExpression);
        }
    }

    /**
     * An exception that is thrown when {@link #getSubKey(int, int)} is used incorrectly.
     */
    public static final class IllegalSubKeyException extends RecordCoreException {
        public static final long serialVersionUID = 1L;

        public IllegalSubKeyException(int start, int end, int columnSize) {
            super("requested subkey is invalid");
            addLogInfo(LogMessageKeys.REQUESTED_START, start);
            addLogInfo(LogMessageKeys.REQUESTED_END, end);
            addLogInfo(LogMessageKeys.COLUMN_SIZE, columnSize);
        }
    }
}
