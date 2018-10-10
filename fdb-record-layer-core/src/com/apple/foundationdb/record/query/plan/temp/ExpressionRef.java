/*
 * ExpressionRef.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.RecordCoreException;

import javax.annotation.Nonnull;

/**
 * This interface is used mostly as an (admittedly surmountable) barrier to rules mutating bound references directly,
 * which is undefined behavior. Unlike {@link MutableExpressionRef}, it does not provide an <code>insert()</code> method,
 * so the rule would need to cast the reference back to {@link MutableExpressionRef} before modifying it.
 * @param <T> the type of planner expression that is contained in this reference
 */
public interface ExpressionRef<T extends PlannerExpression> extends Bindable {
    /**
     * Return the expression contained in this reference. If the reference does not support getting its expresssion
     * (for example, because it holds more than one expression, or none at all), this should throw an exception.
     * @return the expression contained in this reference
     * @throws UngettableReferenceException if the reference does not support retrieving its expression
     */
    @Nonnull
    T get();

    /**
     * An exception thrown when {@link #get()} is called on a reference that does not support it, such as a group reference.
     * A client that encounters this exception is buggy and should not try to recover any information from the reference
     * that threw it.
     *
     * This exceeds the normal maximum inheritance depth because of the depth of the <code>RecordCoreException</code>
     * hierarchy.
     */
    public static class UngettableReferenceException extends RecordCoreException {
        private static final long serialVersionUID = 1;

        public UngettableReferenceException(String message) {
            super(message);
        }
    }
}
