/*
 * IndexFieldVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.MetaDataException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Visitor pattern for visiting KeyExpressions as they are being created in an index.
 */
public interface KeyExpressionVisitor {

    KeyExpression visitField(FieldKeyExpression fke);

    KeyExpression visitThen(ThenKeyExpression thenKey);

    KeyExpression visitNestingKey(NestingKeyExpression nke);

    KeyExpression visitGroupingKey(GroupingKeyExpression gke);

    boolean applies(String indexType);

    default KeyExpression visit(KeyExpression ke) {
        if (ke instanceof FieldKeyExpression) {
            return visitField((FieldKeyExpression)ke);
        } else if (ke instanceof ThenKeyExpression) {
            return visitThen((ThenKeyExpression)ke);
        } else if (ke instanceof NestingKeyExpression) {
            return visitNestingKey((NestingKeyExpression)ke);
        } else if (ke instanceof GroupingKeyExpression) {
            return visitGroupingKey((GroupingKeyExpression)ke);
        } else {
            throw new MetaDataException("Cannot visit KeyExpression of type <" + ke + ">");
        }
    }

    interface Factory {
        KeyExpressionVisitor createVisitor();
    }

    class Registry {
        private static final Registry INSTANCE = new Registry();

        @Nullable
        private volatile List<KeyExpressionVisitor> functions;

        private Registry() {
            // Will be initialized the first time a builder is requested
            functions = null;
        }

        @Nonnull
        public static Registry instance() {
            return INSTANCE;
        }

        public List<KeyExpressionVisitor> visitors() {
            return initOrGetRegistry();
        }

        @Nonnull
        private List<KeyExpressionVisitor> initOrGetRegistry() {
            // The reference to the registry is copied into a local variable to avoid referencing the
            // volatile multiple times
            List<KeyExpressionVisitor> currRegistry = functions;
            if (currRegistry != null) {
                return currRegistry;
            }
            synchronized (this) {
                currRegistry = functions;
                if (currRegistry == null) {
                    // Create the registry
                    List<KeyExpressionVisitor> newRegistry = initRegistry();
                    functions = newRegistry;
                    return newRegistry;
                } else {
                    // Another thread created the registry for us
                    return currRegistry;
                }
            }
        }

        @Nonnull
        private static List<KeyExpressionVisitor> initRegistry() {
            try {
                List<KeyExpressionVisitor> functions = new LinkedList<>();
                for (KeyExpressionVisitor.Factory factory : ServiceLoader.load(KeyExpressionVisitor.Factory.class)) {
                    functions.add(factory.createVisitor());
                }
                return new CopyOnWriteArrayList<>(functions);
            } catch (ServiceConfigurationError err) {
                throw new RecordCoreException("Unable to load all defined FunctionKeyExpressions", err);
            }
        }
    }
}
