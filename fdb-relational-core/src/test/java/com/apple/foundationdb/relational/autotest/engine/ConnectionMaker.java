/*
 * ConnectionMaker.java
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

package com.apple.foundationdb.relational.autotest.engine;

import com.apple.foundationdb.relational.autotest.Connector;
import org.junit.platform.commons.JUnitException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

abstract class ConnectionMaker {

    abstract Connector createConnector(Object testInstance);

    public static ConnectionMaker makeConnection(Field field) {
        return new FieldMaker(field);
    }

    public static ConnectionMaker makeConnection(Method method) {
        return new MethodMaker(method);
    }

    private static final class FieldMaker extends ConnectionMaker {
        private final Field field;

        private FieldMaker(Field field) {
            this.field = field;
        }

        @Override
        Connector createConnector(Object testInstance) {
            try {
                Object o = field.get(testInstance);
                return (Connector) o;
            } catch (ClassCastException ce) {
                String message = "Connector field [" + field.getName() + "] must be a single Connector instance.";
                throw new JUnitException(message, ce);
            } catch (IllegalAccessException e) {
                String message = "Connector field [" + field.getName() + "] must be public.";
                throw new JUnitException(message, e);
            }
        }
    }

    private static final class MethodMaker extends ConnectionMaker {
        private final Method method;

        private MethodMaker(Method method) {
            this.method = method;
        }

        @Override
        Connector createConnector(Object testInstance) {
            try {
                Object o = method.invoke(testInstance);
                return (Connector) o;
            } catch (ClassCastException | InvocationTargetException ce) {
                String message = "Connector method [" + method.getName() + "] must return a Connector type";
                throw new JUnitException(message, ce);
            } catch (IllegalAccessException e) {
                String message = "Connector method [" + method.getName() + "] must be public.";
                throw new JUnitException(message, e);
            }
        }
    }
}
