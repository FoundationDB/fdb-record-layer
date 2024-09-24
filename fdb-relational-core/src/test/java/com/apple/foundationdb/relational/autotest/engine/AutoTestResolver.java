/*
 * AutoTestResolver.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.autotest.Connection;
import com.apple.foundationdb.relational.autotest.Data;
import com.apple.foundationdb.relational.autotest.Query;
import com.apple.foundationdb.relational.autotest.Schema;
import com.apple.foundationdb.relational.autotest.WorkloadConfiguration;

import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.jupiter.engine.descriptor.ClassTestDescriptor;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.support.ReflectionSupport;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 *  internal class which handles creating the test structure for a given auto test.
 */
class AutoTestResolver {

    public TestDescriptor resolveTest(TestDescriptor parentDesc, JupiterConfiguration config, Class<?> testClass) {
        UniqueId uid = parentDesc.getUniqueId().append(ClassTestDescriptor.SEGMENT_TYPE, testClass.getName());
        final List<Field> connecterFields = ReflectionSupport.findFields(testClass,
                field -> AnnotationSupport.isAnnotated(field, Connection.class), HierarchyTraversalMode.TOP_DOWN);
        final List<Method> connectorMethods = ReflectionSupport.findMethods(testClass,
                method -> AnnotationSupport.isAnnotated(method, Connection.class), HierarchyTraversalMode.TOP_DOWN);

        //get the Configuration
        ConfigurationInvoker configInvoker = new ConfigurationInvoker(getMethods(testClass, WorkloadConfiguration.class),
                getFields(testClass, WorkloadConfiguration.class));

        List<ConnectionMaker> connMakers = new ArrayList<>(connecterFields.size() + connectorMethods.size());
        connecterFields.forEach(field -> connMakers.add(ConnectionMaker.makeConnection(field)));
        connectorMethods.forEach(method -> connMakers.add(ConnectionMaker.makeConnection(method)));

        List<Field> schemaFields = getFields(testClass, Schema.class);
        List<Method> schemaMethods = getMethods(testClass, Schema.class);
        SchemaInvoker schemaInvoker = new SchemaInvoker(schemaMethods, schemaFields);

        List<Field> dataFields = getFields(testClass, Data.class);
        List<Method> dataMethods = getMethods(testClass, Data.class);
        DataInvoker dataSetInvoker = new DataInvoker(dataMethods, dataFields);

        final List<Field> querySetFields = getFields(testClass, Query.class);
        List<Method> queryMethods = getMethods(testClass, Query.class);
        QueryInvoker qi = new QueryInvoker(queryMethods, querySetFields);

        return new AutoTestDescriptor(uid, testClass, config, connMakers, schemaInvoker, dataSetInvoker, qi, configInvoker);
    }

    private List<Field> getFields(Class<?> testClass, Class<? extends Annotation> annotation) {
        return ReflectionSupport.findFields(testClass, field -> AnnotationSupport.isAnnotated(field, annotation), HierarchyTraversalMode.TOP_DOWN);
    }

    private List<Method> getMethods(Class<?> testClass, Class<? extends Annotation> annotation) {
        return ReflectionSupport.findMethods(testClass, method -> AnnotationSupport.isAnnotated(method, annotation), HierarchyTraversalMode.TOP_DOWN);
    }
}
