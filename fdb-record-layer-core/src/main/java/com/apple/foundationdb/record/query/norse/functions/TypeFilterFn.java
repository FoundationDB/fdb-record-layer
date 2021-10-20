/*
 * TypeFilterFn.java
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

package com.apple.foundationdb.record.query.norse.functions;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.query.norse.BuiltInFunction;
import com.apple.foundationdb.record.query.norse.ParserContext;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.predicates.Atom;
import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Function
 * typeFilter(STRING...) -> STREAM.
 */
@AutoService(BuiltInFunction.class)
public class TypeFilterFn extends BuiltInFunction<RelationalExpression> {
    public TypeFilterFn() {
        super("typeFilter",
                ImmutableList.of(new Type.Stream()), Type.primitiveType(Type.TypeCode.STRING), TypeFilterFn::encapsulate);
    }

    private static RelationalExpression encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<RelationalExpression> builtInFunction, @Nonnull final List<Atom> arguments) {
        Verify.verify(arguments.get(0) instanceof RecordQueryPlan);

        // force evaluation of the string-type arguments (for the record types)
        final ImmutableSet<String> recordTypeNames = arguments
                .stream()
                .skip(1L) // that's the stream
                .peek(argument -> Verify.verify(argument.getResultType().getTypeCode() == Type.TypeCode.STRING))
                .map(argument -> {
                    final Object result = ((Value)argument).compileTimeEval(EvaluationContext.forDynamicSchema(parserContext.getDynamicSchemaBuilder().build()));
                    if (result instanceof String) {
                        return (String)result;
                    } else {
                        throw new IllegalArgumentException("arguments to from() must be strings (for record types)");
                    }
                })
                .collect(ImmutableSet.toImmutableSet());

        Verify.verify(!recordTypeNames.isEmpty());

        final RecordMetaData recordMetaData = parserContext.getRecordMetaData();

        final Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap = recordTypeNames
                .stream()
                .flatMap(recordTypeName -> recordMetaData.getRecordType(recordTypeName).getDescriptor().getFields().stream())
                .collect(Collectors.groupingBy(Descriptors.FieldDescriptor::getName,
                        Collectors.reducing(null,
                                (fieldDescriptor, fieldDescriptor2) -> {
                                    Verify.verify(fieldDescriptor != null || fieldDescriptor2 != null);
                                    if (fieldDescriptor == null) {
                                        return fieldDescriptor2;
                                    }
                                    if (fieldDescriptor2 == null) {
                                        return fieldDescriptor;
                                    }
                                    // TODO improve
                                    final Type.TypeCode typeCode = Type.TypeCode.fromProtobufType(fieldDescriptor.getType());
                                    final Type.TypeCode typeCode2 = Type.TypeCode.fromProtobufType(fieldDescriptor2.getType());
                                    if (typeCode.isPrimitive() &&
                                            typeCode2.isPrimitive() &&
                                            typeCode == typeCode2) {
                                        return fieldDescriptor;
                                    }

                                    throw new IllegalArgumentException("cannot form union type of complex fields");
                                })));

        return new RecordQueryTypeFilterPlan(Quantifier.physical(GroupExpressionRef.of((RelationalExpression)arguments.get(0))), recordTypeNames,
                Type.Record.fromFieldDescriptorsMap(fieldDescriptorMap));
    }
}
