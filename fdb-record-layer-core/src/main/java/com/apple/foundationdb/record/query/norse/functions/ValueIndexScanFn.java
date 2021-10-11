/*
 * ScanFn.java
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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.norse.BuiltInFunction;
import com.apple.foundationdb.record.query.norse.ParserContext;
import com.apple.foundationdb.record.query.norse.SemanticException;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GraphExpansion;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.predicates.Atom;
import com.apple.foundationdb.record.query.predicates.Lambda;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.RelOpValue;
import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Function
 * indexScan(STRING, ...) -> STREAM.
 */
@AutoService(BuiltInFunction.class)
public class ValueIndexScanFn extends BuiltInFunction<RelationalExpression> {
    public ValueIndexScanFn() {
        super("valueIndexScan",
                ImmutableList.of(Type.primitiveType(Type.TypeCode.STRING)), new Type.Function(), ValueIndexScanFn::encapsulate);
    }

    @SuppressWarnings("java:S3655")
    private static RelationalExpression encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<RelationalExpression> builtInFunction, @Nonnull final List<Atom> atoms) {
        final RecordMetaData recordMetaData = parserContext.getRecordMetaData();

        Verify.verify(!atoms.isEmpty());
        final Atom atom0 = atoms.get(0);
        SemanticException.check(atom0.getResultType().getTypeCode() == Type.TypeCode.STRING, "index name must be a literal string");
        SemanticException.check(atom0 instanceof Value, "index name must be a value");

        final Value argument0 = (Value)atom0;
        Object result = argument0.compileTimeEval(EvaluationContext.EMPTY);
        Verify.verify(result instanceof String);
        final String indexName = (String)result;

        // get the index definition from

        final Index index = recordMetaData.getIndex(indexName);
        final RecordType recordType = findRecordTypeForIndex(recordMetaData, indexName);
        final List<Descriptors.FieldDescriptor> fieldDescriptors = index.validate(Objects.requireNonNull(recordType).getDescriptor());

        int i = 0;
        for (final Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
            if (i + 1 >= atoms.size()) {
                break;
            }

            final Atom atomi = atoms.get(i + 1);
            SemanticException.check(atomi.getResultType().getTypeCode() == Type.TypeCode.FUNCTION, "function expected");
            SemanticException.check(atomi instanceof Lambda, "comparator lambda expected");
            final Lambda lambda = (Lambda)atomi;

            final Type.TypeCode typeCode = Type.TypeCode.fromProtobufType(fieldDescriptor.getType());
            Verify.verify(typeCode.isPrimitive()); // TODO what to do for messages that encode nullabilities

            final GraphExpansion graphExpansion =
                    lambda.unifyBody(ImmutableList.of(QuantifiedColumnValue.of(CorrelationIdentifier.UNGROUNDED, 0, Type.primitiveType(typeCode))));

            Verify.verify(graphExpansion.getQuantifiers().isEmpty());
            Verify.verify(graphExpansion.getPredicates().isEmpty());
            final List<? extends RelOpValue> resultsAs = graphExpansion.getResultsAs(RelOpValue.class);
            Verify.verify(resultsAs.size() == 1);

            final RelOpValue comparatorValue = resultsAs.get(0);
            final Optional<ValuePredicate> valuePredicateOptional = comparatorValue.toQueryPredicate(CorrelationIdentifier.UNGROUNDED);
            SemanticException.check(valuePredicateOptional.isPresent(), "unable to compile in comparator");
            final ValuePredicate valuePredicate = valuePredicateOptional.get();
            final Comparisons.Comparison comparison = valuePredicate.getComparison();

            i++;
        }


        final Set<String> allAvailableRecordTypes = recordMetaData.getRecordTypes().keySet();

        return new RecordQueryScanPlan(allAvailableRecordTypes,
                ScanComparisons.EMPTY, false, false);
    }

    @Nullable
    private static RecordType findRecordTypeForIndex(@Nonnull final RecordMetaData recordMetaData, @Nonnull final String indexName) {
        for (final RecordType recordType : recordMetaData.getRecordTypes().values()) {
            for (final Index index : recordType.getIndexes()) {
                if (indexName.equals(index.getName())) {
                    return recordType;
                }
            }
        }
        return null;
    }
}
