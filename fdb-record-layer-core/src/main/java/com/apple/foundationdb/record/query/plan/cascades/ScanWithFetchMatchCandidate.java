/*
 * ScanWithFetchMatchCandidate.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Interface to represent a candidate that replaces with an index scan.
 */
public interface ScanWithFetchMatchCandidate extends WithPrimaryKeyMatchCandidate {

    @Nonnull
    Optional<Value> pushValueThroughFetch(@Nonnull Value value,
                                          @Nonnull CorrelationIdentifier sourceAlias,
                                          @Nonnull CorrelationIdentifier targetAlias);

    @Nonnull
    static Optional<Value> pushValueThroughFetch(@Nonnull final Value toBePushedValue,
                                                 @Nonnull final CorrelationIdentifier baseAlias,
                                                 @Nonnull final CorrelationIdentifier sourceAlias,
                                                 @Nonnull final CorrelationIdentifier targetAlias,
                                                 @Nonnull final Iterable<? extends Value> providedValuesFromIndex) {
        if (!(toBePushedValue instanceof FieldValue || toBePushedValue instanceof RecordConstructorValue)) {
            // At the moment, we can only push field and record value operations through fetches. Once this is fixed,
            // we can expand the list of push-able values
            return Optional.empty();
        }

        final AliasMap equivalenceMap = AliasMap.of(sourceAlias, baseAlias);
        final AliasMap toTargetAliasMap = AliasMap.of(sourceAlias, targetAlias);

        final var translatedValueOptional =
                toBePushedValue.<Value>mapMaybe((value, mappedChildren) -> {
                    for (final var providedValue : providedValuesFromIndex) {
                        if (value.semanticEquals(providedValue, equivalenceMap)) {
                            return value.withChildren(mappedChildren)
                                    .rebase(toTargetAliasMap);
                        }
                    }
                    return value.withChildren(mappedChildren); // this may be correlated to sourceAlias
                });

        // the translation was successful if the translated value is not correlated to sourceAlias anymore
        return translatedValueOptional.filter(translatedValue -> !translatedValue.getCorrelatedTo().contains(sourceAlias));
    }
}
