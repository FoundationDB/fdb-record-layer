/*
 * CollateFunctionKeyExpression.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.common.text.TextCollator;
import com.apple.foundationdb.record.provider.common.text.TextCollatorRegistry;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * {@code COLLATE} function.
 *
 * Turns a string into a locale-specific sort key.
 *
 * <p>
 * The name of the function is determined by the underlying collation rules chosen.
 * For the Java Platform's own collations, this is {@code collate_jre}.
 *
 * <p>
 * The function takes the following arguments:<ol>
 * <li>text field to be collated (mandatory)</li>
 * <li>collation to use (optional)</li>
 * <li>strength of character matching (optional)</li>
 * </ol>
 *
 * <p>
 * For example,
 * <pre>
 * Key.Expressions.function("collate_jre")
 * </pre>
 * is the default locale (from the JVM) and the default (weakest -- ignoring almost all character modifiers) strength.
 *
 * <pre>
 * Key.Expressions.function("collate_jre", Key.Expressions.concat(Key.Expressions.field("text_field"),
 *     Key.Expressions.value("fr_CA"), Key.Expressions.value(TextCollator.Strength.SECONDARY)))
 * </pre>
 * is case-insensitive, accent-sensitive, Canadian French.
 *
 * @see TextCollator
 */
@API(API.Status.EXPERIMENTAL)
public class CollateFunctionKeyExpression extends FunctionKeyExpression implements QueryableKeyExpression {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Collate-Function-Key-Expression");

    @Nonnull
    private final TextCollatorRegistry collatorRegistry;
    @Nullable
    private TextCollator invariableCollator;

    protected CollateFunctionKeyExpression(@Nonnull TextCollatorRegistry collatorRegistry,
                                           @Nonnull String name, @Nonnull KeyExpression arguments) {
        super(name, arguments);
        this.collatorRegistry = collatorRegistry;
        this.invariableCollator = getInvariableCollator(collatorRegistry, arguments);
    }

    @Nullable
    protected static TextCollator getInvariableCollator(@Nonnull TextCollatorRegistry collatorRegistry,
                                                        @Nonnull KeyExpression arguments) {
        // If the locale and strength are missing or literals, we can lookup immediately.
        if (arguments.getColumnSize() < 2) {
            return collatorRegistry.getTextCollator();
        }
        if (arguments instanceof ThenKeyExpression) {
            final List<KeyExpression> children = ((ThenKeyExpression)arguments).getChildren();
            if (children.size() > 1 && !(children.get(1) instanceof LiteralKeyExpression<?>)) {
                return null;
            }
            if (children.size() > 2 && !(children.get(2) instanceof LiteralKeyExpression<?>)) {
                return null;
            }
            String locale = null;
            Integer strength = null;
            if (children.size() > 1) {
                Object literal1 = ((LiteralKeyExpression<?>)children.get(1)).getValue();
                if (literal1 instanceof String) {
                    locale = (String)literal1;
                } else if (literal1 != null) {
                    return null;
                }
            }
            if (children.size() > 2) {
                Object literal2 = ((LiteralKeyExpression<?>)children.get(2)).getValue();
                if (literal2 instanceof Number) {
                    strength = ((Number)literal2).intValue();
                } else if (literal2 != null) {
                    return null;
                }
            }
            if (locale == null) {
                return strength == null ? collatorRegistry.getTextCollator() : collatorRegistry.getTextCollator(strength);
            } else {
                return strength == null ? collatorRegistry.getTextCollator(locale) : collatorRegistry.getTextCollator(locale, strength);

            }
        }
        return null;
    }

    protected TextCollator getTextCollator(@Nonnull Key.Evaluated arguments) {
        if (invariableCollator != null) {
            return invariableCollator;
        }
        if (arguments.size() < 2) {
            return collatorRegistry.getTextCollator();
        }
        final String locale = arguments.getString(1);
        if (arguments.size() < 3) {
            return locale == null ? collatorRegistry.getTextCollator() : collatorRegistry.getTextCollator(locale);
        }
        final int strength = (int)arguments.getLong(2);
        return locale == null ? collatorRegistry.getTextCollator(strength) : collatorRegistry.getTextCollator(locale, strength);
    }

    @Override
    public int getMinArguments() {
        return 1;
    }

    @Override
    public int getMaxArguments() {
        return 3;
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> record,
                                                                    @Nullable Message message,
                                                                    @Nonnull Key.Evaluated arguments) {
        final String value = arguments.getString(0);
        if (value == null) {
            return Collections.singletonList(Key.Evaluated.NULL);
        }

        final TextCollator textCollator = getTextCollator(arguments);
        return Collections.singletonList(Key.Evaluated.scalar(textCollator.getKey(value)));
    }

    @Override
    public boolean createsDuplicates() {
        return getArguments().createsDuplicates();
    }

    @Override
    public int getColumnSize() {
        return 1;
    }

    @Nonnull
    @Override
    public Value toValue(@Nonnull final CorrelationIdentifier baseAlias,
                         @Nonnull final Type baseType,
                         @Nonnull final List<String> fieldNamePrefix) {
        // TODO support this
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Function<Object, Object> getComparandConversionFunction() {
        final TextCollator textCollator = invariableCollator;
        if (textCollator == null) {
            throw new MetaDataException("can only be used in queries when collation name is constant");
        }
        return o -> textCollator.getKey((String)o);
    }

    @Override
    public int planHash(@Nonnull final PlanHashable.PlanHashMode mode) {
        return super.basePlanHash(mode, BASE_HASH);
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return super.baseQueryHash(hashKind, BASE_HASH);
    }
}
