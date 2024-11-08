/*
 * Bindings.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.planprotos.PParameterComparison.PBindingKind;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A map of bound parameter values passed to query evaluation.
 * <p>
 * A binding map can also have a parent from which values are taken if they are absent in the child.
 * </p>
 */
@API(API.Status.MAINTAINED)
public class Bindings {

    /**
     * Bindings slots used internally by plan operators.
     */
    public enum Internal {
        IN("__in_"),
        RANK("__rank_"),
        CORRELATION("__corr_"),
        CONSTANT("__const_");

        public static final String PREFIX = "__";
        private final String value;


        Internal(String value) {
            this.value = value;
        }

        public static boolean isInternal(@Nonnull String name) {
            return name.startsWith(PREFIX);
        }

        public boolean isOfType(@Nonnull String name) {
            return name.startsWith(value);
        }

        public String bindingName(@Nonnull String suffix) {
            return value + suffix;
        }

        public String identifier(@Nonnull String bindingName) {
            Verify.verify(bindingName.startsWith(value));
            return bindingName.substring(value.length());
        }

        @Nonnull
        @SuppressWarnings("unused")
        public PBindingKind toProto(@Nonnull final PlanSerializationContext serializationContext) {
            switch (this) {
                case IN:
                    return PBindingKind.IN;
                case RANK:
                    return PBindingKind.RANK;
                case CONSTANT:
                    return PBindingKind.CONSTANT;
                case CORRELATION:
                    return PBindingKind.CORRELATION;
                default:
                    throw new RecordCoreException("unknown binding mapping. did you forget to map it?");
            }
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static Internal fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PBindingKind bindingKindProto) {
            switch (bindingKindProto) {
                case IN:
                    return IN;
                case RANK:
                    return RANK;
                case CONSTANT:
                    return CONSTANT;
                case CORRELATION:
                    return CORRELATION;
                default:
                    throw new RecordCoreException("unknown binding mapping. did you forget to map it?");
            }
        }
    }

    @Nonnull
    private final Map<String, Object> values;
    @Nullable
    private final Bindings parent;

    public static final Bindings EMPTY_BINDINGS = new Bindings();

    private Bindings(@Nullable Bindings parent) {
        this.values = new HashMap<>();
        this.parent = parent;
    }

    public Bindings() {
        this(null);
    }
    
    @Nullable
    public Object get(@Nonnull String name) {
        if (values.containsKey(name)) {
            return values.get(name);
        } else if (parent != null) {
            return parent.get(name);
        } else {
            throw new RecordCoreException("Missing binding for " + name);
        }
    }

    public boolean containsBinding(@Nonnull String name) {
        if (values.containsKey(name)) {
            return true;
        } else if (parent != null) {
            return parent.containsBinding(name);
        } else {
            return false;
        }
    }

    public static Builder newBuilder() {
        return new Builder(null);
    }

    public Builder childBuilder() {
        return new Builder(this);
    }

    @Nonnull
    public List<Map.Entry<String, Object>> asMappingList() {
        final ImmutableList.Builder<Map.Entry<String, Object>> resultBuilder = ImmutableList.builder();
        values.forEach((key, value) -> resultBuilder.add(Pair.of(key, value)));
        if (parent != null) {
            resultBuilder.addAll(parent.asMappingList());
        }
        return resultBuilder.build();
    }

    @Override
    public String toString() {
        return "Bindings(" + asMappingList() + ")";
    }

    /**
     * A builder for {@link Bindings}.
     * <pre><code>
     * Bindings.newBuilder().set("x", x).set("y", y).build()
     * </code></pre>
     */
    public static class Builder {
        @Nonnull
        private final Bindings bindings;
        private boolean built;

        private Builder(@Nullable Bindings parent) {
            this.bindings = new Bindings(parent);
        }

        @Nullable
        public Object get(@Nonnull String name) {
            return bindings.get(name);
        }

        public Builder set(@Nonnull String name, @Nullable Object value) {
            if (built) {
                throw new RecordCoreException("Cannot change bindings after building");
            }
            if (bindings.values.containsKey(name)) {
                throw new RecordCoreException("Duplicate binding for " + name);
            }
            bindings.values.put(name, value);
            return this;
        }

        @Nonnull
        public Bindings build() {
            built = true;
            return bindings;
        }
    }

}
