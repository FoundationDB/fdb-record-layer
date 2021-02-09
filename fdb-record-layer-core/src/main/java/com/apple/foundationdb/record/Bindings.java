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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
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
        CORRELATION("__corr_");

        public static final String PREFIX = "__";
        private final String value;


        Internal(String value) {
            this.value = value;
        }

        public static boolean isInternal(@Nonnull String name) {
            return name.startsWith(PREFIX);
        }

        public String bindingName(@Nonnull String suffix) {
            return value + suffix;
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

    public static Builder newBuilder() {
        return new Builder(null);
    }

    public Builder childBuilder() {
        return new Builder(this);
    }

    /**
     * A builder for {@link Bindings}.
     * <pre><code>
     * Bindings.newBuilder().set("x", x).set("y", y).build()
     * </code></pre>
     */
    public static class Builder {
        @Nonnull
        private Bindings bindings;
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
