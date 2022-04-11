/*
 * Attribute.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.explain;

import javax.annotation.Nonnull;

/**
 * Basic interface for all attributes of {@link AbstractPlannerGraph.AbstractNode}
 * as well as {@link AbstractPlannerGraph.AbstractEdge}.
 *
 * Represents a tag object that annotates an {@link AbstractPlannerGraph.AbstractNode} or an
 * {@link AbstractPlannerGraph.AbstractEdge}, providing additional information to a {@link GraphExporter}.
 */
public interface Attribute {
    /**
     * Returns whether this method is visible in the given context.
     * @param context the exporter context of the exporter being used
     * @param <N> node type
     * @param <E> edge type
     * @return {@code true} if the attribute is semantic, {@code false} otherwise.
     */
    <N, E> boolean isVisible(@Nonnull GraphExporter<N, E>.ExporterContext context);

    /**
     * Return the underlying object this attribute refers to.
     * @return the object.
     */
    @Nonnull
    Object getReference();

    /**
     * Basic interface all attributes of {@link AbstractPlannerGraph.AbstractNode}
     * as well as {@link AbstractPlannerGraph.AbstractEdge} that are use to serialize to GML.
     */
    interface GmlAttribute extends Attribute {
        @Override
        default <N, E> boolean isVisible(@Nonnull final GraphExporter<N, E>.ExporterContext context) {
            return context.getExporter() instanceof GmlExporter;
        }
    }

    /**
     * Interface for attributes of {@link AbstractPlannerGraph.AbstractNode}
     * as well as {@link AbstractPlannerGraph.AbstractEdge} that are used to serialize to DOT.
     */
    interface DotAttribute extends Attribute {
        @Override
        default <N, E> boolean isVisible(@Nonnull final GraphExporter<N, E>.ExporterContext context) {
            return context.getExporter() instanceof DotExporter;
        }
    }

    /**
     * Interface for attributes of {@link AbstractPlannerGraph.AbstractNode}
     * as well as {@link AbstractPlannerGraph.AbstractEdge} that are used to serialize to GML or to DOT.
     */
    interface CommonAttribute extends GmlAttribute, DotAttribute {
        @Override
        default <N, E> boolean isVisible(@Nonnull final GraphExporter<N, E>.ExporterContext context) {
            return true;
        }
    }

    /**
     * Interface for attributes of {@link AbstractPlannerGraph.AbstractNode}
     * as well as {@link AbstractPlannerGraph.AbstractEdge} that are used to neither serialize to GML or to DOT.
     * Attributes of this kind can only be referred to via variable substitution.
     */
    interface InvisibleAttribute extends GmlAttribute, DotAttribute {
        @Override
        default <N, E> boolean isVisible(@Nonnull final GraphExporter<N, E>.ExporterContext context) {
            return false;
        }
    }

    /**
     * Static factory method to create a GML attribute based on a reference to some object.
     * @param reference the reference
     * @return a new attribute that is only visible to the GML exporter
     */
    @Nonnull
    static GmlAttribute gml(@Nonnull final Object reference) {
        return new GmlAttribute() {
            @Override
            @Nonnull
            public Object getReference() {
                return reference;
            }

            @Override
            @Nonnull
            public String toString() {
                return getReference().toString();
            }
        };
    }

    /**
     * Static factory method to create a DOT attribute based on a reference to some object.
     * @param reference the reference
     * @return a new attribute that is only visible to the DOT exporter
     */
    @Nonnull
    static DotAttribute dot(@Nonnull final Object reference) {
        return new DotAttribute() {
            @Override
            @Nonnull
            public Object getReference() {
                return reference;
            }

            @Override
            @Nonnull
            public String toString() {
                return getReference().toString();
            }
        };
    }

    /**
     * Static factory method to create an attribute based on a reference to some object.
     * @param reference the reference
     * @return a new attribute that is visible to all exporters
     */
    @Nonnull
    static CommonAttribute common(@Nonnull final Object reference) {
        return new CommonAttribute() {
            @Override
            @Nonnull
            public Object getReference() {
                return reference;
            }

            @Override
            @Nonnull
            public String toString() {
                return getReference().toString();
            }
        };
    }

    /**
     * Static factory method to create an attribute based on a reference to some object.
     * @param reference the reference
     * @return a new attribute that is invisible to all exporters. Attributes of this kind can only be used in variable
     *         substitutions.
     */
    @Nonnull
    static InvisibleAttribute invisible(@Nonnull final Object reference) {
        return new InvisibleAttribute() {
            @Override
            @Nonnull
            public Object getReference() {
                return reference;
            }

            @Override
            @Nonnull
            public String toString() {
                return getReference().toString();
            }
        };
    }
}
