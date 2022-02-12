/*
 * InterestingPropertiesMap.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.google.common.base.Verify;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A map to keep track of interesting physical properties for an expression reference. References ({@link ExpressionRef})
 * are a central part of the memoization effort within the {@link CascadesPlanner}. Member expressions contained in
 * a reference all yield the same final result not taking into account physical properties such as sorted-ness,
 * existence of duplicates, etc. A set of plan members of a reference can be partitioned into subsets by physical
 * property in order to be used by conscious rules that are dependent on a particular set of properties. Many times
 * we need to create all possible plans in the anticipation that these plans are all consumed by other implementation
 * rules later on. That is not guaranteed, though, and in reality a large set of plans does not get used due to an
 * incompatible physical property. In any case, the planner must explore plan variations once created even though
 * the resulting structures may not get used at all. In essence, we would like to avoid the creation of useless plans
 * if at all possible.
 *
 * A map of interesting properties is used by a reference to "push down" such an interesting property through the data
 * flow graph. The "pushing down" is facilitated by rules that are executed by the {@link CascadesPlanner} much like
 * other rules yielding new {@link RelationalExpression}s or {@link PartialMatch}es. The only, and very important,
 * difference in concept is that the information is passed downwards instead of bubbling up like all the other structures.
 *
 * The planner needs to make sure that expression references are explored, and if necessary, re-explored if new
 * interesting properties are declared through the execution of other planner tasks.
 * Planner rules ({@link CascadesRuleCall}) can declare to interest in the change of a property which then signals to the
 * planner to trigger a re-exploration using that rule if an interesting property changes on that reference.
 *
 * The map implementation uses a logical clock and a watermark system to understand if a re-exploration becomes
 * necessary.
 */
public class InterestingPropertiesMap {
    /**
     * Current tick of the map. Gets updated though {@link #bumpTick()} and indicates that a change in the properties
     * map needs to trigger (re-)exploration of appropriate rules.
     */
    private long currentTick;

    /**
     * The watermark goal tick. This tick is between the {@link #watermarkCommittedTick} and the {@link #currentTick}.
     * This needs to be tracked as this tick is used to understand that a particular change to the interesting properties
     * has caused a proper (re-)exploration of the affected sub dag. If other pushes happen through other rules,
     * {@link #currentTick} will get further bumped and needs to cause further exploration. This tick tells that
     * algorithm that exploration has at least happened to the goal tick.
     */
    private long watermarkGoalTick;

    /**
     * This is the committed tick. All exploration is up-to-date to at least that tick.
     */
    private long watermarkCommittedTick;

    /**
     * The map for the interesting properties of a reference. Note that this map is stable with respect to iteration.
     */
    private final Map<PlannerAttribute<?>, AttributeEntry> attributeToInterestingPropertiesMap;

    /**
     * Default constructor.
     */
    public InterestingPropertiesMap() {
        this.currentTick = 0L;
        this.watermarkGoalTick = -1L;
        this.watermarkCommittedTick = -1L;
        this.attributeToInterestingPropertiesMap = Maps.newLinkedHashMap();
    }

    /**
     * Method to return if a particular attribute is contained in the map.
     * @param attribute the attribute key to check
     * @param <T> the type of the attribute
     * @return {@code true} is the attribute is contained in this map, {@code false} otherwise.
     */
    public <T> boolean containsAttribute(@Nonnull final PlannerAttribute<T> attribute) {
        return attributeToInterestingPropertiesMap.containsKey(attribute);
    }

    /**
     * Method to return an optional interesting property given an attribute key passed in.
     * @param attribute the attribute key
     * @param <T> the type of the attribute
     * @return {@code Optional.of(property)} if this map contains the property selected by the attribute key handed in,
     *         {@code Optional.empty()} otherwise
     */
    @Nonnull
    public <T> Optional<T> getPropertyOptional(@Nonnull final PlannerAttribute<T> attribute) {
        if (containsAttribute(attribute)) {
            return Optional.of(attribute.narrow(attributeToInterestingPropertiesMap.get(attribute).property));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Method to return an interesting property given an attribute key passed in or to throw an exception.
     * @param attribute the attribute key
     * @param exceptionSupplier a supplier to be called if an attribute could not be found
     * @param <T> the type of the attribute
     * @param <X> the type of throwable to throw if an attribute cannot be found
     * @return the property if this map contains the attribute given by the key handed in
     * @throws X the exception generated by the supplier passed in
     */
    @Nonnull
    public <T, X extends Throwable> T getPropertyOrThrow(@Nonnull final PlannerAttribute<T> attribute, @Nonnull final Supplier<? extends X> exceptionSupplier) throws X {
        if (containsAttribute(attribute)) {
            return attribute.narrow(attributeToInterestingPropertiesMap.get(attribute).property);
        } else {
            throw exceptionSupplier.get();
        }
    }

    /**
     * Method to return an optional interesting property given an attribute key passed in or an alternative default
     * property.
     * @param attribute the attribute key
     * @param defaultProperty a property to be returned if an attribute could not be found
     * @param <T> the type of the attribute
     * @return the property contained in this map if there is such a property as addressed by the attribute key passed
     *         in or {@code defaultProperty} if this map does not contain the attribute.
     */
    @Nonnull
    public <T> T getPropertyOrElse(@Nonnull final PlannerAttribute<T> attribute, @Nonnull final T defaultProperty) {
        if (containsAttribute(attribute)) {
            return attribute.narrow(attributeToInterestingPropertiesMap.get(attribute).property);
        } else {
            return defaultProperty;
        }
    }

    /**
     * Push an interesting property into this map. The new property may get added to the interesting properties map as is
     * if this is the first time a property of that kind was added. If there is a pre-existing property (wit respect to
     * the attribute key) already contained in the map, we delegate to {@link PlannerAttribute#combine(Object, Object)}
     * to properly combine the existing and the new property. If the new property is distinct from the old one
     * (or there is no prior property determined the given attribnute key), we update the internal state in order to
     * cause (re-)exploration of the current reference.
     * @param attribute the attribute key
     * @param property the new property to be pushed
     * @param <T> the type of the attribute
     * @return an optional containing the combined property if the push operation was successful, or
     *         {@code Optional.empty()} otherwise.
     */
    public <T> Optional<T> pushProperty(@Nonnull final PlannerAttribute<T> attribute, @Nonnull T property) {
        if (containsAttribute(attribute)) {
            final AttributeEntry attributeEntry = attributeToInterestingPropertiesMap.get(attribute);
            return attribute.combine(attribute.narrow(attributeEntry.property), property)
                    .map(combinedProperty -> {
                        bumpTick();
                        attributeEntry.property = combinedProperty;
                        attributeEntry.lastUpdatedTick = currentTick;
                        return combinedProperty;
                    });
        } else {
            bumpTick();
            attributeToInterestingPropertiesMap.put(attribute, new AttributeEntry(property));
            return Optional.of(property);
        }
    }

    /**
     * Method to establish whether this map is explored.
     * @return {@code true} if further (re-)exploration is required, {@code false} otherwise.
     */
    public boolean isExplored() {
        Verify.verify(watermarkGoalTick <= currentTick);
        return watermarkGoalTick == currentTick;
    }

    /**
     * This method indicates whether the reference containing this map is currently being explored. That means that
     * the reference has started (re-)exploration but has not yet finished it.
     * @return {@code true} if the associated reference is currently being explored, {@code false} otherwise.
     */
    public boolean isExploring() {
        return watermarkGoalTick > watermarkCommittedTick;
    }

    /**
     * This method indicates whether the reference containing this map is currently being explored for the first time (a full
     * exploration). That means that the reference has started exploration but has not yet finished it.
     * @return {@code true} if the associated reference is currently being fully explored, {@code false} otherwise.
     */
    public boolean isFullyExploring() {
        return watermarkCommittedTick < 0 && isExploring();
    }

    /**
     * This method indicates whether the reference containing this map is currently being explored because of a change to
     * a properties as defined by a set of attributes passed in. That means that the reference has started exploration
     * but has not yet finished it.
     * @param attributes a set of attribute keys
     * @return {@code true} if the associated reference is currently being fully explored, {@code false} otherwise.
     */
    public boolean isExploredForAttributes(@Nonnull final Set<PlannerAttribute<?>> attributes) {
        if (watermarkCommittedTick < 0) {
            // never been planned
            return false;
        } else {
            for (final PlannerAttribute<?> interestingKey : attributes) {
                if (containsAttribute(interestingKey)) {
                    final AttributeEntry attributeEntry = attributeToInterestingPropertiesMap.get(interestingKey);
                    if (attributeEntry.lastUpdatedTick > watermarkCommittedTick) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    /**
     * Method to be called when exploration of a group is started.
     */
    public void startExploration() {
        watermarkGoalTick = currentTick;
    }

    /**
     * Method to be called when exploration of a group is completed.
     */
    public void commitExploration() {
        watermarkCommittedTick = watermarkGoalTick;
    }

    @CanIgnoreReturnValue
    private long bumpTick() {
        return ++currentTick;
    }

    private class AttributeEntry {
        private long lastUpdatedTick;
        @Nonnull
        private Object property;

        public AttributeEntry(@Nonnull final Object property) {
            this.lastUpdatedTick = currentTick;
            this.property = property;
        }
    }
}
