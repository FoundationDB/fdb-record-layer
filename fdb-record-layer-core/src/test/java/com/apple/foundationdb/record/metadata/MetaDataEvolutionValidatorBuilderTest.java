/*
 * MetaDataEvolutionValidatorBuilderTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of the {@link MetaDataEvolutionValidator.Builder} class. These tests are mainly present to make sure
 * that the various methods we have to tweak the behavior of the {@link MetaDataEvolutionValidator} actually
 * set the values. These tests make sure that if the value is set on the builder, it is also set on the
 * {@link MetaDataEvolutionValidator}, and that if the validator is turned back into a builder, the value
 * is preserved. The tests within {@link MetaDataEvolutionValidatorTest} should be consulted for validating
 * actual behavior.
 */
class MetaDataEvolutionValidatorBuilderTest {

    private <T> void testSettingOption(@Nonnull String name,
                                       @Nonnull BiConsumer<MetaDataEvolutionValidator.Builder, ? super T> setter,
                                       @Nonnull Function<MetaDataEvolutionValidator.Builder, ? extends T> getterFromBuilder,
                                       @Nonnull Function<MetaDataEvolutionValidator, ? extends T> getterFromValidator,
                                       @Nonnull List<? extends T> values) {
        final MetaDataEvolutionValidator.Builder builder = MetaDataEvolutionValidator.newBuilder();
        T defaultValue = values.get(0);
        assertThat(getterFromBuilder.apply(builder))
                .as("unexpected default value from builder for field %s", name)
                .isEqualTo(defaultValue);
        assertThat(getterFromValidator.apply(MetaDataEvolutionValidator.getDefaultInstance()))
                .as("unexpected default value from validator for field %s", name)
                .isEqualTo(defaultValue);
        for (T value : values) {
            setter.accept(builder, value);
            assertThat(getterFromBuilder.apply(builder))
                    .as("field %s on the meta-data evolution validator builder should have reflected value change", name)
                    .isEqualTo(value);
            final MetaDataEvolutionValidator validator = builder.build();
            assertThat(getterFromValidator.apply(validator))
                    .as("field %s on the meta-data evolution validator should have reflected value change", name)
                    .isEqualTo(value);
            final MetaDataEvolutionValidator.Builder builderAgain = validator.asBuilder();
            assertThat(getterFromBuilder.apply(builderAgain))
                    .as("field %s on the rebuilt meta-data evolution validator builder should have reflected value change", name)
                    .isEqualTo(value);
        }
    }

    private void testSettingBooleanOption(@Nonnull String name,
                                          @Nonnull BiConsumer<MetaDataEvolutionValidator.Builder, Boolean> setter,
                                          @Nonnull Function<MetaDataEvolutionValidator.Builder, Boolean> getterFromBuilder,
                                          @Nonnull Function<MetaDataEvolutionValidator, Boolean> getterFromValidator) {
        testSettingOption(name, setter, getterFromBuilder, getterFromValidator, List.of(false, true));
    }

    @Test
    void indexValidatorRegistry() {
        final IndexValidatorRegistry testRegistry = IndexValidator::new;
        testSettingOption("indexValidatorRegistry",
                MetaDataEvolutionValidator.Builder::setIndexValidatorRegistry,
                MetaDataEvolutionValidator.Builder::getIndexValidatorRegistry,
                MetaDataEvolutionValidator::getIndexValidatorRegistry,
                List.of(IndexMaintainerFactoryRegistryImpl.instance(), testRegistry));
    }

    @Test
    void allowNoVersionChanges() {
        testSettingBooleanOption("allowNoVersionChanges",
                MetaDataEvolutionValidator.Builder::setAllowNoVersionChange,
                MetaDataEvolutionValidator.Builder::allowsNoVersionChange,
                MetaDataEvolutionValidator::allowsNoVersionChange);
    }

    @Test
    void allowNoSinceVersion() {
        testSettingBooleanOption("allowNoSinceVersion",
                MetaDataEvolutionValidator.Builder::setAllowNoSinceVersion,
                MetaDataEvolutionValidator.Builder::allowsNoSinceVersion,
                MetaDataEvolutionValidator::allowsNoSinceVersion);
    }

    @Test
    void allowIndexRebuilds() {
        testSettingBooleanOption("allowIndexRebuilds",
                MetaDataEvolutionValidator.Builder::setAllowIndexRebuilds,
                MetaDataEvolutionValidator.Builder::allowsIndexRebuilds,
                MetaDataEvolutionValidator::allowsIndexRebuilds);
    }

    @Test
    void allowMissingFormerIndexNames() {
        testSettingBooleanOption("allowMissingFormerIndexNames",
                MetaDataEvolutionValidator.Builder::setAllowMissingFormerIndexNames,
                MetaDataEvolutionValidator.Builder::allowsMissingFormerIndexNames,
                MetaDataEvolutionValidator::allowsMissingFormerIndexNames);
    }

    @Test
    void allowOlderFormerIndexAddedVersion() {
        testSettingBooleanOption("allowOlderFormerIndexAddedVersion",
                MetaDataEvolutionValidator.Builder::setAllowOlderFormerIndexAddedVerions,
                MetaDataEvolutionValidator.Builder::allowsOlderFormerIndexAddedVersions,
                MetaDataEvolutionValidator::allowsOlderFormerIndexAddedVersions);
    }

    @Test
    void allowUnsplitToSplit() {
        testSettingBooleanOption("allowUnsplitToSplit",
                MetaDataEvolutionValidator.Builder::setAllowUnsplitToSplit,
                MetaDataEvolutionValidator.Builder::allowsUnsplitToSplit,
                MetaDataEvolutionValidator::allowsUnsplitToSplit);
    }

    @Test
    void disallowTypeRenames() {
        testSettingBooleanOption("disallowTypeRenames",
                MetaDataEvolutionValidator.Builder::setDisallowTypeRenames,
                MetaDataEvolutionValidator.Builder::disallowsTypeRenames,
                MetaDataEvolutionValidator::disallowsTypeRenames);
    }
}
