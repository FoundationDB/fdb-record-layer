/*
 * IndexMaintainerFactoryRegistryImplTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.provider.foundationdb.indexes.ValueIndexMaintainerFactory;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Tests for {@link IndexMaintainerFactoryRegistryImpl}.
 */
class IndexMaintainerFactoryRegistryImplTest {

    @Test
    void factoriesClaimingDistinctTypesAreAllRegistered() {
        final var valueFactory = new StubFactory("stub_value");
        final var rankFactory = new StubFactory("stub_rank", "stub_rank_long");

        final var registry = IndexMaintainerFactoryRegistryImpl.buildRegistry(
                ImmutableList.of(valueFactory, rankFactory));

        assertThat(registry).containsOnlyKeys("stub_value", "stub_rank", "stub_rank_long");
        assertThat(registry.get("stub_value")).isSameAs(valueFactory);
        assertThat(registry.get("stub_rank")).isSameAs(rankFactory);
        assertThat(registry.get("stub_rank_long")).isSameAs(rankFactory);
    }

    @Test
    void twoFactoriesClaimingTheSameTypeAreRejected() {
        final var thrown = catchThrowableOfType(RecordCoreException.class,
                () -> IndexMaintainerFactoryRegistryImpl.buildRegistry(
                        ImmutableList.of(new StubFactory("stub_value"), new StubFactory("stub_value"))));

        assertThat(thrown).hasMessageContaining("duplicate index maintainer factory");
        assertThat(thrown.getLogInfo())
                .containsEntry(LogMessageKeys.INDEX_TYPE.toString(), "stub_value");
    }

    @Test
    void oneFactoryClaimingTheSameTypeTwiceIsRejected() {
        final var thrown = catchThrowableOfType(RecordCoreException.class,
                () -> IndexMaintainerFactoryRegistryImpl.buildRegistry(
                        ImmutableList.of(new StubFactory("stub_value", "stub_value"))));

        assertThat(thrown).hasMessageContaining("duplicate index maintainer factory");
        assertThat(thrown.getLogInfo())
                .containsEntry(LogMessageKeys.INDEX_TYPE.toString(), "stub_value");
    }

    @Test
    void theRegistryOnTheClassPathBuilds() {
        final var index = new Index("stub_index", field("field"), IndexTypes.VALUE);

        assertThat(IndexMaintainerFactoryRegistryImpl.instance().getIndexMaintainerFactory(index))
                .isInstanceOf(ValueIndexMaintainerFactory.class);
    }

    private static final class StubFactory implements IndexMaintainerFactory {
        @Nonnull
        private final List<String> indexTypes;

        StubFactory(@Nonnull final String... indexTypes) {
            this.indexTypes = ImmutableList.copyOf(indexTypes);
        }

        @Nonnull
        @Override
        public Iterable<String> getIndexTypes() {
            return indexTypes;
        }

        @Nonnull
        @Override
        public IndexValidator getIndexValidator(final Index index) {
            throw new UnsupportedOperationException("not used");
        }

        @Nonnull
        @Override
        public IndexMaintainer getIndexMaintainer(@Nonnull final IndexMaintainerState state) {
            throw new UnsupportedOperationException("not used");
        }
    }
}
