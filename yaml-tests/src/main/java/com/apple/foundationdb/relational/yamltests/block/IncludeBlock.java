/*
 * IncludeBlock.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests.block;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamsqlReference;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.resolver.Resolver;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A block that is used to import other yamsql file.
 */
@SuppressWarnings({"PMD.GuardLogStatement", "PMD.AvoidCatchingThrowable"})
public class IncludeBlock extends SupportBlock {

    public static final String INCLUDE = "include";
    private static final Logger logger = LogManager.getLogger(IncludeBlock.class);
    private static final Yaml YAML_ENGINE;

    static {
        LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setAllowDuplicateKeys(true);
        DumperOptions dumperOptions = new DumperOptions();

        YAML_ENGINE = new Yaml(new CustomYamlConstructor(loaderOptions), new Representer(dumperOptions),
                new DumperOptions(), loaderOptions, new Resolver());
    }

    private IncludeBlock() {

    }

    @Nonnull
    public static List<Block> parse(@Nonnull final YamsqlReference reference, @Nonnull final Object document,
                                    @Nonnull final YamlExecutionContext executionContext) {
        try {
            final var resourceName = Matchers.string(document, "resource name");
            return parse(reference.newResource(Objects.requireNonNull(resourceName)), executionContext);
        } catch (Throwable e) {
            throw YamlExecutionContext.wrapContext(e, () -> "Error parsing block at line " + reference, INCLUDE, reference);
        }
    }

    @Nonnull
    public static List<Block> parse(@Nonnull final YamsqlReference.YamsqlResource resource, @Nonnull final YamlExecutionContext executionContext)
            throws Exception {
        try {
            final var allBlocks = ImmutableList.<Block>builder();
            final var finalizingBlocks = ImmutableList.<Block>builder();
            int blockNumber = 0;
            executionContext.registerResource(resource);
            try (var inputStream = getInputStream(resource)) {
                final var docs = StreamSupport.stream(YAML_ENGINE.loadAll(inputStream).spliterator(), false).collect(Collectors.toList());
                for (var doc : docs) {
                    final var blocks = Block.parse(resource, doc, blockNumber, executionContext, resource.isTopLevel());
                    allBlocks.addAll(blocks);
                    for (final var block: blocks) {
                        finalizingBlocks.addAll(block.getAndClearFinalizingBlocks());
                    }
                    blockNumber++;
                }
            }
            allBlocks.addAll(finalizingBlocks.build());
            return allBlocks.build();
        } catch (RelationalException | IOException e) {
            logger.error("‼️ running test file '{}' was not successful", resource.getPath(), e);
            throw e;
        }
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    private static InputStream getInputStream(@Nonnull final YamsqlReference.YamsqlResource resource) throws RelationalException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(resource.getPath());
        Assert.notNull(inputStream, String.format(Locale.ROOT, "could not find '%s' in resources bundle", resource.getPath()));
        return inputStream;
    }
}
