/*
 * YamlRunner.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.cli.CliCommandFactory;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.Mark;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.resolver.Resolver;

import javax.annotation.Nonnull;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

@SuppressWarnings({"PMD.GuardLogStatement"}) // It already is, but PMD is confused and reporting error in unrelated locations.
public final class YamlRunner implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(YamlRunner.class);

    @Nonnull
    final String resourcePath;

    @Nonnull
    private final InputStream inputStream;
    private final CliCommandFactory cliCommandFactory;

    public YamlRunner(@Nonnull String resourcePath, @Nonnull CliCommandFactory commandFactory) throws RelationalException {
        this.resourcePath = resourcePath;
        this.inputStream = getInputStream(resourcePath);
        this.cliCommandFactory = commandFactory;
    }

    private static class CustomTagsInject extends SafeConstructor {

        private boolean recursing;

        public CustomTagsInject() {
            yamlConstructors.put(new Tag("!ignore"), new ConstructIgnore());
            yamlConstructors.put(new Tag("!l"), new CustomTagsInject.ConstructLong());
            yamlConstructors.put(new Tag("!sc"), new CustomTagsInject.ConstructStringContains());
            yamlConstructors.put(new Tag("!null"), new CustomTagsInject.ConstructNullPlaceholder());
            yamlConstructors.put(new Tag("!not_null"), new CustomTagsInject.ConstructNotNull());
        }

        @Override
        protected Object constructObject(Node node) {
            if (recursing) {
                return super.constructObject(node);
            } else {
                recursing = true;
                Object o = super.constructObject(node);
                recursing = false;
                return new LinedObject(o, node.getStartMark());
            }
        }

        private static final class LinedObject {
            private final Object object;

            private final Mark startMark;

            private LinedObject(final Object object, final Mark startMark) {
                this.object = object;
                this.startMark = startMark;
            }

            public Object getObject() {
                return object;
            }

            public Mark getStartMark() {
                return startMark;
            }
        }

        private static class ConstructIgnore extends AbstractConstruct {
            @Override
            public Object construct(Node node) {
                return YamlRunner.Ignore.INSTANCE;
            }
        }

        private static class ConstructLong extends AbstractConstruct {
            @Override
            public Object construct(Node node) {
                if (!(node instanceof ScalarNode)) {
                    Assert.failUnchecked(String.format("The value of the long (!l) tag must be a scalar, however '%s' is found!", node));
                }
                return Long.valueOf(((ScalarNode) node).getValue());
            }
        }

        private static class ConstructStringContains extends AbstractConstruct {
            @Override
            public Object construct(Node node) {
                if (!(node instanceof ScalarNode)) {
                    Assert.failUnchecked(String.format("The value of the string-contains (!sc) tag must be a scalar, however '%s' is found!", node));
                }
                return new StringContains(((ScalarNode) node).getValue());
            }
        }

        private static class ConstructNullPlaceholder extends AbstractConstruct {
            @Override
            public Object construct(Node node) {
                return NullPlaceholder.INSTANCE;
            }
        }

        private static class ConstructNotNull extends AbstractConstruct {
            @Override
            public Object construct(Node node) {
                return NotNull.INSTANCE;
            }
        }
    }

    static final class Ignore {
        static final Ignore INSTANCE = new Ignore();

        private Ignore() {
        }

        @Override
        public String toString() {
            return "!ignore";
        }
    }

    static final class StringContains {
        @Nonnull
        private final String value;

        StringContains(@Nonnull final String value) {
            this.value = value;
        }

        @Nonnull
        public String getValue() {
            return value;
        }

        @Nonnull
        public Matchers.ResultSetMatchResult matchWith(@Nonnull Object other, @Nonnull Matchers.ResultSetPrettyPrinter printer) {
            if (other instanceof String) {
                final var otherStr = (String) other;
                if (otherStr.contains(value)) {
                    return Matchers.ResultSetMatchResult.success();
                } else {
                    return Matchers.ResultSetMatchResult.fail(String.format("The string '%s' does not contain '%s'", otherStr, value), printer);
                }
            } else {
                return Matchers.ResultSetMatchResult.fail(String.format("expected to match against a %s value, however we got %s which is %s", String.class.getSimpleName(), other.toString(), other.getClass().getSimpleName()), printer);
            }
        }

        @Override
        public String toString() {
            return "!sc " + value;
        }
    }

    static final class NullPlaceholder {
        static final NullPlaceholder INSTANCE = new NullPlaceholder();

        private NullPlaceholder() {
        }

        @Override
        public String toString() {
            return "!null";
        }
    }

    static final class NotNull {
        static final NotNull INSTANCE = new NotNull();

        private NotNull() {
        }

        @Override
        public String toString() {
            return "!not_null";
        }
    }

    @Nonnull
    private Command resolveCommand(@Nonnull final List<?> commandAndConfiguration) {
        final var commandAndArgument = Matchers.firstEntry(Matchers.first(commandAndConfiguration, "command list"), "command list");
        final var commandStr = Matchers.notNull(Matchers.string(Matchers.notNull(Matchers.notNull(commandAndArgument, "command").getKey(), "command"), "command"), "command");
        return Objects.requireNonNull(Command.resolve(commandStr));
    }

    public void run() throws Exception {
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(true);
        final var yaml = new Yaml(new CustomTagsInject(), new Representer(), new DumperOptions(), options, new Resolver());
        int currentLine = 0;
        for (final var region : yaml.loadAll(inputStream)) {
            final var regionWithLines = (CustomTagsInject.LinedObject) region;
            currentLine = regionWithLines.getStartMark().getLine() + 1;
            LOG.debug("📍 executing at line {} of '{}'", currentLine, resourcePath);
            final var commandObject = regionWithLines.getObject();
            final var commandAndConfiguration = Matchers.arrayList(commandObject, "test commands");
            final var command = resolveCommand(commandAndConfiguration);
            try {
                command.invoke(commandAndConfiguration, this.cliCommandFactory);
            } catch (Exception | Error e) {
                addYamlFileStackFrameToException(e, resourcePath, currentLine);
                throw e;
            }
        }
        LOG.debug("🏁 executed all tests in '{}' successfully!", resourcePath);
    }

    private static void addYamlFileStackFrameToException(@Nonnull final Throwable exception, @Nonnull final String path,
                                                         int line) {
        final StackTraceElement[] stackTrace = exception.getStackTrace();
        StackTraceElement[] newStackTrace = new StackTraceElement[stackTrace.length + 1];
        newStackTrace[0] = new StackTraceElement("<YAML FILE>", "", path, line);
        System.arraycopy(stackTrace, 0, newStackTrace, 1, stackTrace.length);
        exception.setStackTrace(newStackTrace);
    }

    @Override
    public void close() throws Exception {
        inputStream.close();
        if (this.cliCommandFactory != null) {
            this.cliCommandFactory.close();
        }
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    private static InputStream getInputStream(@Nonnull final String resourcePath) throws RelationalException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(resourcePath);
        Assert.notNull(inputStream, String.format("could not find '%s' in resources bundle", resourcePath));
        return inputStream;
    }
}
