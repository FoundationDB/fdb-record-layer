/*
 * QueryInterpreter.java
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

package com.apple.foundationdb.relational.yamltests.command;

import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.command.parameterinjection.InListParameter;
import com.apple.foundationdb.relational.yamltests.command.parameterinjection.ListParameter;
import com.apple.foundationdb.relational.yamltests.command.parameterinjection.Parameter;
import com.apple.foundationdb.relational.yamltests.command.parameterinjection.PrimitiveParameter;
import com.apple.foundationdb.relational.yamltests.command.parameterinjection.TupleParameter;
import com.apple.foundationdb.relational.yamltests.command.parameterinjection.UnboundParameter;
import org.apache.commons.lang3.tuple.Pair;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;
import org.yaml.snakeyaml.nodes.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.apple.foundationdb.relational.yamltests.Matchers.constructVectorFromString;

/**
 * {@link QueryInterpreter} interprets the query that is provided in the YAML testing framework using the
 * {@link QueryCommand} command. The query in the framework is expected to be a valid query string based on the
 * Relational parser except that it can have parameter injection snippets.
 * <p>
 * A parameter injection snippet is a statement surrounded by `/{` and `}/` that can substitute anywhere in the query
 * where a prepared statement parameter can be put. For example, lets take a simple query:
 * <pre>{@code
 *      SELECT * FROM foo WHERE foo.a > 10
 *}</pre>
 * Here, 10 can be substituted for a parameter and hence qualify to be substituted by our parameter injection snippet,
 * as:
 * <pre>{@code
 *      SELECT * FROM foo WHERE foo.a > %% 10 %%
 *}</pre>
 * QueryInterpreter interprets {@code /{10}/} as a singleton {@link PrimitiveParameter}. A mapping of {@link Parameter}s
 * in the given query helps to adapt the query to be executed as a simple statement or as a prepared statement, as required.
 * <p>
 * A {@link Parameter} can itself be a literal like {@code STRING}, {@code INTEGER} or {@code BOOLEAN} value, or it
 * can be an {@link UnboundParameter} that is not literal and needs to be "bound" using a {@link Random} generator to
 * take a literal value. An example of query with {@link UnboundParameter} is:
 * <pre>{@code
 *      SELECT * FROM foo WHERE foo.a > %% !r [2, 9] %%
 *}</pre>
 * Here, the parameter can randomly take different values between 2 and 9 in each binding.
 */
public final class QueryInterpreter {
    @Nonnull
    private final String query;
    private final int lineNumber;
    @Nonnull
    private final YamlExecutionContext executionContext;
    /**
     * {@link List} of the snippet text and interpreted parameter injection. Note that the parameter can be a
     * hierarchy of other parameters and some of which might be {@link UnboundParameter}s. The {@link UnboundParameter}
     * should fixate on a value before they can be injected to the query string to form an executable query.
     */
    private final List<Pair<String, Parameter>> injections;

    private static final Yaml INTERPRETER = new Yaml(new QueryParameterYamlConstructor(new LoaderOptions()));

    private static final class QueryParameterYamlConstructor extends SafeConstructor {
        private static final Tag RANDOM_TAG = new Tag("!r");
        private static final Tag ARRAY_GENERATOR_TAG = new Tag("!a");
        private static final Tag IN_LIST_TAG = new Tag("!in");
        private static final Tag NULL_TAG = new Tag("!n");
        private static final Tag UUID_TAG = new Tag("!uuid");
        private static final Tag VECTOR16_TAG = new Tag("!v16");

        private QueryParameterYamlConstructor(LoaderOptions loaderOptions) {
            super(loaderOptions);

            this.yamlConstructors.put(RANDOM_TAG, new ConstructRandom());
            this.yamlConstructors.put(ARRAY_GENERATOR_TAG, new ConstructArrayGenerator());
            this.yamlConstructors.put(IN_LIST_TAG, new ConstructInList());
            this.yamlConstructors.put(NULL_TAG, new ConstructNull());
            this.yamlConstructors.put(UUID_TAG, new ConstructUuid());
            this.yamlConstructors.put(VECTOR16_TAG, new ConstructVector16());

            this.yamlConstructors.put(new Tag("!l"), new CustomYamlConstructor.ConstructLong());
        }

        @Override
        public Parameter constructObject(Node node) {
            if (node.getTag().equals(RANDOM_TAG) || node.getTag().equals(ARRAY_GENERATOR_TAG) || node.getTag().equals(IN_LIST_TAG)
                    || node.getTag().equals(NULL_TAG) || node.getTag().equals(UUID_TAG) || node.getTag().equals(VECTOR16_TAG)) {
                return (Parameter) super.constructObject(node);
            } else if (node instanceof SequenceNode) {
                // simple list
                return new ListParameter(((SequenceNode) node).getValue().stream().map(this::constructObject).collect(Collectors.toList()));
            } else if (node instanceof MappingNode) {
                // simple tuple
                return new TupleParameter(((MappingNode) node).getValue().stream().map(t -> constructObject(t.getKeyNode())).collect(Collectors.toList()));
            } else {
                // literal of one of primitive type
                return new PrimitiveParameter(super.constructObject(node));
            }
        }

        /**
         * Constructor for Random unbound parameters.
         */
        private class ConstructRandom extends AbstractConstruct {

            @Override
            public Object construct(Node node) {
                if (node instanceof SequenceNode) {
                    var values = ((SequenceNode) node).getValue();
                    if (values.size() == 1) {
                        return new UnboundParameter.RandomRangeParameter(constructObject(values.get(0)));
                    } else if (values.size() == 2) {
                        return new UnboundParameter.RandomRangeParameter(constructObject(values.get(0)), constructObject(values.get(1)));
                    }
                    Assert.failUnchecked("!r expects a list of 1 or 2 elements.");
                } else if (node instanceof MappingNode) {
                    var values = ((MappingNode) node).getValue();
                    return new UnboundParameter.RandomSetParameter(values.stream().map(v -> constructObject(v.getKeyNode())).collect(Collectors.toList()));
                }
                return null;
            }
        }

        /**
         * Constructor for Array Generator unbound parameters.
         */
        private class ConstructArrayGenerator extends AbstractConstruct {

            @Override
            public Object construct(Node node) {
                if (node instanceof SequenceNode) {
                    var values = ((SequenceNode) node).getValue();
                    if (values.size() == 1) {
                        return new UnboundParameter.ListRangeParameter(constructObject(values.get(0)));
                    } else if (values.size() == 2) {
                        return new UnboundParameter.ListRangeParameter(constructObject(values.get(0)), constructObject(values.get(1)));
                    }
                    Assert.failUnchecked("!a expects a list of 1 or 2 elements.");
                } else if (node instanceof MappingNode) {
                    var values = ((MappingNode) node).getValue();
                    Assert.thatUnchecked(values.size() == 2, "!a expects a set to have 2 elements.");
                    return new UnboundParameter.ElementMultiplicityListParameter(constructObject(values.get(0).getKeyNode()), constructObject(values.get(1).getKeyNode()));
                }
                return null;
            }
        }

        /**
         * Constructor for {@link InListParameter}.
         */
        private class ConstructInList extends AbstractConstruct {

            @Override
            public Object construct(Node node) {
                Assert.thatUnchecked(node instanceof MappingNode, "!in expects a set.");
                Assert.thatUnchecked(((MappingNode) node).getValue().size() == 1, "!in expects a set of 1 element.");
                return new InListParameter(constructObject(((MappingNode) node).getValue().get(0).getKeyNode()));
            }
        }

        /**
         * Constructor for NULL literal value.
         */
        private static class ConstructNull extends AbstractConstruct {

            @Override
            public Object construct(Node node) {
                return new PrimitiveParameter(null);
            }
        }

        /**
         * Constructor for UUID literal value.
         */
        private static class ConstructUuid extends AbstractConstruct {

            @Override
            public Object construct(Node node) {
                Assert.thatUnchecked(node instanceof ScalarNode, "!uuid expects a string value.");
                return new PrimitiveParameter(UUID.fromString(((ScalarNode) node).getValue()));
            }
        }

        /**
         * Constructor for Vector16 (HalfVector) literal value.
         */
        private static class ConstructVector16 extends AbstractConstruct {

            @Override
            public Object construct(Node node) {
                return new PrimitiveParameter(constructVectorFromString(16, Assert.castUnchecked(node, SequenceNode.class)));
            }
        }
    }

    private QueryInterpreter(int lineNumber, @Nonnull String query, @Nonnull final YamlExecutionContext executionContext) {
        this.query = query;
        this.lineNumber = lineNumber;
        this.injections = getInjections(query);
        this.executionContext = executionContext;
    }

    /**
     * The original query string with embedded parameter injections.
     */
    @Nonnull
    String getQuery() {
        return query;
    }

    private List<Pair<String, Parameter>> getInjections(@Nonnull String query) {

        final var lst = new ArrayList<Pair<String, Parameter>>();
        int cursor = 0;
        while (true) {
            int start = query.indexOf("!!", cursor);
            if (start == -1) {
                break;
            }
            int end = query.indexOf("!!", start + 2);
            Assert.thatUnchecked(end != -1, "Illegal format: Parameter injection not formed correctly in query " + query);
            cursor = end + 2;
            lst.add(Pair.of(query.substring(start, end + 2), INTERPRETER.load(query.substring(start + 2, end))));
        }
        return lst;
    }

    public static QueryInterpreter withQueryString(int lineNumber, @Nonnull String query, @Nonnull final YamlExecutionContext executionContext) {
        return new QueryInterpreter(lineNumber, query, executionContext);
    }

    /**
     * Binds the parameter injections, injects the bound parameters to the query and returns a
     * {@link QueryExecutor} instance that can execute the adapted query as a simple statement or prepared statement,
     * as indicated in the argument. Note that {@code runAsPreparedStatement} does not guarantee expected execution.
     * If the {@code random} in {@code NULL}, we expect that there are no injections and hence executes as a simple
     * statement.
     *
     * @param random a {@link Random} instance to "bind" the parameter injections.
     * @param runAsPreparedStatement if the executor should execute the query using {@link com.apple.foundationdb.relational.api.RelationalPreparedStatement} API.
     * @return the executor that can execute the query.
     */
    @Nonnull
    public QueryExecutor getExecutor(@Nullable Random random, boolean runAsPreparedStatement) {
        try {
            final boolean forceContinuations = (boolean)executionContext.getOption(YamlExecutionContext.OPTION_FORCE_CONTINUATIONS, false);
            if (random == null) {
                // we do not allow prepared statements if the Random generator is not there
                Assert.thatUnchecked(injections.isEmpty(), "Parameter injection is not allowed in query without a Random(generator)");
                return new QueryExecutor(query, lineNumber, null, forceContinuations);
            } else {
                var boundInjections = injections.stream().map(i -> (Pair.of(i.getLeft(), i.getRight().bind(random)))).collect(Collectors.toList());
                if (runAsPreparedStatement) {
                    return new QueryExecutor(adaptToPreparedStatement(query, boundInjections), lineNumber, boundInjections.stream().map(Pair::getRight).collect(Collectors.toList()), forceContinuations);
                } else {
                    return new QueryExecutor(adaptToSimpleStatement(query, boundInjections), lineNumber, null, forceContinuations);
                }
            }
        } catch (Exception e) {
            throw executionContext.wrapContext(e, () -> String.format(Locale.ROOT, "‼️ Error initializing query executor for query %s at line %d", query, lineNumber), "query", lineNumber);
        }
    }

    @Nonnull
    private static String adaptToPreparedStatement(@Nonnull String query, @Nonnull List<Pair<String, Parameter>> injections) {
        String adaptedString = query;
        for (var injection : injections) {
            adaptedString = adaptedString.replace(injection.getLeft(), "?");
        }
        return adaptedString;
    }

    @Nonnull
    private static String adaptToSimpleStatement(@Nonnull String query, @Nonnull List<Pair<String, Parameter>> injections) {
        String adaptedString = query;
        for (var injection : injections) {
            adaptedString = adaptedString.replace(injection.getLeft(), injection.getRight().getSqlText());
        }
        return adaptedString;
    }
}
