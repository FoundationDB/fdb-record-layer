/*
 * FunctionKeyExpression.java
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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.expressions.RecordKeyExpressionProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.values.BuiltInFunctionCatalog;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.ServiceLoaderProvider;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.function.BiFunction;

/**
 * A <code>FunctionKeyExpression</code> is a {@link KeyExpression} that is dynamically loaded and defined by a
 * <code>String</code> name and a <code>Key.Expression</code> that produces sets of arguments to which the function
 * is to be evaluated. <code>FunctionKeyExpressions</code> provide a mechanism by which indexes can be defined on
 * arbitrarily complex logic applied to a record being inserted. For example, assuming a function called
 * "<code>substr</code>" that functions much like the java <code>String.substring()</code> method, you could
 * create, say, a unique index with key of
 *
 * <pre>
 *     function("subsstr", concat(field("firstname"), value(0), value(2)))
 * </pre>
 *
 * Which would prevent duplicate records in which the first two characters of the <code>firstname</code> are identical.
 *
 * <p>Similarly, the function can be made to apply in a fan-out fashion, simply by providing an argument an expression
 * that itself fans out into a set of arguments to <code>substr</code>.  For example, given message definitions
 * such as:
 *
 * <pre>
 * message SubString {
 *   required string content = 1;
 *   required int32 start = 2;
 *   required int32 end = 3;
 * }
 *
 * message SubStrings {
 *   repeated SubString substrings = 1;
 * }
 * </pre>
 *
 * In which we want to have the arguments to <code>substr</code> driven by data stored in the records themselves,
 * you could define the index key expression as:
 *
 * <pre>
 *     function("substr", field("substrings", FanType.FanOut).nest(concatenateFields("content", "start", "end")))
 * </pre>
 *
 * This would produce the result of performing <code>substr(content, start, end)</code> for each <code>SubString</code>value
 * in substrings.
 *
 * <p>Actual implementations of <code>FunctionKeyExpressions</code> are discovered by polling all available
 * {@link Registry} implementations. A <code>Registry</code> returns a list of {@link Builder}s which, given a set of
 * arguments, are capable of creating an implementation of a function.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class FunctionKeyExpression extends BaseKeyExpression implements AtomKeyExpression, KeyExpressionWithChild {
    protected final String name;
    protected final KeyExpression arguments;

    protected FunctionKeyExpression(String name, KeyExpression arguments) {
        this.name = name;
        this.arguments = arguments;
    }

    /**
     * Create a function.
     *
     * @param name the name of the function
     * @param arguments an expression that produces the arguments to the function
     * @return the key expression that implements the function
     * @throws InvalidExpressionException if the function name provided does not have an available
     *   implementation, or the arguments provided are not suitable for the function
     */
    public static FunctionKeyExpression create(String name, KeyExpression arguments) {
        Optional<Builder> funcBuilder = Registry.instance().getBuilder(name);
        if (funcBuilder.isEmpty()) {
            throw new InvalidExpressionException("Function not defined")
                    .addLogInfo(LogMessageKeys.FUNCTION, name);
        }
        final FunctionKeyExpression function = funcBuilder.get().build(arguments);
        final int argumentCount = arguments.getColumnSize();
        if (argumentCount < function.getMinArguments() || argumentCount > function.getMaxArguments()) {
            throw new InvalidExpressionException("Invalid number of arguments provided to function",
                    LogMessageKeys.FUNCTION, name,
                    "args_provided", argumentCount,
                    "min_args_expected", function.getMinArguments(),
                    "max_args_expected", function.getMaxArguments());
        }
        return function;
    }

    public final String getName() {
        return name;
    }

    @Override
    public KeyExpression getChild() {
        return getArguments();
    }

    public final KeyExpression getArguments() {
        return arguments;
    }

    /**
     * Get the minimum number of arguments supported by this function.
     * @return the minimum number of arguments supported by this function
     */
    public abstract int getMinArguments();

    /**
     * Get the maximum number of arguments supported by this function.
     * @return the maximum number of arguments supported by this function
     */
    public abstract int getMaxArguments();

    public GroupingKeyExpression groupBy(KeyExpression groupByFirst, KeyExpression... groupByRest) {
        return GroupingKeyExpression.of(this, groupByFirst, groupByRest);
    }

    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        final List<Key.Evaluated> evaluatedArguments = getArguments().evaluateMessage(record, message);
        final List<Key.Evaluated> results = new ArrayList<>(evaluatedArguments.size());
        for (Key.Evaluated evaluatedArgument : evaluatedArguments) {
            validateArgumentCount(evaluatedArgument);
            results.addAll(evaluateFunction(record, message, evaluatedArgument));
        }
        validateColumnCounts(results);
        return results;
    }

    /**
     * The <code>evaluateFunction</code> method implements the function execution. This method is invoked once per
     * <code>Key.Evaluated</code> that was produced by the evaluation of the function's argument.  Put another way,
     * the function's argument expression is evaluated and is expected to produce a set of arguments. This method
     * is invoked once for each of these and, itself, may produce a set of <code>Key.Evaluated</code> values that
     * produce the final set of keys.
     * <p>
     * Note that the <code>record</code> parameter might be <code>null</code>. Function implementors should treat
     * this case the same way that they would treat a non-<code>null</code> record that has all of its non-repeated
     * fields unset and all of its repeated fields empty. If the function result depends only on the
     * value of <code>arguments</code> and not <code>record</code> directly, then the implementor can ignore
     * the nullity of <code>record</code>.
     * </p>
     *
     * @param <M> the type of the records
     * @param record the record against which this function will produce a key
     * @param message the Protobuf message against which this function will produce a key
     * @param arguments the set of arguments to be applied by the function against the <code>record</code>
     * @return the list of keys for the given record
     */
    public abstract <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> record,
                                                                             @Nullable Message message,
                                                                             Key.Evaluated arguments);

    private void validateArgumentCount(Key.Evaluated arguments) {
        final int argumentCount = arguments.size();
        if (argumentCount < getMinArguments() || argumentCount > getMaxArguments()) {
            throw new InvalidResultException("Invalid number of arguments provided to function").addLogInfo(
                    LogMessageKeys.FUNCTION, getName(),
                    "args_provided", argumentCount,
                    "min_args_expected", getMinArguments(),
                    "max_args_expected", getMaxArguments());
        }
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(Descriptors.Descriptor descriptor) {
        return getArguments().validate(descriptor);
    }

    @Override
    public boolean equalsAtomic(AtomKeyExpression other) {
        return equals(other);
    }

    /**
     * Create a function from its protobuf serialized form.
     * @param function the protobuf definition of the function
     * @return the key expression that implements the function
     * @throws InvalidExpressionException If the function name provided does not have an available
     *   implementation, or the arguments provided are not suitable for the function.
     */
    public static FunctionKeyExpression fromProto(RecordKeyExpressionProto.Function function) throws DeserializationException {
        try {
            return create(function.getName(), KeyExpression.fromProto(function.getArguments()));
        } catch (RecordCoreException e) {
            throw new DeserializationException(Objects.requireNonNullElse(e.getMessage(), "Error deserializing function"), e);
        }
    }

    @Override
    public final RecordKeyExpressionProto.Function toProto() throws SerializationException {
        RecordKeyExpressionProto.Function.Builder builder = RecordKeyExpressionProto.Function.newBuilder()
                .setName(getName());
        builder.setArguments(getArguments().toKeyExpression());
        return builder.build();
    }

    @Override
    public final RecordKeyExpressionProto.KeyExpression toKeyExpression() {
        return RecordKeyExpressionProto.KeyExpression.newBuilder().setFunction(toProto()).build();
    }

    @Override
    public <S extends KeyExpressionVisitor.State, R> R expand(final KeyExpressionVisitor<S, R> visitor) {
        return visitor.visitExpression(this);
    }

    /**
     * This method creates a {@link Value} based on this key expression. The caller provides the {@link Value}s
     * that serve as the arguments to the function. In reality this method is exclusively called from the
     * expansion visitors, e.g. {@link com.apple.foundationdb.record.query.plan.cascades.KeyExpressionExpansionVisitor},
     * thus forming the bridge between key expressions and Cascades values.
     * @param argumentValues the argument values
     * @return a new {@link Value}
     */
    public abstract Value toValue(List<? extends Value> argumentValues);

    protected Value resolveAndEncapsulateFunction(final String functionName,
                                                  final List<? extends Value> argumentValues) {
        final BuiltInFunction<?> builtInFunction =
                BuiltInFunctionCatalog.resolve(functionName, argumentValues.size())
                        .orElseThrow(() -> new RecordCoreArgumentException("unknown function",
                                LogMessageKeys.FUNCTION, getName()));
        return (Value)builtInFunction.encapsulate(CallSiteArguments.ofPositional(argumentValues));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FunctionKeyExpression)) {
            return false;
        }

        FunctionKeyExpression that = (FunctionKeyExpression) o;
        if (!getName().equals(that.getName())) {
            return false;
        }

        return this.getArguments().equals(that.getArguments());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getArguments());
    }

    /**
     * Base implementation of {@link #planHash(PlanHashMode)}.
     * This implementation makes each concrete subclass implement its own version of {@link #planHash(PlanHashMode)} so
     * that they are guided to add their own class modifier (See {@link ObjectPlanHash ObjectPlanHash}).
     * This implementation is meant to give subclasses common functionality for their own implementation.
     * @param mode the plan hash kind to use
     * @param baseHash the subclass' base hash (concrete identifier)
     * @param hashables the rest of the subclass' hashable parameters (if any)
     * @return the plan hash value calculated
     */
    protected int basePlanHash(final PlanHashMode mode, ObjectPlanHash baseHash, Object... hashables) {
        switch (mode.getKind()) {
            case LEGACY:
                return getName().hashCode() + getArguments().planHash(mode);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, baseHash, getName(), getArguments(), hashables);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public String toString() {
        return getName() + "(" + getArguments() + ")";
    }

    /**
     * A builder is capable of producing an instance of a <code>FunctionKeyExpression</code> given the arguments
     * to the function.
     */
    public abstract static class Builder {
        protected final String functionName;

        public Builder(String functionName) {
            this.functionName = functionName;
        }

        public String getName() {
            return functionName;
        }

        public abstract FunctionKeyExpression build(KeyExpression arguments);
    }

    /**
     * An implementation of a <code>Builder</code> that can construct a <code>KeyExpressionFunction</code>
     * via a provided generator.
     */
    public static class BiFunctionBuilder extends Builder {
        private final BiFunction<String, KeyExpression, FunctionKeyExpression> generator;

        public BiFunctionBuilder(String functionName,
                               BiFunction<String, KeyExpression, FunctionKeyExpression> generator) {
            super(functionName);
            this.generator = generator;
        }

        @Override
        public FunctionKeyExpression build(KeyExpression expression) {
            return generator.apply(super.getName(), expression);
        }
    }

    /**
     * Implementations of <code>FunctionKeyExpression.Factory</code> are dynamically located by the {@link Registry}
     * and are polled once to request a list of builders for functions that the factory is capable of producing.
     */
    public interface Factory {
        List<FunctionKeyExpression.Builder> getBuilders();
    }

    /**
     * The <code>Registry</code> maintains a mapping from a function name to a <code>Builder</code> capable of
     * producing an instance of the function.
     */
    public static class Registry {
        private static final Registry INSTANCE = new Registry();

        @Nullable
        private volatile Map<String, Builder> functions;

        private Registry() {
            // Will be initialized the first time a builder is requested
            functions = null;
        }

        public static Registry instance() {
            return INSTANCE;
        }

        public Optional<Builder> getBuilder(String name) {
            Map<String, Builder> registry = initOrGetRegistry();
            return Optional.ofNullable(registry.get(name));
        }

        private Map<String, Builder> initOrGetRegistry() {
            // The reference to the registry is copied into a local variable to avoid referencing the
            // volatile multiple times
            Map<String, Builder> currRegistry = functions;
            if (currRegistry != null) {
                return currRegistry;
            }
            synchronized (this) {
                currRegistry = functions;
                if (currRegistry == null) {
                    // Create the registry
                    Map<String, Builder> newRegistry = initRegistry();
                    functions = newRegistry;
                    return newRegistry;
                } else {
                    // Another thread created the registry for us
                    return currRegistry;
                }
            }
        }

        private static Map<String, Builder> initRegistry() {
            try {
                Map<String, Builder> functions = new HashMap<>();
                for (Factory factory : ServiceLoaderProvider.load(Factory.class)) {
                    for (Builder function : factory.getBuilders()) {
                        if (functions.containsKey(function.getName())) {
                            throw new RecordCoreException("Function already defined").addLogInfo(
                                    LogMessageKeys.FUNCTION, function.getName());
                        }
                        functions.put(function.getName(), function);
                    }
                }
                return functions;
            } catch (ServiceConfigurationError err) {
                throw new RecordCoreException("Unable to load all defined FunctionKeyExpressions", err);
            }
        }
    }
}
