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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.temp.ExpansionVisitor;
import com.apple.foundationdb.record.query.plan.temp.GraphExpansion;
import com.apple.foundationdb.record.query.plan.temp.KeyExpressionVisitor;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
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
 * <p>Similarly, the function can be made to apply in a fan-out fashion, simply by providing a argument an expression
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
    @Nonnull
    protected final String name;
    @Nonnull
    protected final KeyExpression arguments;

    protected FunctionKeyExpression(@Nonnull String name, @Nonnull KeyExpression arguments) {
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
    public static FunctionKeyExpression create(@Nonnull String name, @Nonnull KeyExpression arguments) {
        Optional<Builder> funcBuilder = Registry.instance().getBuilder(name);
        if (!funcBuilder.isPresent()) {
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

    @Nonnull
    public final String getName() {
        return name;
    }

    @Nonnull
    @Override
    public KeyExpression getChild() {
        return getArguments();
    }

    @Nonnull
    public final KeyExpression getArguments() {
        return arguments;
    }

    /**
     * Get the the minimum number of arguments supported by this function.
     * @return the minimum number of arguments supported by this function
     */
    public abstract int getMinArguments();

    /**
     * Get the maximum number of arguments supported by this function.
     * @return the maximum number of arguments supported by this function
     */
    public abstract int getMaxArguments();

    @Nonnull
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
    @Nonnull
    public abstract <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> record,
                                                                             @Nullable Message message,
                                                                             @Nonnull Key.Evaluated arguments);

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
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
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
    @Nonnull
    public static FunctionKeyExpression fromProto(RecordMetaDataProto.Function function) throws DeserializationException {
        try {
            return create(function.getName(), KeyExpression.fromProto(function.getArguments()));
        } catch (RecordCoreException e) {
            throw new DeserializationException(e.getMessage(), e);
        }
    }

    @Nonnull
    @Override
    public final RecordMetaDataProto.Function toProto() throws SerializationException {
        RecordMetaDataProto.Function.Builder builder = RecordMetaDataProto.Function.newBuilder()
                .setName(getName());
        builder.setArguments(getArguments().toKeyExpression());
        return builder.build();
    }

    @Nonnull
    @Override
    public final RecordMetaDataProto.KeyExpression toKeyExpression() {
        return RecordMetaDataProto.KeyExpression.newBuilder().setFunction(toProto()).build();
    }

    @Nonnull
    @Override
    public <S extends KeyExpressionVisitor.State> GraphExpansion expand(@Nonnull final ExpansionVisitor<S> visitor) {
        return visitor.visitExpression(this);
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
     * Base implementation of {@link #planHash}.
     * This implementation makes each concrete subclass implement its own version of {@link #planHash} so that they are
     * guided to add their own class modifier (See {@link com.apple.foundationdb.record.ObjectPlanHash ObjectPlanHash}).
     * This implementation is meant to give subclasses common functionality for their own implementation.
     * @param hashKind the plan hash kind to use
     * @param baseHash the subclass' base hash (concrete identifier)
     * @param hashables the rest of the subclass' hashable parameters (if any)
     * @return the plan hash value calculated
     */
    protected int basePlanHash(@Nonnull final PlanHashKind hashKind, ObjectPlanHash baseHash, Object... hashables) {
        switch (hashKind) {
            case LEGACY:
                return getName().hashCode() + getArguments().planHash(hashKind);
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, baseHash, getName(), getArguments(), hashables);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
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
        @Nonnull
        protected final String functionName;

        public Builder(@Nonnull String functionName) {
            this.functionName = functionName;
        }

        @Nonnull
        public String getName() {
            return functionName;
        }

        @Nonnull
        public abstract FunctionKeyExpression build(@Nonnull KeyExpression arguments);
    }

    /**
     * An implementation of a <code>Builder</code> that can construct a <code>KeyExpressionFunction</code>
     * via a provided generator.
     */
    public static class BiFunctionBuilder extends Builder {
        private final BiFunction<String, KeyExpression, FunctionKeyExpression> generator;

        public BiFunctionBuilder(@Nonnull String functionName,
                               @Nonnull BiFunction<String, KeyExpression, FunctionKeyExpression> generator) {
            super(functionName);
            this.generator = generator;
        }

        @Nonnull
        @Override
        public FunctionKeyExpression build(@Nonnull KeyExpression expression) {
            return generator.apply(super.getName(), expression);
        }
    }

    /**
     * Implementations of <code>FunctionKeyExpression.Factory</code> are dynamically located by the {@link Registry}
     * and are polled once to request a list of builders for functions that the factory is capable of producing.
     */
    public interface Factory {
        @Nonnull
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

        @Nonnull
        public static Registry instance() {
            return INSTANCE;
        }

        public Optional<Builder> getBuilder(String name) {
            Map<String, Builder> registry = initOrGetRegistry();
            return Optional.ofNullable(registry.get(name));
        }

        @Nonnull
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

        @Nonnull
        private static Map<String, Builder> initRegistry() {
            try {
                Map<String, Builder> functions = new HashMap<>();
                for (Factory factory : ServiceLoader.load(Factory.class)) {
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
