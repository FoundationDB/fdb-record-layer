/*
 * LikeOperatorValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PLikeOperatorValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence.Precedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A {@link Value} that applies a like operator on its child expressions.
 */
@API(API.Status.EXPERIMENTAL)
public class LikeOperatorValue extends AbstractValue implements BooleanValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Like-Operator-Value");

    @Nonnull
    private final Value srcChild;
    @Nonnull
    private final Value patternChild;

    /**
     * Constructs a new instance of {@link LikeOperatorValue}.
     * @param srcChild the string
     * @param patternChild the pattern
     */
    public LikeOperatorValue(@Nonnull final Value srcChild, @Nonnull final Value patternChild) {
        this.srcChild = srcChild;
        this.patternChild = patternChild;
    }

    @Nullable
    @Override
    @SuppressWarnings("java:S6213")
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        String lhs = (String)srcChild.eval(store, context);
        Message rhs = (Message)patternChild.eval(store, context);
        return likeOperation(lhs, rhs);
    }

    @Nullable
    public static Boolean likeOperation(@Nullable final String lhs, @Nullable final Message rhs) {
        if (lhs == null || rhs == null) {
            return null;
        }
        Descriptors.Descriptor rhsDescriptor = rhs.getDescriptorForType();
        final Descriptors.FieldDescriptor patternField = rhsDescriptor.findFieldByNumber(1);
        if (!rhs.hasField(patternField)) {
            return null;
        }
        final String pattern = (String) rhs.getField(patternField);
        final Descriptors.FieldDescriptor escapeField = rhsDescriptor.findFieldByNumber(2);
        final String escape = rhs.hasField(escapeField) ? (String) rhs.getField(escapeField) : null;
        return matchLike(lhs, pattern, escape);
    }

    /**
     * Matcher for the SQL {@code LIKE} operator. It returns whether the given {@code text} matches the given
     * {@code pattern}.
     *
     * <p>
     * The {@code pattern} is a {@code LIKE} pattern as specified by the SQL specification: {@code '%'} matches any
     * sequence of characters, {@code '_'} matches exactly one character, and {@code escape} escapes the next character
     * as a literal. All other characters match themselves. If {@code escape} is null, then no characters are escaped
     * (which means that there is no way to match the literal {@code '%'} or {@code '_'} characters).
     * </p>
     *
     * <p>
     * Note that the escape character represents "match the next character as a literal" regardless of whether that
     * character is actually special. So {@code escape + '_'} matches the literal {@code '_'} and {@code escape + '%'}
     * matches the literal {@code '%'}, but also {@code escape + escape} matches the escape character {@code escape} and
     * {@code escale + 'b'} matches the literal character {@code 'b'}. This means that if the character following
     * {@code escape} is not a special character, then the escape character is effectively dropped from the pattern.
     * Finally, note that if the escape character is the final character in the pattern, then that is a malformed pattern,
     * which will thus always return {@code false}.
     * </p>
     *
     * <p>
     * One note on the implementation. Some care has been taken here to avoid exponential backtracking, which naïve
     * regex compilation could result in for certain patterns. Instead, this is designed to operate in polynomial time,
     * though given an adversarial pattern, it can still devolve to <i>O</i>(<i>n</i> &sdot; <i>p</i>) where
     * <i>n</i> is the length of the {@code text} and <i>p</i> is the length of the {@code pattern}.
     * </p>
     *
     * @param text the text to attempt to match
     * @param pattern the pattern to match the text against
     * @param escape an optional escape character
     */
    private static boolean matchLike(@Nonnull final String text, @Nonnull final String pattern, @Nullable final String escape) {
        int t = 0;
        int p = 0;
        int starP = -1;
        int starT = -1;
        final int tLen = text.length();
        final int pLen = pattern.length();
        final char escapeChar = escape == null ? '\0' : escape.charAt(0);
        if (escape != null && escape.length() > 1) {
            SemanticException.fail(SemanticException.ErrorCode.ESCAPE_CHAR_OF_LIKE_OPERATOR_IS_NOT_SINGLE_CHAR, "");
        }

        // Conceptually, this is similar to breaking the pattern down into chunks, separated by the wildcard
        // character %. For each sequence between %s, we can evaluate if a subsequence from the text
        // matches in linear time. We then do the following:
        //
        //   1. Match a prefix from the text against the prefix from the pattern before the first % (or return false)
        //   2. Match different lengths of substrings from the text (starting with zero) for the %. Find the shortest
        //      that allows the next substring of non-% characters from the pattern to match.
        //   3. If we are able to consume the whole text, then we have a match
        //
        // The reason that we can always pick the shortest string for each % is that we can always defer text into
        // the next % if multiple different choices for each % would match. To see this, consider a pattern
        // p_1 + % + p_2 + % + p_3, where p_1, p_2, and p_3 are sub-patterns that all contain no %s. Suppose that there's
        // a string that matches, so t = t_1 + x_1 + t_2 + x_2 + t_3, where t_1 matches p_1, t_2 matches p_2, and t_3
        // matches p_3 and x_1 and x_2 are "matched" to each %. Assume that the algorithm is wrong, so we are *not*
        // allowed to pick the smallest string such that there's a substring after it in t that matches p_2. So there's
        // a smaller y_1 such that t begins t_1 + y_1 + t_2p where t_2p matches p_2. It's possible that t_2 and t_2p overlap,
        // but it's still possible to construct a y_2 from any non-overlapping suffix of t_2 and x_2 so that
        // t = t_1 + y_1 + t_2p + y_2 + t_3, which will also match. That's a contradiction, so we were in fact allowed to
        // always take the smallest one. (Generalizing this to an arbitrary number of %s is left as an exercise for the reader.)
        //
        // Worst case, this operates in O(tLen * pLen) time. The reason for this is that we need to visit each character in
        // the text. For each such character, we need to perform an O(pLen) operation (namely: check the next subsequence
        // of characters in the text to see if they match the pattern) before we can determine if the character should
        // be identified with the previous wildcard or not. For a string that approximates this worst case, consider
        // a text like 100,000 'a' characters, and then a pattern like '%aaaa'. To validate this match, we need to
        // check 100,000 - 4 ≈ 100,000 different substrings for the prefix, and each one requires reading the next 4
        // characters to evaluate, so that's around 400,000 character comparisons.
        while (t < tLen) {
            boolean matched = false;
            if (p < pLen) {
                final char pc = pattern.charAt(p);
                if (escape != null && pc == escapeChar) {
                    // Escape character. Only allow this to be matched if the next character in the
                    // text exactly matches the next character in the pattern, even if it is a wildcard
                    if (p + 1 >= pLen) {
                        // Pattern terminates in the escape character, which is malformed. Return "no match"
                        return false;
                    }
                    if (pattern.charAt(p + 1) == text.charAt(t)) {
                        t++;
                        p += 2;
                        matched = true;
                    }
                } else if (pc == '%') {
                    if (p + 1 == pLen) {
                        // Pattern ends with a %, and we've been able to match everything up to it, so we are
                        // guaranteed a match. We could remove this early out, but then we'd iterate through
                        // the rest of the text for no reason
                        return true;
                    }
                    // New %. "Lock in" progress so far. Future backtracking will only reset p to this value.
                    // Do not advance t, indicating that we're initially mapping the empty string to the wildcard.
                    starP = p++;
                    starT = t;
                    matched = true;
                } else if (pc == '_' || pc == text.charAt(t)) {
                    // Single character in the text matches the character in the pattern
                    t++;
                    p++;
                    matched = true;
                }
            }
            if (!matched) {
                if (starP >= 0) {
                    // We did not find a match. Try again from the last %, but match an additional character
                    // in the text to that last wildcard character.
                    p = starP + 1;
                    t = ++starT;
                } else {
                    // We have not yet hit a % wildcard. Cannot possibly match, so return now.
                    return false;
                }
            }
        }
        if (escapeChar != '%') {
            // Match any trailing wildcards against the empty string.
            // (Note that if % is the escape character, we have to skip it, as a string of trailing
            // %s in the pattern should be matched against a (half-as-long) sequence of trailing %s
            // in the text, which is handled in the loop.)
            while (p < pLen && pattern.charAt(p) == '%') {
                p++;
            }
        }
        return p == pLen;
    }

    @Override
    public Optional<QueryPredicate> toQueryPredicate(@Nullable final TypeRepository typeRepository,
                                                     @Nonnull final Set<CorrelationIdentifier> localAliases) {
        return Optional.of(new ValuePredicate(srcChild, new Comparisons.ValueComparison(Comparisons.Type.LIKE, patternChild)));
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(srcChild, patternChild);
    }

    @Nonnull
    @Override
    public LikeOperatorValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 2);
        return new LikeOperatorValue(
                Iterables.get(newChildren, 0),
                Iterables.get(newChildren, 1));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, srcChild, patternChild);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final var src = Iterables.get(explainSuppliers, 0).get();
        final var pattern = Iterables.get(explainSuppliers, 1).get();
        return ExplainTokensWithPrecedence.of(Precedence.BETWEEN,
                Precedence.BETWEEN.parenthesizeChild(src, true).addWhitespace().addKeyword("LIKE")
                        .addWhitespace().addNested(Precedence.BETWEEN.parenthesizeChild(pattern)));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public PLikeOperatorValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PLikeOperatorValue.newBuilder()
                .setSrcChild(srcChild.toValueProto(serializationContext))
                .setPatternChild(patternChild.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setLikeOperatorValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static LikeOperatorValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PLikeOperatorValue likeOperatorValueProto) {
        return new LikeOperatorValue(Value.fromValueProto(serializationContext, Objects.requireNonNull(likeOperatorValueProto.getSrcChild())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(likeOperatorValueProto.getPatternChild())));
    }

    @Nonnull
    private static Value encapsulate(@Nonnull final List<? extends Typed> arguments) {
        Verify.verify(arguments.size() == 2);
        Type srcType = arguments.get(0).getResultType();
        Type patternType = arguments.get(1).getResultType();
        SemanticException.check(srcType.getTypeCode().equals(TypeCode.STRING), SemanticException.ErrorCode.OPERAND_OF_LIKE_OPERATOR_IS_NOT_STRING);
        SemanticException.check(PatternForLikeValue.TYPE.equals(patternType), SemanticException.ErrorCode.OPERAND_OF_LIKE_OPERATOR_IS_NOT_STRING);

        return new LikeOperatorValue((Value) arguments.get(0), (Value) arguments.get(1));
    }

    /**
     * The {@code like} operator.
     */
    @AutoService(BuiltInFunction.class)
    public static class LikeFn extends BuiltInFunction<Value> {
        public LikeFn() {
            super("like",
                    ImmutableList.of(Type.primitiveType(TypeCode.STRING), PatternForLikeValue.TYPE),
                    (ignored, args) -> LikeOperatorValue.encapsulate(args.getArgumentsList()));
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PLikeOperatorValue, LikeOperatorValue> {
        @Nonnull
        @Override
        public Class<PLikeOperatorValue> getProtoMessageClass() {
            return PLikeOperatorValue.class;
        }

        @Nonnull
        @Override
        public LikeOperatorValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                           @Nonnull final PLikeOperatorValue likeOperatorValueProto) {
            return LikeOperatorValue.fromProto(serializationContext, likeOperatorValueProto);
        }
    }
}
