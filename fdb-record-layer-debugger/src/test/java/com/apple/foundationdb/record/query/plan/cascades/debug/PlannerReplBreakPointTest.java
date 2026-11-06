/*
 * PlannerReplBreakPointTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.debug;

import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEvent.Location;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEvent.Shorthand;
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of the break points defined by {@link PlannerRepl}, which do not need a running REPL.
 */
class PlannerReplBreakPointTest {
    @Test
    void eventTypeBreakPointEqualsItself() {
        validateEquals(
                new PlannerRepl.OnEventTypeBreakPoint(Shorthand.TRANSFORM, "ref1", Location.BEGIN),
                new PlannerRepl.OnEventTypeBreakPoint(Shorthand.TRANSFORM, "ref1", Location.BEGIN));
    }

    @Test
    void eventTypeBreakPointReferenceNameIsCaseInsensitive() {
        validateEquals(
                new PlannerRepl.OnEventTypeBreakPoint(Shorthand.INITPHASE, "REF1", Location.END),
                new PlannerRepl.OnEventTypeBreakPoint(Shorthand.INITPHASE, "ref1", Location.END));
    }

    @Test
    void eventTypeBreakPointWithoutReferenceEqualsOneWithNullReference() {
        validateEquals(new PlannerRepl.OnEventTypeBreakPoint(Shorthand.INITPHASE, Location.END),
                new PlannerRepl.OnEventTypeBreakPoint(Shorthand.INITPHASE, null, Location.END));
    }

    @Test
    void eventTypeBreakPointsDifferByShorthand() {
        validateNotEquals(
                new PlannerRepl.OnEventTypeBreakPoint(Shorthand.TRANSFORM, "ref1", Location.BEGIN),
                new PlannerRepl.OnEventTypeBreakPoint(Shorthand.RULECALL, "ref1", Location.BEGIN));
    }

    @Test
    void eventTypeBreakPointsDifferByReferenceName() {
        validateNotEquals(
                new PlannerRepl.OnEventTypeBreakPoint(Shorthand.INSERT_INTO_MEMO, "ref1", Location.BEGIN),
                new PlannerRepl.OnEventTypeBreakPoint(Shorthand.INSERT_INTO_MEMO, "ref2", Location.BEGIN));
    }

    @Test
    void eventTypeBreakPointsDifferByLocation() {
        validateNotEquals(
                new PlannerRepl.OnEventTypeBreakPoint(Shorthand.ADJUSTMATCH, "ref2", Location.BEGIN),
                new PlannerRepl.OnEventTypeBreakPoint(Shorthand.ADJUSTMATCH, "ref2", Location.END));
    }

    @Test
    void eventTypeBreakPointIsNotEqualToOtherKinds() {
        validateNotEquals(
                new PlannerRepl.OnEventTypeBreakPoint(Shorthand.INITPHASE, Location.BEGIN),
                new PlannerRepl.OnPhaseBreakPoint(Location.BEGIN, null));
    }

    @Test
    void phaseBreakPointEqualsItselfAndEqualBreakPoints() {
        validateEquals(
                new PlannerRepl.OnPhaseBreakPoint(Location.BEGIN, PlannerPhase.PLANNING),
                new PlannerRepl.OnPhaseBreakPoint(Location.BEGIN, PlannerPhase.PLANNING));
    }

    @Test
    void phaseBreakPointsForAnyPhaseAreEqual() {
        validateEquals(
                new PlannerRepl.OnPhaseBreakPoint(Location.BEGIN, null),
                new PlannerRepl.OnPhaseBreakPoint(Location.BEGIN, null));
    }

    @Test
    void phaseBreakPointsDifferByPhase() {
        validateNotEquals(
                new PlannerRepl.OnPhaseBreakPoint(Location.BEGIN, null),
                new PlannerRepl.OnPhaseBreakPoint(Location.BEGIN, PlannerPhase.PLANNING));
        validateNotEquals(
                new PlannerRepl.OnPhaseBreakPoint(Location.BEGIN, PlannerPhase.PLANNING),
                new PlannerRepl.OnPhaseBreakPoint(Location.BEGIN, PlannerPhase.REWRITING));
    }

    @Test
    void phaseBreakPointsDifferByLocation() {
        validateNotEquals(
                new PlannerRepl.OnPhaseBreakPoint(Location.BEGIN, PlannerPhase.PLANNING),
                new PlannerRepl.OnPhaseBreakPoint(Location.END, PlannerPhase.PLANNING));
    }

    @Test
    void phaseBreakPointIsNotEqualToOtherKinds() {
        validateNotEquals(
                new PlannerRepl.OnPhaseBreakPoint(Location.BEGIN, null),
                new PlannerRepl.OnEventTypeBreakPoint(Shorthand.INITPHASE, Location.BEGIN));
    }

    @Test
    void phaseBreakPointListsItsPlannerPhase() throws IOException {
        assertThat(listed(new PlannerRepl.OnPhaseBreakPoint(Location.BEGIN, PlannerPhase.PLANNING)))
                .contains(ReplTestUtil.coloredKeyValue("kind", "OnPhaseBreakPoint"),
                        ReplTestUtil.coloredKeyValue("shorthand", "initphase"),
                        ReplTestUtil.coloredKeyValue("location", "begin"),
                        ReplTestUtil.coloredKeyValue("plannerPhase", "planning"));
    }

    @Test
    void phaseBreakPointForAnyPhaseListsAny() throws IOException {
        assertThat(listed(new PlannerRepl.OnPhaseBreakPoint(Location.BEGIN, null)))
                .contains(ReplTestUtil.coloredKeyValue("plannerPhase", "any"));
    }

    private <T> void validateEquals(@Nonnull T original, @Nonnull T copy) {
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(copy)
                    .isEqualTo(original)
                    .hasSameHashCodeAs(original);
        }
    }

    private <T> void validateNotEquals(@Nonnull T original, @Nonnull T copy) {
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(copy)
                    .isNotEqualTo(original)
                    .doesNotHaveSameHashCodeAs(original);
        }
    }

    private static String listed(final PlannerRepl.BreakPoint breakPoint) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (Terminal terminal = new DumbTerminal(new ByteArrayInputStream(new byte[0]), outputStream)) {
            breakPoint.onList(new PlannerRepl(terminal, false));
            terminal.writer().flush();
        }
        return outputStream.toString(StandardCharsets.UTF_8);
    }
}
