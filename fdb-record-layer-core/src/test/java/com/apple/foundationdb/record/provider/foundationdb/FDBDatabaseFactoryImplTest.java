/*
 * FDBDatabaseFactoryImplTest.java
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

import com.apple.foundationdb.record.RecordCoreArgumentException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of the client knob-related methods on {@link FDBDatabaseFactory}, exercised against a fresh,
 * un-initialized {@link FDBDatabaseFactoryImpl} so that they can run without a running FDB instance.
 */
@Execution(ExecutionMode.CONCURRENT)
class FDBDatabaseFactoryImplTest {
    private FDBDatabaseFactory factory;

    @BeforeEach
    void createFactory() {
        // Deliberately create a new factory rather than using the singleton. That way, setting
        // the knobs in these tests does not actually attempt to set the knobs on any concurrently
        // running FDB client.
        factory = new FDBDatabaseFactoryImpl();
    }

    @Test
    void setKnobByNameRecordsValue() {
        factory.setKnob("some_unknown_knob", "42");
        assertThat(factory.getKnobs()).containsExactly(Map.entry("some_unknown_knob", "42"));
    }

    @Test
    void setKnobByEnumUsesLowerCasedNameAsKnobName() {
        factory.setKnob(FDBClientKnob.TLS_CLIENT_HANDSHAKE_THREADS, "3");
        assertThat(factory.getKnobs()).containsExactly(Map.entry("tls_client_handshake_threads", "3"));
    }

    @Test
    void setKnobOverwritesPreviousValue() {
        factory.setKnob(FDBClientKnob.TLS_CLIENT_HANDSHAKE_THREADS, "3");
        factory.setKnob(FDBClientKnob.TLS_CLIENT_HANDSHAKE_THREADS, "5");
        assertThat(factory.getKnobs()).containsExactly(Map.entry("tls_client_handshake_threads", "5"));
    }

    @Test
    void clearKnobsRemovesAllConfiguredKnobs() {
        factory.setKnob(FDBClientKnob.TLS_CLIENT_HANDSHAKE_THREADS, "3");
        factory.setKnob("some_unknown_knob", "42");
        factory.clearKnobs();
        assertThat(factory.getKnobs()).isEmpty();
    }

    @Test
    void getKnobsReturnsUnmodifiableView() {
        factory.setKnob(FDBClientKnob.TLS_CLIENT_HANDSHAKE_THREADS, "3");
        assertThatThrownBy(() -> factory.getKnobs().put("some_unknown_knob", "1"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void getKnobsReturnsLiveViewOfLaterChanges() {
        // getKnobs() is documented as returning a view of the current knobs, so it should reflect subsequent
        // setKnob()/clearKnobs() calls made through the same map instance rather than a point-in-time snapshot.
        final Map<String, String> knobsView = factory.getKnobs();
        factory.setKnob(FDBClientKnob.TLS_CLIENT_HANDSHAKE_THREADS, "3");
        assertThat(knobsView).containsExactly(Map.entry("tls_client_handshake_threads", "3"));
        factory.clearKnobs();
        assertThat(knobsView).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "bad=name"})
    void setKnobByNameRejectsInvalidNames(String knobName) {
        assertThatThrownBy(() -> factory.setKnob(knobName, "3"))
                .isInstanceOf(RecordCoreArgumentException.class);
        assertThat(factory.getKnobs()).isEmpty();
    }

    @Test
    void setKnobByNameAcceptsValidValueForKnownKnob() {
        assertThatCode(() -> factory.setKnob(FDBClientKnob.TLS_CLIENT_HANDSHAKE_THREADS.getKnobName(), "3"))
                .doesNotThrowAnyException();
        assertThat(factory.getKnobs()).containsExactly(Map.entry("tls_client_handshake_threads", "3"));
    }

    @Test
    void setKnobByNameRejectsInvalidValueForKnownKnob() {
        // "tls_client_handshake_threads" is a known INT knob, so setting it via the raw-string overload should
        // still be validated as if it had been set through the FDBClientKnob-typed overload.
        assertThatThrownBy(() -> factory.setKnob(FDBClientKnob.TLS_CLIENT_HANDSHAKE_THREADS.getKnobName(), "not_an_int"))
                .isInstanceOf(RecordCoreArgumentException.class);
        assertThat(factory.getKnobs()).isEmpty();
    }

    @Test
    void setKnobByNameRejectsInvalidValueForKnownLongKnob() {
        assertThatThrownBy(() -> factory.setKnob(FDBClientKnob.PACKET_LIMIT.getKnobName(), "not_a_long"))
                .isInstanceOf(RecordCoreArgumentException.class);
        assertThat(factory.getKnobs()).isEmpty();
    }

    @Test
    void setKnobByNameRejectsInvalidValueForKnownDoubleKnob() {
        assertThatThrownBy(() -> factory.setKnob(FDBClientKnob.TLS_CLIENT_CONNECTION_THROTTLE_TIMEOUT.getKnobName(), "not_a_double"))
                .isInstanceOf(RecordCoreArgumentException.class);
        assertThat(factory.getKnobs()).isEmpty();
    }

    @Test
    void setKnobByNameRejectsInvalidValueForKnownBooleanKnob() {
        assertThatThrownBy(() -> factory.setKnob(FDBClientKnob.LOG_CONNECTION_ATTEMPTS_ENABLED.getKnobName(), "not_a_bool"))
                .isInstanceOf(RecordCoreArgumentException.class);
        assertThat(factory.getKnobs()).isEmpty();
    }

    @Test
    void setKnobByNameAcceptsAnyValueForKnownStringKnob() {
        assertThatCode(() -> factory.setKnob(FDBClientKnob.CONNECTION_LOG_DIRECTORY.getKnobName(), "anything at all"))
                .doesNotThrowAnyException();
        assertThat(factory.getKnobs()).containsExactly(Map.entry("connection_log_directory", "anything at all"));
    }

    @Test
    void setKnobByNameDoesNotValidateUnknownKnob() {
        assertThatCode(() -> factory.setKnob("some_unknown_knob", "not_a_number_but_who_knows"))
                .doesNotThrowAnyException();
        assertThat(factory.getKnobs()).containsExactly(Map.entry("some_unknown_knob", "not_a_number_but_who_knows"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"3", "-1", "0x10", "2147483647", "-2147483648"})
    void setKnobByEnumAcceptsValidIntValues(String value) {
        assertThatCode(() -> factory.setKnob(FDBClientKnob.TLS_CLIENT_HANDSHAKE_THREADS, value))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"not_an_int", "3.5", "", "#10"})
    void setKnobByEnumRejectsInvalidIntValues(String value) {
        assertThatThrownBy(() -> factory.setKnob(FDBClientKnob.TLS_CLIENT_HANDSHAKE_THREADS, value))
                .isInstanceOf(RecordCoreArgumentException.class);
        assertThat(factory.getKnobs()).isEmpty();
    }

    @Test
    void setKnobByEnumAcceptsOctalIntValueMatchingNativeClientSemantics() {
        // "010" is octal in the native client's base-0 stoi parsing, just as it is for Integer.decode, and both
        // interpret it as the decimal value 8 (not 10).
        assertThatCode(() -> factory.setKnob(FDBClientKnob.TLS_CLIENT_HANDSHAKE_THREADS, "010"))
                .doesNotThrowAnyException();
        assertThat(Integer.decode("010")).isEqualTo(8);
    }

    @ParameterizedTest
    @ValueSource(strings = {"11.0", "0", "-1.5", "NaN", "Infinity", "-Infinity"})
    void setKnobByEnumAcceptsValidDoubleValues(String value) {
        assertThatCode(() -> factory.setKnob(FDBClientKnob.TLS_CLIENT_CONNECTION_THROTTLE_TIMEOUT, value))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"not_a_double", ""})
    void setKnobByEnumRejectsInvalidDoubleValues(String value) {
        assertThatThrownBy(() -> factory.setKnob(FDBClientKnob.TLS_CLIENT_CONNECTION_THROTTLE_TIMEOUT, value))
                .isInstanceOf(RecordCoreArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "TRUE", "false", "FALSE", "1", "0", "-1", "0x10", "010"})
    void setKnobByEnumAcceptsValidBooleanValues(String value) {
        assertThatCode(() -> factory.setKnob(FDBClientKnob.LOG_CONNECTION_ATTEMPTS_ENABLED, value))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"not_a_bool", "yes", ""})
    void setKnobByEnumRejectsInvalidBooleanValues(String value) {
        assertThatThrownBy(() -> factory.setKnob(FDBClientKnob.LOG_CONNECTION_ATTEMPTS_ENABLED, value))
                .isInstanceOf(RecordCoreArgumentException.class);
    }

    @Test
    void setKnobByEnumAcceptsAnyStringValueForStringKnobs() {
        assertThatCode(() -> factory.setKnob(FDBClientKnob.CONNECTION_LOG_DIRECTORY, "/tmp/fdb-connections"))
                .doesNotThrowAnyException();
        assertThat(factory.getKnobs()).containsExactly(Map.entry("connection_log_directory", "/tmp/fdb-connections"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "9223372036854775807", "-9223372036854775808"})
    void setKnobByEnumAcceptsValidLongValues(String value) {
        assertThatCode(() -> factory.setKnob(FDBClientKnob.PACKET_WARNING, value))
                .doesNotThrowAnyException();
    }

    @Test
    void setKnobByEnumRejectsLongValuesThatOverflowInt() {
        // sanity check that the LONG knob type is checked against long parsing, not int parsing
        final String tooBigForInt = Long.toString((long) Integer.MAX_VALUE + 1);
        assertThatCode(() -> factory.setKnob(FDBClientKnob.PACKET_WARNING, tooBigForInt))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> factory.setKnob(FDBClientKnob.TLS_CLIENT_HANDSHAKE_THREADS, tooBigForInt))
                .isInstanceOf(RecordCoreArgumentException.class);
    }
}
