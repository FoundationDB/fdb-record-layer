/*
 * CliJsonParserTest.java
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

package com.apple.foundationdb.relational.cli;


import com.apple.foundationdb.relational.generated.CliLexer;
import com.apple.foundationdb.relational.generated.CliParser;

import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CliJsonParserTest {
    @Test
    void parsesJsonObjectWithTwoFields() throws Exception {
        CliJsonVisitor visitor = new CliJsonVisitor();
        String json = "{\"field1\":true,\"field2\":\"str\",\"field3\":{\"nested\":123},\"field4\":[1,2,3.3],\"field5\":23.2,\"field6\":null}";

        JsonObject parsedJson = visitor.visitJsonObj(parse(json).jsonObj());
        Assertions.assertEquals(Set.of("field1", "field2", "field3", "field4", "field5", "field6"), parsedJson.keySet(), "Missing fields");
        Assertions.assertTrue(parsedJson.getBoolean("field1"), "Incorrect field1 value");

        Assertions.assertEquals("str", parsedJson.getString("field2"), "Incorrect field2 value");

        Assertions.assertEquals(123, parsedJson.getJsonObject("field3").getJsonNumber("nested").intValue(), "Incorrect nested structure for field3");

        List<BigDecimal> expectedF4Values = List.of(new BigDecimal("1"), new BigDecimal("2"), new BigDecimal("3.3"));
        List<BigDecimal> parsedF4Values = parsedJson.getJsonArray("field4").stream().map(jv -> ((JsonNumber) jv).bigDecimalValue()).collect(Collectors.toList());
        Assertions.assertIterableEquals(expectedF4Values, parsedF4Values, "Incorrect field 4 values");

        assertNumbersMatch(new BigDecimal("23.2"), parsedJson.getJsonNumber("field5"));

        Assertions.assertEquals(JsonValue.NULL, parsedJson.get("field6"), "Incorrect field6 value");
    }

    @Test
    void parsesJsonObjectWithBoolean() throws Exception {
        CliJsonVisitor parser = new CliJsonVisitor();
        String json = "{\"test_string\":true}";

        JsonObject parsedJson = parser.visitJsonObj(parse(json).jsonObj());

        Assertions.assertTrue(parsedJson.containsKey("test_string"), "Missing field key!");
        Assertions.assertTrue(parsedJson.getBoolean("test_string"), "Incorrect parse of true");

        json = "{\"test_string\":false}";

        parsedJson = parser.visitJsonObj(parse(json).jsonObj());

        Assertions.assertTrue(parsedJson.containsKey("test_string"), "Missing field key!");
        Assertions.assertFalse(parsedJson.getBoolean("test_string"), "Incorrect parse of false");
    }

    @Test
    void parsesJsonObjectWithStringField() throws Exception {
        CliJsonVisitor parser = new CliJsonVisitor();
        String json = "{\"test_string\":\"test_value\"}";

        JsonObject parsedJson = parser.visitJsonObj(parse(json).jsonObj());

        Assertions.assertTrue(parsedJson.containsKey("test_string"), "Missing field key!");
        Assertions.assertEquals("test_value", parsedJson.getString("test_string"), "Missing field key!");
        JsonValue jv = parsedJson.get("test_string");
        Assertions.assertTrue(jv instanceof JsonString, "Not a String type!");
        JsonString parsedString = (JsonString) jv;
        Assertions.assertEquals("test_value", parsedString.getString());
        Assertions.assertEquals("test_value", parsedString.getChars());
        Assertions.assertEquals(JsonValue.ValueType.STRING, parsedString.getValueType());

        //this is kinda pointless, but it helps with test coverage
        JsonString expectedString = new TestString("test_value");
        Assertions.assertEquals(parsedString, expectedString, "equals method fails");
        Assertions.assertEquals(expectedString.hashCode(), parsedString.hashCode());
    }

    @Test
    void parsesJsonObjectWithInvalidStringField() {
        String json = "{\"test_string\":missing_a_quote}";

        ParseCancellationException pce = Assertions.assertThrows(ParseCancellationException.class, () -> parse(json).jsonObj());
        Throwable t = pce.getCause();
        Assertions.assertTrue(t instanceof NoViableAltException, "Throwing the wrong kind of parse error!");
        NoViableAltException nvae = (NoViableAltException) t;
        Assertions.assertEquals("}", nvae.getOffendingToken().getText(), "Incorrect offending token");
    }

    @Test
    void parsesJsonObjectWithNumberField() {
        CliJsonVisitor parser = new CliJsonVisitor();
        String json = "{\"test_string\":23.2}";

        JsonObject parsedJson = parser.visitJsonObj(parse(json).jsonObj());

        Assertions.assertTrue(parsedJson.containsKey("test_string"), "Missing field key!");
        final JsonValue parsedField = parsedJson.get("test_string");
        Assertions.assertTrue(parsedField instanceof JsonNumber, "Did not parse a number field correctly");
        assertNumbersMatch(new BigDecimal("23.2"), (JsonNumber) parsedField);
    }

    @Test
    void parsesJsonObjectWithIntegerField() {
        CliJsonVisitor parser = new CliJsonVisitor();
        String json = "{\"test_string\":23}";

        JsonObject parsedJson = parser.visitJsonObj(parse(json).jsonObj());

        Assertions.assertTrue(parsedJson.containsKey("test_string"), "Missing field key!");
        final JsonValue parsedField = parsedJson.get("test_string");
        Assertions.assertTrue(parsedField instanceof JsonNumber, "Did not parse a number field correctly");
        assertNumbersMatch(new BigDecimal("23"), (JsonNumber) parsedField);
    }

    @Test
    void parsesJsonObjectWithJsonObject() throws Exception {
        CliJsonVisitor parser = new CliJsonVisitor();
        String json = "{\"test_string\":{\"nested_field\":123}}";

        JsonObject parsedJson = parser.visitJsonObj(parse(json).jsonObj());
        Assertions.assertTrue(parsedJson.containsKey("test_string"), "Missing field key!");
        JsonObject nested = parsedJson.getJsonObject("test_string");
        Assertions.assertTrue(nested.containsKey("nested_field"), "Missing nested field");
        assertNumbersMatch(new BigDecimal("123"), nested.getJsonNumber("nested_field"));
    }

    @Test
    void parsesJsonObjectWithInvalidNumberField() {
        String json = "{\"test_string\":23s2}";

        ParseCancellationException pce = Assertions.assertThrows(ParseCancellationException.class, () -> parse(json).jsonObj());
        Throwable t = pce.getCause();
        Assertions.assertTrue(t instanceof InputMismatchException, "Incorrect parse error");
        InputMismatchException ime = (InputMismatchException) t;
        Assertions.assertEquals("2", ime.getOffendingToken().getText());
    }

    @Test
    void parsesJsonObjectWithNull() throws Exception {
        CliJsonVisitor parser = new CliJsonVisitor();
        String json = "{\"test_string\":null}";

        JsonObject parsedJson = parser.visitJsonObj(parse(json).jsonObj());

        Assertions.assertTrue(parsedJson.containsKey("test_string"), "Missing field key!");
        final JsonValue parsedField = parsedJson.get("test_string");
        Assertions.assertEquals(JsonValue.NULL, parsedField, "Did not produce a null!");
    }

    @Test
    void parseObjectWithArrayOfStrings() throws Exception {
        CliJsonVisitor parser = new CliJsonVisitor();
        String json = "{\"test_string\":[\"string1\"]}";

        JsonObject parsedJson = parser.visitJsonObj(parse(json).jsonObj());

        Assertions.assertTrue(parsedJson.containsKey("test_string"), "Missing field key!");
        final JsonArray array = parsedJson.getJsonArray("test_string");
        List<String> values = List.of("string1");
        List<String> parsedValued = array.stream()
                .map(jv -> ((JsonString) jv).getString())
                .collect(Collectors.toList());

        Assertions.assertIterableEquals(values, parsedValued, "Incorrect array of strings!");
    }

    @Test
    void parseObjectWithArrayOfObjects() throws Exception {
        CliJsonVisitor parser = new CliJsonVisitor();
        String json = "{\"test_string\":[{\"string1\":\"value1\"}]}";

        JsonObject parsedJson = parser.visitJsonObj(parse(json).jsonObj());

        Assertions.assertTrue(parsedJson.containsKey("test_string"), "Missing field key!");
        final JsonArray array = parsedJson.getJsonArray("test_string");
        List<String> values = List.of("value1");
        List<JsonObject> parsedValued = array.stream()
                .map(jv -> (JsonObject) jv)
                .collect(Collectors.toList());

        Assertions.assertEquals(values.size(), parsedValued.size());
        parsedValued.forEach(jsonObject -> Assertions.assertTrue(jsonObject.containsKey("string1"), "Missing field!"));
        List<String> nestedValues = parsedValued.stream()
                .map(jo -> jo.getString("string1"))
                .collect(Collectors.toList());

        Assertions.assertIterableEquals(values, nestedValues, "Incorrect array of strings!");
    }

    @Test
    void parseObjectWithEmptyArray() throws Exception {
        CliJsonVisitor parser = new CliJsonVisitor();
        String json = "{\"test_string\":[]}";

        JsonObject parsedJson = parser.visitJsonObj(parse(json).jsonObj());

        Assertions.assertTrue(parsedJson.containsKey("test_string"), "Missing field key!");
        final JsonArray array = parsedJson.getJsonArray("test_string");
        List<String> values = Collections.emptyList();
        List<String> parsedValued = array.stream()
                .map(jv -> ((JsonString) jv).getString())
                .collect(Collectors.toList());

        Assertions.assertIterableEquals(values, parsedValued, "Incorrect empty array!");
    }

    @Test
    void parseObjectWithArrayOfNumbers() throws Exception {
        CliJsonVisitor parser = new CliJsonVisitor();
        String json = "{\"test_string\":[23.2,1234]}";

        JsonObject parsedJson = parser.visitJsonObj(parse(json).jsonObj());

        Assertions.assertTrue(parsedJson.containsKey("test_string"), "Missing field key!");
        final JsonArray array = parsedJson.getJsonArray("test_string");
        List<BigDecimal> values = List.of(new BigDecimal("23.2"), new BigDecimal("1234"));
        List<BigDecimal> parsedValued = array.stream()
                .map(jv -> ((JsonNumber) jv).bigDecimalValue())
                .collect(Collectors.toList());

        Assertions.assertIterableEquals(values, parsedValued, "Incorrect array of Numbers!");
    }

    @Test
    void parseObjectWithArrayOfBooleans() throws Exception {
        CliJsonVisitor parser = new CliJsonVisitor();
        String json = "{\"test_string\":[true,true,false]}";

        JsonObject parsedJson = parser.visitJsonObj(parse(json).jsonObj());

        Assertions.assertTrue(parsedJson.containsKey("test_string"), "Missing field key!");
        final JsonArray array = parsedJson.getJsonArray("test_string");
        List<Boolean> values = List.of(true, true, false);
        List<Boolean> parsedValued = array.stream()
                .map(jv -> {
                    if (jv == JsonValue.TRUE) {
                        return true;
                    } else if (jv == JsonValue.FALSE) {
                        return false;
                    }
                    Assertions.fail("Unexpected non-boolean value!");
                    return false;
                })
                .collect(Collectors.toList());

        Assertions.assertIterableEquals(values, parsedValued, "Incorrect array of Numbers!");
    }

    private CliParser parse(String command) {
        CliLexer lexer = new CliLexer(CharStreams.fromString(command));
        CliParser parser = new CliParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(new BailErrorStrategy());
        lexer.removeErrorListeners();
        parser.removeErrorListeners();
        return parser;
    }

    private static class TestString implements JsonString {
        private final String expectedString;

        public TestString(String expectedString) {
            this.expectedString = expectedString;
        }

        @Override
        public String getString() {
            return expectedString;
        }

        @Override
        public CharSequence getChars() {
            return expectedString;
        }

        @Override
        public ValueType getValueType() {
            return ValueType.STRING;
        }

        @Override
        public int hashCode() {
            return expectedString.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof JsonString)) {
                return false;
            }
            return getString().equals(((JsonString) obj).getString());
        }

    }

    private void assertNumbersMatch(BigDecimal expectedNumber, JsonNumber parsedField) {
        Assertions.assertEquals(expectedNumber, parsedField.bigDecimalValue(), "Incorrect big decimal value!");
        Assertions.assertEquals(expectedNumber.longValue(), parsedField.longValue(), "Incorrect long value");
        Assertions.assertEquals(expectedNumber.intValue(), parsedField.intValue(), "Incorrect int value");
        Assertions.assertEquals(expectedNumber.doubleValue(), parsedField.doubleValue(), "Incorrect double value");
        Assertions.assertEquals(JsonValue.ValueType.NUMBER, parsedField.getValueType(), "Incorrect value type");

        try {
            long exactLong = expectedNumber.longValueExact();
            long actualExactLong = parsedField.longValueExact(); //might throw an error, which will fail the test
            Assertions.assertEquals(exactLong, actualExactLong, "Incorrect exact long");
            Assertions.assertTrue(parsedField.isIntegral());

            BigInteger expectedIntVal = expectedNumber.toBigInteger();
            Assertions.assertEquals(expectedIntVal, parsedField.bigIntegerValue(), "Incorrect bigInteger value");

            expectedIntVal = expectedNumber.toBigIntegerExact();
            Assertions.assertEquals(expectedIntVal, parsedField.bigIntegerValueExact(), "Incorrect bigIntegerExact value");
        } catch (ArithmeticException ae) {
            Assertions.assertThrows(ArithmeticException.class, parsedField::longValueExact);
            Assertions.assertThrows(ArithmeticException.class, parsedField::bigIntegerValueExact);
            Assertions.assertFalse(parsedField.isIntegral());
        }

        try {
            int exactInt = expectedNumber.intValueExact();
            int actualExactInt = parsedField.intValueExact(); //might throw an error, which will fail the test
            Assertions.assertEquals(exactInt, actualExactInt, "Incorrect exact int");
            Assertions.assertTrue(parsedField.isIntegral());
        } catch (ArithmeticException ae) {
            Assertions.assertThrows(ArithmeticException.class, parsedField::intValueExact);
        }

    }
}
