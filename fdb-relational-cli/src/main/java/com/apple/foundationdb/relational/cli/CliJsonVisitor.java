/*
 * CliJsonVisitor.java
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

import com.apple.foundationdb.relational.generated.CliParser;
import com.apple.foundationdb.relational.generated.CliParserBaseVisitor;

import org.antlr.v4.runtime.misc.Pair;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import java.math.BigDecimal;
import java.math.BigInteger;

class CliJsonVisitor extends CliParserBaseVisitor<Object> {

    @Override
    public JsonArray visitJsonArr(CliParser.JsonArrContext ctx) {
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
        for (CliParser.JsonValueContext valueCtx : ctx.jsonValue()) {
            arrayBuilder.add(visitJsonValue(valueCtx));
        }
        return arrayBuilder.build();
    }

    @Override
    public JsonObject visitJsonObj(CliParser.JsonObjContext ctx) {
        JsonObjectBuilder objBuilder = Json.createObjectBuilder();
        for (CliParser.JsonPairContext pairCtx : ctx.jsonPair()) {
            final Pair<String, JsonValue> pair = visitJsonPair(pairCtx);
            objBuilder.add(pair.a, pair.b);
        }
        return objBuilder.build();
    }

    @Override
    public JsonValue visitJsonValue(CliParser.JsonValueContext ctx) {
        if (ctx.jsonObj() != null) {
            return visitJsonObj(ctx.jsonObj());
        } else if (ctx.jsonArr() != null) {
            return visitJsonArr(ctx.jsonArr());
        } else if (ctx.jsonBool() != null) {
            return visitJsonBool(ctx.jsonBool());
        } else if (ctx.JSON_NUMBER() != null) {
            return new JsonBigDec(new BigDecimal(ctx.JSON_NUMBER().getText()));
        } else if (ctx.JSON_STRING() != null) {
            String value = stripQuotes(ctx.JSON_STRING().getText());
            return new JsonString() {
                @Override
                public String getString() {
                    return value;
                }

                @Override
                public CharSequence getChars() {
                    return value;
                }

                @Override
                public ValueType getValueType() {
                    return ValueType.STRING;
                }

                @Override
                public int hashCode() {
                    return value.hashCode();
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
            };
        } else {
            return JsonValue.NULL;
        }
    }

    @Override
    public JsonValue visitJsonBool(CliParser.JsonBoolContext ctx) {
        if (ctx.JTRUE() != null) {
            return JsonValue.TRUE;
        } else {
            return JsonValue.FALSE;
        }
    }

    @Override
    public Pair<String, JsonValue> visitJsonPair(CliParser.JsonPairContext ctx) {
        String name = stripQuotes(ctx.JSON_STRING().getText());
        JsonValue value = visitJsonValue(ctx.jsonValue());
        return new Pair<>(name, value);
    }

    private static class JsonBigDec implements JsonNumber {
        private final BigDecimal value;

        public JsonBigDec(BigDecimal value) {
            this.value = value;
        }

        @Override
        public boolean isIntegral() {
            return BigDecimal.valueOf(value.longValue()).equals(value);
        }

        @Override
        public int intValue() {
            return value.intValue();
        }

        @Override
        public int intValueExact() {
            return value.intValueExact();
        }

        @Override
        public long longValue() {
            return value.longValue();
        }

        @Override
        public long longValueExact() {
            return value.longValueExact();
        }

        @Override
        public BigInteger bigIntegerValue() {
            return value.toBigInteger();
        }

        @Override
        public BigInteger bigIntegerValueExact() {
            return value.toBigIntegerExact();
        }

        @Override
        public double doubleValue() {
            return value.doubleValue();
        }

        @Override
        public BigDecimal bigDecimalValue() {
            return value;
        }

        @Override
        public ValueType getValueType() {
            return ValueType.NUMBER;
        }
    }

    private String stripQuotes(String text) {
        text = text.trim();
        if (text.startsWith("'") || text.startsWith("\"") || text.startsWith("`")) {
            text = text.substring(1, text.length() - 1);
        }
        return text;
    }
}
