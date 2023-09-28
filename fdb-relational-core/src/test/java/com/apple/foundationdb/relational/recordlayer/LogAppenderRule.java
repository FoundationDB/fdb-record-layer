/*
 * LogAppenderRule.java
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

package com.apple.foundationdb.relational.recordlayer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class LogAppenderRule implements BeforeEachCallback, AfterEachCallback, AutoCloseable {

    private LogAppender logAppender;
    private Logger logger;
    @Nonnull
    private final String name;
    @Nonnull
    private final Class<?> clazz;
    @Nonnull
    private final Level level;
    private Level beforeLogLevel;

    private static class LogAppender extends AbstractAppender {
        private final List<LogEvent> log = new ArrayList<>();

        protected LogAppender(String name) {
            super(name, null, null, false, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            log.add(event.toImmutable());
        }

        public List<LogEvent> getLogs() {
            return log;
        }
    }

    public LogAppenderRule(@Nonnull String name, @Nonnull Class<?> clazz, @Nonnull Level level) {
        this.name = name;
        this.clazz = clazz;
        this.level = level;
    }

    public static LogAppenderRule of(@Nonnull String name, @Nonnull Class<?> clazz, @Nonnull Level level) throws SQLException {
        final var rule = new LogAppenderRule(name, clazz, level);
        rule.beforeEach(null);
        return rule;
    }

    @Override
    public void afterEach(ExtensionContext context) throws SQLException {
        if (logAppender != null) {
            logAppender.stop();
        }
        if (logger != null) {
            logger.removeAppender(logAppender);
            logger.setLevel(beforeLogLevel);
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws SQLException {
        logAppender = new LogAppender(name);
        logger = (Logger) LogManager.getLogger(clazz);
        logger.addAppender(logAppender);
        beforeLogLevel = logger.getLevel();
        logger.setLevel(level);
        logAppender.start();
    }

    @Override
    public void close() throws Exception {
        afterEach(null);
    }

    public LogEvent getLastLogEvent() {
        return logAppender.getLogs().get(logAppender.getLogs().size() - 1);
    }

    public String getLastLogEventMessage() {
        return getLastLogEvent().getMessage().getFormattedMessage();
    }

    public List<LogEvent> getLogEvents() {
        return logAppender.getLogs();
    }
}
