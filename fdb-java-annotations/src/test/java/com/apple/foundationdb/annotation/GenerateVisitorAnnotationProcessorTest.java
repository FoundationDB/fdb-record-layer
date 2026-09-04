/*
 * GenerateVisitorAnnotationProcessorTest.java
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

package com.apple.foundationdb.annotation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compiles sources annotated with {@link GenerateVisitor} and asserts what the processor makes of them.
 */
class GenerateVisitorAnnotationProcessorTest {

    @TempDir
    Path workingDir;

    /**
     * A hierarchy whose subclasses are nested -- one of them two levels deep, one of them generic, one of them abstract.
     */
    private static final String SHAPES = """
            package fixture;

            import com.apple.foundationdb.annotation.GenerateVisitor;

            @GenerateVisitor
            public abstract class Shape {
                public static final class Circle extends Shape {
                }

                public static final class Box<T> extends Shape {
                }

                public abstract static class Curved extends Shape {
                    public static final class Wobble extends Curved {
                    }
                }
            }
            """;

    @Test
    void visitsEveryConcreteSubclassIncludingNestedOnes() throws Exception {
        final var result = compile(SHAPES);
        assertThat(result.succeeded()).as(result.messages()).isTrue();

        final var visitor = result.generatedSource("fixture/ShapeVisitor.java");
        assertThat(visitor)
                .contains("visitCircle")
                .contains("visitBox")
                .contains("visitWobble");
    }

    @Test
    void skipsAbstractSubclasses() throws Exception {
        final var visitor = compile(SHAPES).generatedSource("fixture/ShapeVisitor.java");
        assertThat(visitor).doesNotContain("visitCurved");
    }

    @Test
    void namesAGenericSubclassWithWildcards() throws Exception {
        final var visitor = compile(SHAPES).generatedSource("fixture/ShapeVisitor.java");
        // the type variable of Box must not reach the visitor, where it would capture the visitor's own result variable
        assertThat(visitor)
                .contains("Shape.Box<?> element")
                .doesNotContain("Shape.Box<T> element");
    }

    @Test
    void namesANestedSubclassThroughItsEnclosingType() throws Exception {
        final var visitor = compile(SHAPES).generatedSource("fixture/ShapeVisitor.java");
        assertThat(visitor).contains("Shape.Curved.Wobble.class");
    }

    @Test
    void generatesAnImplementationThatDefaultsEveryVisitation() throws Exception {
        final var withDefaults = compile(SHAPES).generatedSource("fixture/ShapeVisitorWithDefaults.java");
        assertThat(withDefaults)
                .contains("interface ShapeVisitorWithDefaults")
                .contains("visitCircle")
                .contains("visitDefault(element)");
    }

    @Test
    void rejectsANonPublicSubclass() throws Exception {
        final var result = compile("""
                package fixture;

                import com.apple.foundationdb.annotation.GenerateVisitor;

                @GenerateVisitor
                public abstract class Thing {
                    static final class Hidden extends Thing {
                    }
                }
                """);
        assertThat(result.succeeded()).isFalse();
        assertThat(result.messages())
                .contains("fixture.Thing.Hidden is a non-public subclass of fixture.Thing");
    }

    @Test
    void rejectsANonPublicEnclosingType() throws Exception {
        final var result = compile("""
                package fixture;

                import com.apple.foundationdb.annotation.GenerateVisitor;

                @GenerateVisitor
                public abstract class Thing {
                    static class Enclosing {
                        public static final class Visible extends Thing {
                        }
                    }
                }
                """);
        assertThat(result.succeeded()).isFalse();
        assertThat(result.messages()).contains("is a non-public subclass of fixture.Thing");
    }

    @Nonnull
    private Result compile(@Nonnull final String source) throws IOException {
        final var sourceDir = Files.createDirectories(workingDir.resolve("src/fixture"));
        final var classesDir = Files.createDirectories(workingDir.resolve("classes"));
        final var generatedDir = Files.createDirectories(workingDir.resolve("generated"));

        final var typeName = source.replaceAll("(?s).*public abstract class (\\w+).*", "$1");
        final var sourceFile = sourceDir.resolve(typeName + ".java");
        Files.writeString(sourceFile, source, StandardCharsets.UTF_8);

        final var compiler = ToolProvider.getSystemJavaCompiler();
        final var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (var fileManager = compiler.getStandardFileManager(diagnostics, null, StandardCharsets.UTF_8)) {
            fileManager.setLocationFromPaths(StandardLocation.CLASS_OUTPUT, List.of(classesDir));
            fileManager.setLocationFromPaths(StandardLocation.SOURCE_OUTPUT, List.of(generatedDir));
            final var succeeded = compiler.getTask(null, fileManager, diagnostics,
                    List.of("-classpath", System.getProperty("java.class.path"),
                            "-processor", GenerateVisitorAnnotationProcessor.class.getName()),
                    null,
                    fileManager.getJavaFileObjects(sourceFile)).call();
            return new Result(succeeded, diagnostics.getDiagnostics().stream()
                    .map(diagnostic -> diagnostic.getMessage(null))
                    .collect(Collectors.joining("\n")), generatedDir);
        }
    }

    /**
     * What one compilation produced.
     */
    private record Result(boolean succeeded, @Nonnull String messages, @Nonnull Path generatedDir) {

        @Nonnull
        String generatedSource(@Nonnull final String relativePath) throws IOException {
            final var generated = generatedDir.resolve(relativePath);
            assertThat(generated).as("generated sources: " + generatedDir).exists();
            return Files.readString(generated, StandardCharsets.UTF_8);
        }
    }
}
