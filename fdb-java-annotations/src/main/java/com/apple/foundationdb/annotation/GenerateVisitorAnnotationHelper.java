/*
 * GenerateVisitorAnnotationHelper.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import com.squareup.javapoet.WildcardTypeName;

import javax.annotation.Nonnull;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A separate class to support (@link GenerateVisitorAnnotationProcessor) so that dependency on javapoet does not leak to anyone
 * just service loading all annotation processors in the class path.
 */
@SuppressWarnings("PMD.GuardLogStatement") // confused by error invocation
class GenerateVisitorAnnotationHelper {
    private static final String parameterName = "element";

    private GenerateVisitorAnnotationHelper() {
    }

    static boolean process(final ProcessingEnvironment processingEnv, Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        final var elementUtils = processingEnv.getElementUtils();
        final var typeUtils = processingEnv.getTypeUtils();
        final var messager = processingEnv.getMessager();
        final var filer = processingEnv.getFiler();

        for (final Element annotatedElement : roundEnv.getElementsAnnotatedWith(GenerateVisitor.class)) {
            if (!annotatedElement.getKind().isClass() && !annotatedElement.getKind().isInterface()) {
                error(messager, annotatedElement, "only classes and interfaces can be annotated with %s", GenerateVisitor.class.getSimpleName());
                return true;
            }

            final var rootTypeElement = (TypeElement)annotatedElement;

            final var moduleElement = elementUtils.getModuleOf(annotatedElement);
            if (moduleElement == null) {
                error(messager, annotatedElement, "cannot annotate class with  %s in null-module", GenerateVisitor.class.getSimpleName());
                return true;
            }

            if (!isValidClass(rootTypeElement)) {
                error(messager,
                        rootTypeElement, "The class %s cannot be annotated with this annotation.",
                        rootTypeElement.getQualifiedName().toString());
                return true;
            }

            final var generateVisitor = annotatedElement.getAnnotation(GenerateVisitor.class);
            final var rootTypeMirror = rootTypeElement.asType();

            final var packageOfRoot = elementUtils.getPackageOf(rootTypeElement);
            final var candidateElements = moduleElement
                    .getEnclosedElements()
                    .stream()
                    .flatMap(GenerateVisitorAnnotationHelper::enclosedTypesRecursively)
                    .filter(element -> element.getKind() == ElementKind.CLASS &&
                            !element.getModifiers().contains(Modifier.ABSTRACT))
                    .filter(element -> {
                        final var mirror = element.asType();
                        return mirror.getKind() == TypeKind.DECLARED && typeUtils.isSubtype(mirror, rootTypeMirror);
                    })
                    .collect(Collectors.toList());

            //
            // The generated visitor lives in the package of the annotated root, so it cannot name a subclass that is
            // not public, or that is nested inside a type that is not. Silently skipping such a subclass would leave it
            // without a visitation method and quietly route it to the default one, which is exactly the kind of gap
            // this generator exists to prevent -- so it is an error instead.
            //
            final var inaccessibleElements = candidateElements
                    .stream()
                    .filter(element -> !isPublicIncludingEnclosingTypes(element))
                    .collect(Collectors.toList());
            if (!inaccessibleElements.isEmpty()) {
                for (final var inaccessibleElement : inaccessibleElements) {
                    error(messager, inaccessibleElement,
                            "%s is a non-public subclass of %s, so the generated visitor cannot name it; make it and "
                                    + "all of its enclosing types public, or move it out of its enclosing type",
                            ((TypeElement)inaccessibleElement).getQualifiedName().toString(),
                            rootTypeElement.getQualifiedName().toString());
                }
                return true;
            }

            final var subClassTypeMirrors = candidateElements
                    .stream()
                    .map(Element::asType)
                    .collect(Collectors.toList());

            try {
                generateCode(typeUtils, filer, generateVisitor, packageOfRoot, rootTypeElement, subClassTypeMirrors);
            } catch (final Exception exception) {
                Objects.requireNonNull(messager)
                        .printMessage(Diagnostic.Kind.ERROR,
                                "unable to generate visitor in " + packageOfRoot.getQualifiedName() + "[" + exception.getMessage() + "]");
            }
        }

        return true;
    }

    private static void generateCode(@Nonnull final Types typeUtils,
                                     @Nonnull final Filer filer,
                                     @Nonnull GenerateVisitor generateVisitor,
                                     @Nonnull final PackageElement packageElement,
                                     @Nonnull final TypeElement rootTypeElement,
                                     @Nonnull final List<TypeMirror> subClassTypeMirrors) throws IOException {
        final var rootTypeMirror = rootTypeElement.asType();
        final var interfaceName = rootTypeElement.getSimpleName() + generateVisitor.classSuffix();
        final var typeVariableName = TypeVariableName.get("T");
        final var defaultMethodName = generateVisitor.methodPrefix() + "Default";

        generateInterface(typeUtils, filer, generateVisitor, packageElement, subClassTypeMirrors, rootTypeMirror, interfaceName, typeVariableName, defaultMethodName);

        final var className = rootTypeElement.getSimpleName() + generateVisitor.classSuffix() + "WithDefaults";
        generateImplementationWithDefaults(typeUtils, filer, generateVisitor, packageElement, subClassTypeMirrors, className, interfaceName, typeVariableName, defaultMethodName);
    }

    private static void generateInterface(@Nonnull final Types typeUtils,
                                          @Nonnull final Filer filer,
                                          @Nonnull final GenerateVisitor generateVisitor,
                                          @Nonnull final PackageElement packageElement,
                                          @Nonnull final List<TypeMirror> subClassTypeMirrors,
                                          @Nonnull final TypeMirror rootTypeMirror,
                                          @Nonnull final String interfaceName,
                                          @Nonnull final TypeVariableName typeVariableName,
                                          @Nonnull final String defaultMethodName) throws IOException {
        final TypeSpec.Builder typeBuilder =
                TypeSpec.interfaceBuilder(interfaceName)
                        .addModifiers(Modifier.PUBLIC)
                        .addTypeVariable(typeVariableName);

        final var jumpMapBuilder = FieldSpec.builder(ParameterizedTypeName.get(ClassName.get(Map.class),
                        ParameterizedTypeName.get(ClassName.get(Class.class), WildcardTypeName.subtypeOf(Object.class)),
                        ParameterizedTypeName.get(ClassName.get(BiFunction.class),
                                ParameterizedTypeName.get(ClassName.get(packageElement.getQualifiedName().toString(), interfaceName), WildcardTypeName.subtypeOf(Object.class)),
                                TypeName.get(rootTypeMirror),
                                WildcardTypeName.subtypeOf(Object.class))),
                "jumpMap", Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL);

        //
        // Emitted through JavaPoet type placeholders rather than by concatenating simple names: a nested type has to be
        // named as Outer.Inner and needs an import for Outer, and a generic type has to be cast to its
        // wildcard-parameterized form so that the cast is checked rather than raw.
        //
        final var initializerBuilder = CodeBlock.builder();
        initializerBuilder.add("$T.ofEntries(", ClassName.get(Map.class));
        boolean firstEntry = true;
        for (final var typeMirror : subClassTypeMirrors) {
            final var typeElement = (TypeElement)typeUtils.asElement(typeMirror);
            if (!firstEntry) {
                initializerBuilder.add(", \n");
            }
            firstEntry = false;
            initializerBuilder.add("$T.entry($T.class, (visitor, element) -> visitor.$L(($T)element))",
                    ClassName.get(Map.class),
                    ClassName.get(typeElement),
                    methodNameOfVisitMethod(generateVisitor, typeElement),
                    visitableTypeName(typeElement));
        }
        initializerBuilder.add(")");

        typeBuilder.addField(jumpMapBuilder
                .initializer(initializerBuilder.build())
                .build());

        for (final var typeMirror : subClassTypeMirrors) {
            final var typeElement = (TypeElement)typeUtils.asElement(typeMirror);
            final var methodName = methodNameOfVisitMethod(generateVisitor, typeElement);
            final MethodSpec.Builder specificVisitMethodBuilder =
                    MethodSpec
                            .methodBuilder(methodName)
                            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                            .addAnnotation(Nonnull.class)
                            .addParameter(ParameterSpec.builder(visitableTypeName(typeElement), parameterName).addAnnotation(Nonnull.class).build())
                            .returns(typeVariableName);
            typeBuilder.addMethod(specificVisitMethodBuilder.build());
        }

        final MethodSpec.Builder visitDefaultMethodBuilder =
                MethodSpec
                        .methodBuilder(defaultMethodName)
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .addAnnotation(Nonnull.class)
                        .addParameter(ParameterSpec.builder(TypeName.get(rootTypeMirror), parameterName).addAnnotation(Nonnull.class).build())
                        .returns(typeVariableName);
        typeBuilder.addMethod(visitDefaultMethodBuilder.build());

        final MethodSpec.Builder visitMethodBuilder =
                MethodSpec
                        .methodBuilder(generateVisitor.methodPrefix())
                        .addModifiers(Modifier.PUBLIC, Modifier.DEFAULT)
                        .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "unchecked").build())
                        .addParameter(ParameterSpec.builder(TypeName.get(rootTypeMirror), parameterName).addAnnotation(Nonnull.class).build())
                        .returns(typeVariableName)
                        .addCode(CodeBlock.builder()
                                .addStatement("final var visitFn = jumpMap.get(" + parameterName + ".getClass())")
                                .addStatement("return visitFn == null ? visitDefault(" + parameterName + ") : (" + typeVariableName + ")visitFn.apply(this, " + parameterName + ")")
                                .build());
        typeBuilder.addMethod(visitMethodBuilder.build());

        JavaFile.builder(packageElement.getQualifiedName().toString(), typeBuilder.build())
                .skipJavaLangImports(true)
                .build()
                .writeTo(Objects.requireNonNull(filer));
    }

    private static void generateImplementationWithDefaults(@Nonnull final Types typeUtils,
                                                           @Nonnull final Filer filer,
                                                           @Nonnull final GenerateVisitor generateVisitor,
                                                           @Nonnull final PackageElement packageElement,
                                                           @Nonnull final List<TypeMirror> subClassTypeMirrors,
                                                           @Nonnull final String className,
                                                           @Nonnull final String interfaceName,
                                                           @Nonnull final TypeVariableName typeVariableName,
                                                           @Nonnull final String defaultMethodName) throws IOException {
        final TypeSpec.Builder typeBuilder =
                TypeSpec.interfaceBuilder(className)
                        .addModifiers(Modifier.PUBLIC)
                        .addTypeVariable(typeVariableName)
                        .addSuperinterface(ParameterizedTypeName.get(ClassName.get(packageElement.getQualifiedName().toString(), interfaceName), typeVariableName));

        for (final var typeMirror : subClassTypeMirrors) {
            final var typeElement = (TypeElement)typeUtils.asElement(typeMirror);
            final var methodName = methodNameOfVisitMethod(generateVisitor, typeElement);
            final MethodSpec.Builder specificVisitMethodBuilder =
                    MethodSpec
                            .methodBuilder(methodName)
                            .addModifiers(Modifier.PUBLIC, Modifier.DEFAULT)
                            .addAnnotation(Nonnull.class)
                            .addAnnotation(Override.class)
                            .addParameter(ParameterSpec.builder(visitableTypeName(typeElement), parameterName).addAnnotation(Nonnull.class).build())
                            .returns(typeVariableName)
                            .addCode(CodeBlock.builder()
                                    .addStatement("return " + defaultMethodName + "(" + parameterName + ")")
                                    .build());
            typeBuilder.addMethod(specificVisitMethodBuilder.build());
        }

        JavaFile.builder(packageElement.getQualifiedName().toString(), typeBuilder.build())
                .skipJavaLangImports(true)
                .build()
                .writeTo(Objects.requireNonNull(filer));
    }

    private static String methodNameOfVisitMethod(@Nonnull final GenerateVisitor generateVisitor, @Nonnull TypeElement typeElement) {
        return generateVisitor.methodPrefix() + typeElement.getSimpleName().toString().replace(generateVisitor.stripPrefix(), "");
    }

    /**
     * Returns the given element together with every type nested inside it, at any depth. The original implementation
     * only looked at the types directly enclosed by a package, which silently skipped every nested subclass of an
     * annotated root.
     * <p>
     * The enclosing type is included alongside its nested types, not replaced by them: a type is routinely both a
     * dispatch target and a container of nested helpers -- almost every {@code Value}, for instance, nests its own
     * {@code Deserializer} -- so descending into a type must not remove the type itself from consideration.
     * </p>
     *
     * @param element the element to descend into, a package or a type
     *
     * @return a stream of the types enclosed by {@code element}, recursively
     */
    @Nonnull
    private static Stream<Element> enclosedTypesRecursively(@Nonnull final Element element) {
        return element.getEnclosedElements()
                .stream()
                .filter(enclosed -> enclosed.getKind().isClass() || enclosed.getKind().isInterface())
                .flatMap(enclosed -> Stream.concat(Stream.of(enclosed), enclosedTypesRecursively(enclosed)));
    }

    /**
     * Returns whether the given type and all of the types enclosing it are public. A nested type that is not, or that
     * is nested inside one that is not, cannot be named by the generated visitor interface, which lives in the package
     * of the annotated root rather than inside the enclosing type.
     *
     * @param element the type to check
     *
     * @return {@code true} if the type is visible to generated code
     */
    private static boolean isPublicIncludingEnclosingTypes(@Nonnull final Element element) {
        for (Element current = element; current != null && !(current instanceof PackageElement);
                current = current.getEnclosingElement()) {
            if (!current.getModifiers().contains(Modifier.PUBLIC)) {
                return false;
            }
        }
        return true;
    }

    /**
     * The name to use when the generated code needs to refer to the given type.
     * <p>
     * A generic type has to be named with a wildcard for each of its type parameters. Using the type as declared would
     * leak its type variables into the visitor interface, where they are out of scope and, if a type parameter happens
     * to be named like the visitor's own result variable, silently capture it instead -- {@code LiteralValue<T>} in a
     * {@code ValueVisitor<T>} reads as "a literal whose type is the visitor's result type", which is not what is meant.
     * </p>
     *
     * @param typeElement the type to name
     *
     * @return the type name to emit, wildcard-parameterized if the type is generic
     */
    @Nonnull
    private static TypeName visitableTypeName(@Nonnull final TypeElement typeElement) {
        final var className = ClassName.get(typeElement);
        final var typeParameters = typeElement.getTypeParameters();
        if (typeParameters.isEmpty()) {
            return className;
        }
        final var wildcards = typeParameters.stream()
                .map(ignored -> (TypeName)WildcardTypeName.subtypeOf(Object.class))
                .toArray(TypeName[]::new);
        return ParameterizedTypeName.get(className, wildcards);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean isValidClass(final TypeElement annotatedClassElement) {
        return annotatedClassElement.getModifiers().contains(Modifier.PUBLIC);
    }

    private static void error(final Messager messager,
               final Element e,
               final String msg,
               final Object... args) {
        Objects.requireNonNull(messager).printMessage(Diagnostic.Kind.ERROR,
                String.format(Locale.ROOT, msg, args),
                e);
    }
}
