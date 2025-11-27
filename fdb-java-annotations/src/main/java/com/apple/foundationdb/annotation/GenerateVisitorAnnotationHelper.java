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
            final var subClassTypeMirrors = moduleElement
                    .getEnclosedElements()
                    .stream()
                    .flatMap(packageElement -> packageElement.getEnclosedElements().stream())
                    .filter(element -> element.getKind() == ElementKind.CLASS &&
                            !element.getModifiers().contains(Modifier.ABSTRACT))
                    .map(Element::asType)
                    .filter(mirror -> mirror.getKind() == TypeKind.DECLARED)
                    .filter(mirror -> typeUtils.isSubtype(mirror, rootTypeMirror))
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

        final var initializerStrings = subClassTypeMirrors.stream()
                .map(typeMirror -> {
                    final var typeElement = (TypeElement)typeUtils.asElement(typeMirror);
                    return "Map.entry(" + typeElement.getSimpleName() + ".class, (visitor, element) -> visitor." + methodNameOfVisitMethod(generateVisitor, typeElement) + "((" + typeElement.getSimpleName() + ")element))";
                })
                .collect(Collectors.joining(", \n"));

        final var initializerBlock = CodeBlock.builder()
                .add("$T.ofEntries(" + initializerStrings + ")", ClassName.get(Map.class))
                .build();

        typeBuilder.addField(jumpMapBuilder
                .initializer(initializerBlock)
                .build());

        for (final var typeMirror : subClassTypeMirrors) {
            final var typeElement = (TypeElement)typeUtils.asElement(typeMirror);
            final var methodName = methodNameOfVisitMethod(generateVisitor, typeElement);
            final MethodSpec.Builder specificVisitMethodBuilder =
                    MethodSpec
                            .methodBuilder(methodName)
                            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                            .addAnnotation(Nonnull.class)
                            .addParameter(ParameterSpec.builder(TypeName.get(typeMirror), parameterName).addAnnotation(Nonnull.class).build())
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
                            .addParameter(ParameterSpec.builder(TypeName.get(typeMirror), parameterName).addAnnotation(Nonnull.class).build())
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
