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

import com.google.common.annotations.VisibleForTesting;
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
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.util.Arrays;
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
            final var subClassTypeMirrors = moduleElement
                    .getEnclosedElements()
                    .stream()
                    .flatMap(packageElement -> packageElement.getEnclosedElements().stream())
                    .flatMap(element -> element.getKind() == ElementKind.CLASS && element.getModifiers().contains(Modifier.ABSTRACT) ? element.getEnclosedElements().stream() :  Stream.of(element) )
                    .filter(element -> element.getKind() == ElementKind.CLASS && !element.getModifiers().contains(Modifier.ABSTRACT))
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

        final var packageName = packageElement.getQualifiedName().toString();
        final var jumpMapBuilder = FieldSpec.builder(ParameterizedTypeName.get(ClassName.get(Map.class),
                        ParameterizedTypeName.get(ClassName.get(Class.class), WildcardTypeName.subtypeOf(Object.class)),
                        ParameterizedTypeName.get(ClassName.get(BiFunction.class),
                                ParameterizedTypeName.get(ClassName.get(packageName, interfaceName), WildcardTypeName.subtypeOf(Object.class)),
                                TypeName.get(rootTypeMirror),
                                WildcardTypeName.subtypeOf(Object.class))),
                "jumpMap", Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL);

        final var initializerStrings = subClassTypeMirrors.stream()
                .map(typeMirror -> {
                    final var typeElement = (TypeElement)typeUtils.asElement(typeMirror);
                    return "Map.entry(" + getRawTypeName(typeMirror, packageName) + ".class, (visitor, element) -> visitor." + methodNameOfVisitMethod(generateVisitor, typeElement) + "((" + getWildcardTypeName(typeMirror, packageName) + ")element))";
                })
                .collect(Collectors.joining(", \n"));

        final var initializerBlock = CodeBlock.builder()
                .add("$T.ofEntries(" + initializerStrings + ")", ClassName.get(Map.class))
                .build();

        typeBuilder.addField(jumpMapBuilder
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "unchecked").build())
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
                            .addParameter(ParameterSpec.builder(getWildcardTypeName(typeMirror, packageName), parameterName).addAnnotation(Nonnull.class).build())
                            .returns(typeVariableName);
            typeBuilder.addMethod(specificVisitMethodBuilder.build());
        }

        final MethodSpec.Builder visitDefaultMethodBuilder =
                MethodSpec
                        .methodBuilder(defaultMethodName)
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .addAnnotation(Nonnull.class)
                        .addParameter(ParameterSpec.builder(getWildcardTypeName(rootTypeMirror, packageName), parameterName).addAnnotation(Nonnull.class).build())
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

    /**
     * Converts a type mirror to a raw TypeName without type parameters.
     * <p>
     * For generic types, this method returns the raw type without any type arguments
     * (e.g., {@code List<String>} becomes {@code List}). For non-generic types, the type
     * is returned as-is. If the type belongs to the same package as {@code currentPackage},
     * the package prefix is omitted from the generated type name.
     * <p>
     * This is particularly useful when generating code that needs to reference the
     * {@code .class} literal of a generic type, as class literals must use raw types.
     *
     * @param typeMirror the type mirror to convert
     * @param currentPackage the current package name, used to determine whether to omit
     *                       package prefixes for types in the same package
     * @return a TypeName representing the raw type (without type parameters) if the type
     *         is generic, or the original type name if not generic
     */
    @Nonnull
    private static TypeName getRawTypeName(@Nonnull TypeMirror typeMirror, @Nonnull String currentPackage) {
        if (typeMirror.getKind() == TypeKind.DECLARED) {
            final var declaredType = (DeclaredType) typeMirror;
            final var typeElement = (TypeElement) declaredType.asElement();
            final boolean isGeneric = !typeElement.getTypeParameters().isEmpty();

            if (isGeneric) {
                final ClassName className = ClassName.get(typeElement);
                return removePackagePrefix(className, currentPackage);
            }
        }

        // return as-is, remove the package if it is the same as the currentPackage.
        final TypeName typeName = TypeName.get(typeMirror);
        if (typeName instanceof ClassName) {
            return removePackagePrefix((ClassName) typeName, currentPackage);
        }

        return typeName;
    }

    /**
     * Converts a type mirror to a TypeName with wildcard type arguments for generic types.
     * <p>
     * For generic types, this method creates a parameterized type with wildcard bounds
     * (e.g., {@code List<String>} becomes {@code List<?>}). For non-generic types, the type
     * is returned as-is. If the type belongs to the same package as {@code currentPackage},
     * the package prefix is omitted from the generated type name.
     *
     * @param typeMirror the type mirror to convert
     * @param currentPackage the current package name, used to determine whether to omit
     *                       package prefixes for types in the same package
     * @return a TypeName representing the type with wildcard type arguments if the type
     *         is generic, or the original type name if not generic
     */
    @Nonnull
    private static TypeName getWildcardTypeName(@Nonnull final TypeMirror typeMirror, @Nonnull final String currentPackage) {
        if (typeMirror.getKind() == TypeKind.DECLARED) {
            final var declaredType = (DeclaredType) typeMirror;
            final var typeElement = (TypeElement) declaredType.asElement();
            final boolean isGeneric = !typeElement.getTypeParameters().isEmpty();

            if (isGeneric) {
                ClassName rawType = ClassName.get(typeElement);
                rawType = removePackagePrefix(rawType, currentPackage);

                final WildcardTypeName[] wildcards = new WildcardTypeName[typeElement.getTypeParameters().size()];
                Arrays.fill(wildcards, WildcardTypeName.subtypeOf(Object.class));
                return ParameterizedTypeName.get(rawType, wildcards);
            }
        }

        // return as-is, remove the package if it is the same as the currentPackage.
        final TypeName typeName = TypeName.get(typeMirror);
        if (typeName instanceof ClassName) {
            return removePackagePrefix((ClassName) typeName, currentPackage);
        }

        return typeName;
    }

    /**
     * Removes the package prefix from a ClassName if it belongs to the same package as currentPackage.
     * <p>
     * This is useful when generating code references to types that are in the same package,
     * as the package prefix can be omitted for brevity.
     *
     * @param className the ClassName to potentially strip the package prefix from
     * @param currentPackage the current package name to compare against
     * @return a ClassName without the package prefix if it's in the same package,
     *         otherwise returns the original ClassName unchanged
     */
    @Nonnull
    private static ClassName removePackagePrefix(@Nonnull final ClassName className, @Nonnull final String currentPackage) {
        if (className.packageName().equals(currentPackage)) {
            return ClassName.get("", className.topLevelClassName().simpleName(),
                    className.simpleNames().subList(1, className.simpleNames().size()).toArray(new String[0]));
        }
        return className;
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
                            .addParameter(ParameterSpec.builder(getWildcardTypeName(typeMirror, packageElement.getQualifiedName().toString()), parameterName).addAnnotation(Nonnull.class).build())
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
