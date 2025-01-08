/*
 * GenerateVisitorAnnotationProcessor.java
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

package com.apple.foundationdb.annotation;

import com.google.auto.service.AutoService;
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
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
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
 * <p>
 * Annotation processor to automatically generate a visitor class for the entire class hierarchy underneath the annotated
 * root class or all implementors of an annotated interface within the unit of compilation. In that sense the
 * annotation views the class/interface hierarchy as a sealed trait.
 * </p><p>
 * Please see {@link GenerateVisitor} for a more conceptual overview. First, the annotation processor
 * discovers all extending/implementing (non-abstract) classes of the annotated interface or class. Due to the way
 * java compiles a module/sub-project/compilation unit, subclasses/implementors in downstream projects are naturally
 * not discoverable.
 * </p><p>
 * Once a set of classes has been discovered, we generate a visitor interface (for convenient mix-ins rather than
 * a superclass) that has a visitation method for each discovered class {@code CLAZZ}:
 * </p><p>
 * {@code
 * ...
 * T visitCLAZZ(@Nonnull final CLAZZ element);
 * ...
 * }
 * </p><p>
 * In addition to a specific visitation method for each such {@code CLAZZ}, there are two other methods defined:
 * </p><p>
 * {@code
 * default T visit(@Nonnull final ANNOTATED_ROOT_CLASS element) ...
 * }
 * </p><p>
 * The default implementation of this method is to dispatch over the dynamic type of the argument {@code element} to the
 * specific visitation methods. If {@code element}'s dynamic type is unknown to the visitor, the method:
 * </p><p>
 * {@code
 * default T visitDefault(@Nonnull final ANNOTATED_ROOT_CLASS element);
 * }
 * </p><p>
 * is called. All methods except {@code visit()} itself must be implemented by the user. Using the annotation processor
 * makes sure every subclass known to the compiler is represented by its own visitation method and is adequately
 * dispatched to. Due to the encoded dispatching, the original class hierarchy does not need to implement an
 * {@code accept()} method.
 * </p><p>
 * In cases where generic code takes care of the bulk of the logic but there may be some specialization for a few
 * subclasses, a second interface called {@code ...WithDefaults} is created with default implementations of all
 * visitation methods which just call {@code visitDefault()}. You can implement the generic logic in
 * {@code visitDefault()}, implement the specific overrides by overriding the specific visitation method.
 * </p>
 */
@AutoService(Processor.class)
@SuppressWarnings("PMD.GuardLogStatement") // confused by error invocation
public class GenerateVisitorAnnotationProcessor extends AbstractProcessor {
    private static final String parameterName = "element";

    @Override
    public synchronized void init(@Nonnull final ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
    }

    @Nonnull
    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Set.of(GenerateVisitor.class.getCanonicalName());
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        final var elementUtils = processingEnv.getElementUtils();
        final var typeUtils = processingEnv.getTypeUtils();
        final var messager = processingEnv.getMessager();
        final var filer = processingEnv.getFiler();


        for (final Element annotatedElement : roundEnv.getElementsAnnotatedWith(GenerateVisitor.class)) {
            if (!annotatedElement.getKind().isClass() && !annotatedElement.getKind().isInterface()) {
                error(messager, annotatedElement, "only classes and interfaces can be annotated with %s", GenerateVisitor.class.getSimpleName());
                return true;
            }

            final var rootTypeElement = (TypeElement) annotatedElement;

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

    private void generateCode(@Nonnull final Types typeUtils,
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

    private void generateInterface(@Nonnull final Types typeUtils,
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

    private void generateImplementationWithDefaults(@Nonnull final Types typeUtils,
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

    private String methodNameOfVisitMethod(@Nonnull final GenerateVisitor generateVisitor, @Nonnull TypeElement typeElement) {
        return generateVisitor.methodPrefix() + typeElement.getSimpleName().toString().replace(generateVisitor.stripPrefix(), "");
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean isValidClass(final TypeElement annotatedClassElement) {
        return annotatedClassElement.getModifiers().contains(Modifier.PUBLIC);
    }

    private void error(final Messager messager,
                       final Element e,
                       final String msg,
                       final Object... args) {
        Objects.requireNonNull(messager).printMessage(Diagnostic.Kind.ERROR,
                String.format(Locale.ROOT, msg, args),
                e);
    }
}
