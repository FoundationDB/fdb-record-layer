/*
 * CheckProtoMessageAnnotationProcessor.java
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
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * TODO.
 */
@AutoService(Processor.class)
@SuppressWarnings("PMD.GuardLogStatement") // confused by error invocation
public class CheckProtoMessageAnnotationProcessor extends AbstractProcessor {
    private static final String fromProto = "fromProto";
    private static final String value = "value";

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
        return Set.of(ProtoMessage.class.getCanonicalName());
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        final var elementUtils = processingEnv.getElementUtils();
        final var typeUtils = processingEnv.getTypeUtils();
        final var messager = processingEnv.getMessager();
        final var filer = processingEnv.getFiler();


        for (final Element annotatedElement : roundEnv.getElementsAnnotatedWith(ProtoMessage.class)) {
            if (!annotatedElement.getKind().isClass()) {
                error(messager, annotatedElement, "only classes can be annotated with %s", ProtoMessage.class.getSimpleName());
                return true;
            }

            final var rootTypeElement = (TypeElement) annotatedElement;

            final var moduleElement = elementUtils.getModuleOf(annotatedElement);
            if (moduleElement == null) {
                error(messager, annotatedElement, "cannot annotate class with  %s in null-module", ProtoMessage.class.getSimpleName());
                return true;
            }

            if (!isValidClass(rootTypeElement)) {
                error(messager,
                        rootTypeElement, "The class %s cannot be annotated with this annotation.",
                        rootTypeElement.getQualifiedName().toString());
                return true;
            }

            final var annotationMirrors = rootTypeElement.getAnnotationMirrors();
            final var protoMessageAnnotationMirrors = annotationMirrors
                    .stream()
                    .filter(annotationMirror -> ((TypeElement)annotationMirror.getAnnotationType().asElement())
                            .getQualifiedName().contentEquals(ProtoMessage.class.getCanonicalName()))
                    .collect(Collectors.toList());
            if (protoMessageAnnotationMirrors.size() != 1) {
                error(messager,
                        rootTypeElement, "Cannot find the annotation mirror for %s for class %s.",
                        ProtoMessage.class.getCanonicalName(),
                        rootTypeElement.getQualifiedName().toString());
                return  true;
            }

            final var protoMessageAnnotationMirror = Iterables.getOnlyElement(protoMessageAnnotationMirrors);
            final var valueMaybe = protoMessageAnnotationMirror.getElementValues().entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().getSimpleName().contentEquals(value))
                    .map(Map.Entry::getValue)
                    .map(AnnotationValue::getValue)
                    .findFirst();
            if (valueMaybe.isEmpty()) {
                error(messager,
                        rootTypeElement, "Cannot find the value in annotation mirror for %s for class %s.",
                        ProtoMessage.class.getCanonicalName(),
                        rootTypeElement.getQualifiedName().toString());
                return  true;
            }

            final var protoMessageQualifiedName = valueMaybe.get().toString();

            final var methodsIn = ElementFilter.methodsIn(rootTypeElement.getEnclosedElements());

            final var fromProtoMethods = methodsIn.stream()
                    .filter(method -> method.getSimpleName().contentEquals(fromProto))
                    .filter(fromProtoMethod -> fromProtoMethod.getReceiverType() == null)  // static method
                    .filter(fromProtoMethod -> {
                        final List<? extends VariableElement> parameters = fromProtoMethod.getParameters();
                        if (parameters.size() != 2) {
                            note(messager, rootTypeElement, "Parameter size:", parameters.size());
                            return false;
                        }
                        final TypeMirror typeMirror0 = parameters.get(0).asType();
                        if (typeMirror0.getKind() != TypeKind.DECLARED) {
                            note(messager, rootTypeElement, "type0 is not declared");
                            return false;
                        }
                        final TypeMirror typeMirror1 = parameters.get(1).asType();
                        if (typeMirror1.getKind() != TypeKind.DECLARED) {
                            note(messager, rootTypeElement, "type1 is not declared");
                            return false;
                        }

                        final var type1 = (TypeElement) ((DeclaredType) typeMirror1).asElement();
                        if (!type1.getQualifiedName().contentEquals(protoMessageQualifiedName)) {
                            note(messager, rootTypeElement, "wrong proto type, %s, %s",
                                    type1.getQualifiedName().toString(), protoMessageQualifiedName);
                            return false;
                        }

                        final var returnTypeMirror = fromProtoMethod.getReturnType();
                        if (returnTypeMirror.getKind() != TypeKind.DECLARED) {
                            note(messager, rootTypeElement, "return type is not declared");
                            return false;
                        }

                        final var returnType = (TypeElement) ((DeclaredType) returnTypeMirror).asElement();
                        if (!returnType.getQualifiedName().contentEquals(rootTypeElement.getQualifiedName())) {
                            note(messager, rootTypeElement, "wrong return type, %s, %s",
                                    type1.getQualifiedName().toString(), rootTypeElement.getQualifiedName().toString());
                            return false;
                        }

                        return true;
                    })
                    .collect(Collectors.toList());

            if (fromProtoMethods.isEmpty()) {
                error(messager,
                        rootTypeElement, "The class %s which is annotated with %s does not define a compatible public static fromProto method.",
                        rootTypeElement.getQualifiedName().toString(),
                        ProtoMessage.class.getCanonicalName());
            }

            if (fromProtoMethods.size() > 1) {
                error(messager,
                        rootTypeElement, "The class %s which is annotated with %s defines ambiguous compatible public static fromProto methods.",
                        rootTypeElement.getQualifiedName().toString(),
                        ProtoMessage.class.getCanonicalName());
            }
        }

        return true;
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
                String.format(msg, args),
                e);
    }

    private void note(final Messager messager,
                       final Element e,
                       final String msg,
                       final Object... args) {
        Objects.requireNonNull(messager).printMessage(Diagnostic.Kind.NOTE,
                String.format(msg, args),
                e);
    }
}
