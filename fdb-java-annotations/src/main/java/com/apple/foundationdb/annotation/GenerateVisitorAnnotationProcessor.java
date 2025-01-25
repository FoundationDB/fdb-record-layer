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

import javax.annotation.Nonnull;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import java.util.Set;

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
public class GenerateVisitorAnnotationProcessor extends AbstractProcessor {
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
        return GenerateVisitorAnnotationHelper.process(processingEnv, annotations, roundEnv);
    }
}
