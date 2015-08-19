/*
 * Copyright (c) 2008-2015 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.google.inject.BindingAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;

/**
 * Marker annotation which separates Guice bindings meant for Bson serealization framework.
 *
 * @author Konstantin Lyamshin (2015-01-07 12:08)
 */
@BindingAnnotation
@Target({ FIELD, PARAMETER, METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface BsonBridge {
}
