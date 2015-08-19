/*
 * Copyright (c) 2008-2013 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * DBObjectFieldOrder
 * <p/>
 * This annotation specifies order of {@link DBObjectSerializer}'s fields.
 * Fields listed in {@link #value()} parameter appears in {@link DBObjectSerializer#keySet()} in the same order.
 * Fields <em>_id, id, version</em> appears before others. Unlisted fields appears after listed ones.
 *
 * @author Konstantin Lyamshin (konstantin.lyamshin@maxifier.com) (2012-08-28 13:23)
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DBObjectFieldOrder {
	String[] value();
}
