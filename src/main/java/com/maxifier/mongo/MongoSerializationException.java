/*
 * Copyright (c) 2008-2013 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.mongodb.MongoInternalException;

/**
 * MongoSerializationException
 * <p/>
 * Exception that indicates serialization errors.
 *
 * @author Konstantin Lyamshin (konstantin.lyamshin@maxifier.com) (2012-09-06 11:45)
 */
public class MongoSerializationException extends MongoInternalException {
	public MongoSerializationException(String msg) {
		super(msg);
	}
	public MongoSerializationException(String msg, Throwable t) {
		super(msg, t);
	}
}
