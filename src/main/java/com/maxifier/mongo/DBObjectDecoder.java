/*
 * Copyright (c) 2008-2013 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.mongodb.*;

/**
 * DBObjectDecoder
 * <p/>
 * This class extends {@link DefaultDBDecoder} to use {@link DBObjectCallback}.
 * Use {@link DBObjectDecoder#FACTORY} to configure {@link DBCollection}.
 * @see DBObjectCallback
 * @see DBCollection#setDBDecoderFactory(com.mongodb.DBDecoderFactory)
 *
 * @author Konstantin Lyamshin (konstantin.lyamshin@maxifier.com) (2012-08-31 17:18)
 */
public class DBObjectDecoder extends DefaultDBDecoder {
	public static final DBDecoderFactory FACTORY = new DBDecoderFactory() {
		public DBDecoder create() {
			return new DBObjectDecoder();
		}
	};

	@Override
	public DBCallback getDBCallback(DBCollection collection) {
		return new DBObjectCallback(collection);
	}
}
