/*
 * Copyright (c) 2008-2013 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.mongodb.*;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

/**
 * DBObjectCallback
 * <p/>
 * This class implements type safe BSON decoding.
 * It use type information from {@link DBObjectSerializer} instances
 * to create new instances of serializers.
 * {@link DBCollection} should be initialized to use this callback:
 * <code>
 *   dbCollection.setObjectClass(collectionType);
 *   dbCollection.setDBDecoderFactory(DBObjectDecoder.FACTORY);
 * </code>
 * @see DBObjectSerializer#configureCollection(com.mongodb.DBCollection, Class)
 * @see DBObjectDecoder
 *
 * @author Konstantin Lyamshin (konstantin.lyamshin@maxifier.com) (2012-08-31 17:18)
 */
public class DBObjectCallback extends BasicBSONCallback implements DBCallback {
	private final DBCollection collection;

	public DBObjectCallback(DBCollection collection) {
		this.collection = collection;
	}

	/**
	 * Creates new BSON object.
	 *
	 * @return new object.
	 * @throws MongoSerializationException instantiation of new object impossible
	 */
	@Override
	public BSONObject create() throws MongoSerializationException {
		if (isStackEmpty()) {
			return collection.getObjectClass() != null? newInstance(collection.getObjectClass()): new BasicDBObject();
		}

		BSONObject cur = cur();
		if (cur instanceof DBObjectSerializer) {
			Type type = ((DBObjectSerializer) cur).getFieldType(curName());
			if (type instanceof Class) {
				Class<?> fieldClass = (Class<?>) type;
				if (DBObjectSerializer.class.isAssignableFrom(fieldClass)) {
					return newInstance(fieldClass);
				}
			}
		} else if (cur instanceof DBListSerializer) {
			Class<?> type = ((DBListSerializer) cur).getFieldType();
			if (type != null && DBObjectSerializer.class.isAssignableFrom(type)) {
                return newInstance(type);
			}
		}

		return new BasicDBObject();
	}

	private BSONObject newInstance(Class<?> clazz) throws MongoSerializationException{
		try {
			return (DBObject) clazz.newInstance();
		} catch (InstantiationException e) {
			throw new MongoSerializationException("Can't instantiate a " + clazz.getName(), e);
		} catch (IllegalAccessException e) {
			throw new MongoSerializationException("Can't instantiate a " + clazz.getName(), e);
		} catch (ClassCastException e) {
			throw new MongoSerializationException("Can't instantiate a " + clazz.getName(), e);
		}
	}

	/**
	 * Instantiates new BSON list.
	 *
	 * @return new list.
	 */
	@Override
	protected BSONObject createList() {
		if (isStackEmpty()) {
			return new BasicDBList();
		}

		BSONObject cur = cur();
		if (cur instanceof DBObjectSerializer) {
			Type type = ((DBObjectSerializer) cur).getFieldType(curName());
			if (type instanceof ParameterizedType) {
				ParameterizedType fieldType = (ParameterizedType) type;
				if (fieldType.getRawType() instanceof Class<?> && fieldType.getActualTypeArguments().length == 1) {
					Class<?> rawType = (Class<?>) fieldType.getRawType();
					if (List.class.isAssignableFrom(rawType)) {
						Type argument = fieldType.getActualTypeArguments()[0];
						if (argument instanceof Class<?>) {
							Class<?> elementClass = (Class<?>) argument;
							if (DBObjectSerializer.class.isAssignableFrom(elementClass)) {
								return createList(elementClass.asSubclass(DBObjectSerializer.class));
							}
						}
					}
				}
			}
		}

		return new BasicDBList();
	}

	private <T extends DBObjectSerializer> DBListSerializer createList(Class<T> elementClass) {
		return new DBListSerializer<T>(elementClass);
	}
}
