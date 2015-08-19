/*
 * Copyright (c) 2008-2013 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.bson.BSONObject;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.*;

import static java.lang.String.format;

/**
 * DBObjectSerializer
 * <p/>
 * Base class for mongoDB transfer objects.
 * Class implements {@link DBObject} interface.
 * Class accessors delegates accessing and storing data to descendants.
 * Instance data accessed using reflection and JavaBeans notation.
 * <p/>
 * Instance serialization can be delegated to other object by
 * {@link #setDelegate(DBObject)} method. This is a way
 * to implement storage data versioning.
 * <p/>
 * Method {@link #configureCollection(com.mongodb.DBCollection, Class)}
 * enables mongoDB collection to return specific transfer objects.
 * <p/>
 * This class hides null fields. To represent null field value use the {@link #NULL} reference.
 * The order of descendant fields can be regulated by {@link DBObjectFieldOrder}.
 *
 * @author Konstantin Lyamshin (konstantin.lyamshin@maxifier.com) (2012-08-24 20:54)
 * @see DBObject
 * @see DBObjectFieldOrder
 */
public class DBObjectSerializer implements DBObject {
    /**
     * Special reference to represent null field value.
     */
    public static final Object NULL = new Object();
    protected final Map<String, DBObjectField> fields;
    protected DBObject delegate;
    protected Object _id;
    protected Integer version;
    private boolean partial;

    /**
     * Enables mongoDB collection to return typed objects from the collection.
     *
     * @param collection collection to initialize.
     * @param collectionType object's class that collection should hold.
     */
    public static DBCollection configureCollection(DBCollection collection, Class<? extends DBObjectSerializer> collectionType) {
        collection.setObjectClass(collectionType);
        collection.setDBDecoderFactory(DBObjectDecoder.FACTORY);
        return collection;
    }

    protected static <T extends Enum<T>> String encodeEnum(T value) {
        return value != null? value.name(): null;
    }

    protected static <T extends Enum<T>> List<String> encodeEnum(Collection<T> value) {
        if (value == null) {
            return null;
        }

        ArrayList<String> result = new ArrayList<String>(value.size());
        for (T v : value) {
            result.add(v.name());
        }

        return result;
    }

    protected static <T extends Enum<T>> T decodeEnum(Class<T> enumType, String value) {
        return value != null? Enum.valueOf(enumType, value): null;
    }

    protected static <T extends Enum<T>> EnumSet<T> decodeEnum(Class<T> enumType, Collection<String> value) {
        if (value == null) {
            return null;
        }

        EnumSet<T> result = EnumSet.noneOf(enumType);
        for (String v : value) {
            result.add(Enum.valueOf(enumType, v));
        }

        return result;
    }

    /**
     * Introspects class and builds internal list of DBObject fields.
     *
     * @throws MongoSerializationException can not introspects this object
     */
    protected DBObjectSerializer() {
        this.fields = DBObjectField.getFieldDescriptors(this.getClass());
    }

    /**
     * Initializes delegate.
     *
     * @param that source of fields values.
     * @return delegate.
     */
    protected DBObject init(DBObjectSerializer that) {
        this._id = that._id;
        this.version = that.version;
        return this;
    }

    /**
     * Returns object that receives all {@link DBObject} calls instead of this object.
     *
     * @return current delegate or null if delegation off.
     */
    public DBObject getDelegate() {
        return delegate;
    }

    /**
     * Delegates storing of DBObject fields to other object.
     * After this method call all DBObject activity will be delegated to specified object.
     *
     * @param d delegate.
     */
    protected void setDelegate(DBObject d) {
        if (delegate != null) {
            throw new IllegalStateException(format("DBObject serialization can not be delegated twice (trying to replace %s by %s)", delegate, d));
        }

        if (partial) {
            d.markAsPartialObject();
        }

        this.delegate = d instanceof DBObjectSerializer? ((DBObjectSerializer) d).init(this): d;
    }

    @Override
    public void markAsPartialObject() {
        this.partial = true;
    }

    @Override
    public boolean isPartialObject() {
        return partial;
    }

    @Override
    public Object get(String key) {
        if (delegate != null) {
            return delegate.get(key);
        }

        DBObjectField field = fields.get(key);
        if (field == null) {
            return null;
        }

        Object value = field.get(this);
        return value != NULL? value: null;
    }

    @Override
    @Deprecated
    public boolean containsKey(String key) {
        return containsField(key);
    }

    @Override
    public boolean containsField(String key) {
        if (delegate != null) {
            return delegate.containsField(key);
        }

        DBObjectField field = fields.get(key);
        return field != null && field.get(this) != null;
    }

    @Override
    public Set<String> keySet() {
        if (delegate != null) {
            return delegate.keySet();
        }

        LinkedHashSet<String> keys = new LinkedHashSet<String>(fields.keySet());
        for (Iterator<String> it = keys.iterator(); it.hasNext(); ) {
            DBObjectField field = fields.get(it.next());
            if (field == null || field.get(this) == null) { // skip null (default) values
                it.remove();
            }
        }

        return keys;
    }

    @Override
    public Map<String, Object> toMap() {
        if (delegate != null) {
            //noinspection unchecked
            return delegate.toMap();
        }

        BasicDBObject m = new BasicDBObject(fields);
        for (Iterator<Map.Entry<String, Object>> it = m.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Object> entry = it.next();
            Object v = ((DBObjectField) entry.getValue()).get(this);
            if (v == null) { // skip null (default) values
                it.remove();
            } else {
                entry.setValue(v != NULL? v: null);
            }
        }

        return m;
    }

    @Override
    public Object put(String key, Object v) throws MongoSerializationException {
        if (delegate != null) {
            return delegate.put(key, v);
        }

        DBObjectField field = fields.get(key);
        if (field == null) {
            throw new MongoSerializationException(format("Instance of class %s have no property '%s'", this.getClass().getName(), key));
        }

        field.set(this, v);

        return v;
    }

    @Override
    public void putAll(BSONObject object) throws MongoSerializationException {
        if (delegate != null) {
            delegate.putAll(object);
            return;
        }

        if (object instanceof DBObjectSerializer) {
            putAll(object.toMap());
        } else {
            for (String key : object.keySet()) {
                put(key, object.get(key));
            }
        }
    }

    @Override
    public void putAll(Map m) throws MongoSerializationException {
        if (delegate != null) {
            delegate.putAll(m);
            return;
        }

        for (Object o : m.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            put(entry.getKey().toString(), entry.getValue());
        }
    }

    @Override
    public Object removeField(String key) {
        if (delegate != null) {
            return delegate.removeField(key);
        }

        DBObjectField field = fields.get(key);
        if (field == null) {
            return null;
        }

        return field.set(this, null);
    }

    /**
     * @return JSON representation of current object.
     */
    public String toString() {
        return JSON.serialize(this);
    }

    public Type getFieldType(String key) {
        DBObjectField field = fields.get(key);
        return field != null? field.getType(): null;
    }

    /**
     * DBObjectField
     * <p/>
     * Internal class that holds DBObject field information.
     * Uses introspection to build list of fields.
     *
     * @author Konstantin Lyamshin (konstantin.lyamshin@maxifier.com) (2012-08-31 17:31)
     */
    static final class DBObjectField {
        private static final Map<Class<?>, Map<String, DBObjectField>> fieldsCache = Collections.synchronizedMap(
            new WeakHashMap<Class<?>, Map<String, DBObjectField>>()
        );
        private final PropertyDescriptor descriptor;

        static Map<String, DBObjectField> getFieldDescriptors(Class<?> clazz) throws MongoSerializationException{
            Map<String, DBObjectField> fields = fieldsCache.get(clazz);
            if (fields != null) {
                return fields;
            }

            DBObjectFieldOrder fieldOrder = clazz.getAnnotation(DBObjectFieldOrder.class);
            PropertyDescriptor[] propertyDescriptors;
            try {
                propertyDescriptors = Introspector.getBeanInfo(clazz).getPropertyDescriptors().clone();
            } catch (IntrospectionException e) {
                throw new MongoSerializationException("Can not read class info of " + clazz, e);
            }

            Arrays.sort(propertyDescriptors, new FieldComparator(null, fieldOrder != null? fieldOrder.value(): null));

            fields = new LinkedHashMap<String, DBObjectField>(propertyDescriptors.length);
            for (PropertyDescriptor descriptor : propertyDescriptors) {
                if (descriptor.getReadMethod() != null && descriptor.getWriteMethod() != null) {
                    if (fields.put(descriptor.getName(), new DBObjectField(descriptor)) != null) {
                        throw new MongoSerializationException(format("Duplicate property %s in class %s", descriptor.getName(), clazz.getName()));
                    }
                }
            }

            fieldsCache.put(clazz, fields);

            return fields;
        }

        private DBObjectField(PropertyDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        Object get(DBObjectSerializer o) throws MongoSerializationException {
            try {
                return descriptor.getReadMethod().invoke(o);
            } catch (IllegalAccessException e) {
                throw new MongoSerializationException(format("Can not read property '%s' of class '%s'", descriptor.getName(), o.getClass().getName()), e);
            } catch (InvocationTargetException e) {
                throw new MongoSerializationException(format("Can not read property '%s' of class '%s'", descriptor.getName(), o.getClass().getName()), e);
            } catch (IllegalArgumentException e) {
                throw new MongoSerializationException(format("Can not read property '%s' of class '%s'", descriptor.getName(), o.getClass().getName()), e);
            }
        }

        Object set(DBObjectSerializer o, Object v) throws MongoSerializationException {
            try {
                return descriptor.getWriteMethod().invoke(o, v);
            } catch (IllegalAccessException e) {
                throw new MongoSerializationException(format("Can not write property '%s' of class '%s'", descriptor.getName(), o.getClass().getName()), e);
            } catch (InvocationTargetException e) {
                throw new MongoSerializationException(format("Can not write property '%s' of class '%s'", descriptor.getName(), o.getClass().getName()), e);
            } catch (IllegalArgumentException e) {
                throw new MongoSerializationException(format("Can not write property '%s' of class '%s'", descriptor.getName(), o.getClass().getName()), e);
            }
        }

        Type getType() {
            return descriptor.getReadMethod().getGenericReturnType();
        }

        /**
         * Internal class used for sorting DBObject fields.
         * Order of fields can be specified by {@link DBObjectFieldOrder} annotation.
         * Field order is [_id, id, version, specifiedOrder, otherFields].
         *
         * @see DBObjectFieldOrder
         */
        private static class FieldComparator implements Comparator<PropertyDescriptor> {
            private final Comparator<String> comparator;
            private final TObjectIntHashMap<String> order = new TObjectIntHashMap<String>();

            FieldComparator(Comparator<String> comparator, String[] fieldOrder) {
                this.comparator = comparator;
                this.order.put("_id", -3);
                this.order.put("id", -2);
                this.order.put("version", -1);
                if (fieldOrder != null) {
                    for (int i = 0; i < fieldOrder.length; i++) {
                        this.order.put(fieldOrder[i], i + 1);
                    }
                }
            }

            public int compare(PropertyDescriptor o1, PropertyDescriptor o2) {
                int n1 = order.get(o1.getName());
                int n2 = order.get(o2.getName());
                if (n1 != 0 && n2 != 0) {
                    return n1 - n2;
                }
                if (n1 != 0) {
                    return -1;
                }
                if (n2 != 0) {
                    return 1;
                }
                if (comparator != null) {
                    return comparator.compare(o1.getName(), o2.getName());
                }

                return 0;
            }
        }
    }
}
