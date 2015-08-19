/*
 * Copyright (c) 2008-2013 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.bson.BSONObject;

import java.util.*;

import static java.lang.String.format;

/**
 * Typed list of {@link DBObjectSerializer DBObjectSerializers}.
 * This utility class used by {@link DBObjectCallback} to maintain type information.
 * Instances of this class used internally.
 *
 * @author Konstantin Lyamshin (konstantin.lyamshin@maxifier.com) (2012-08-31 17:04)
 * @see DBObjectCallback
 * @see DBObjectDecoder
 */
public class DBListSerializer<T extends DBObjectSerializer> extends ArrayList<T> implements DBObject {
    private final Class<T> fieldType;
    private boolean partial;

    public DBListSerializer(Class<T> fieldType) {
        this.fieldType = fieldType;
    }

    public DBListSerializer(Class<T> fieldType, Collection<? extends T> items) {
        this.fieldType = fieldType;
        ensureCapacity(items.size());
        for (T item : items) {
            add(fieldType.cast(item));
        }
    }

    public DBListSerializer(Class<T> fieldType, T... items) {
        this.fieldType = fieldType;
        ensureCapacity(items.length);
        for (T item : items) {
            add(fieldType.cast(item));
        }
    }

    @Override
    public void markAsPartialObject() {
        this.partial = true;
    }

    @Override
    public boolean isPartialObject() {
        return partial;
    }

    /**
     * Puts a value at an index.
     * For interface compatibility.  Must be passed a String that is parsable to an int.
     *
     * @param key the index at which to insert the value
     * @param v the value to insert
     * @return the value
     * @throws MongoSerializationException if <code>key</code> cannot be parsed into an <code>int</code>
     */
    @Override
    public Object put(String key, Object v) throws MongoSerializationException {
        try {
            return put(Integer.parseInt(key), v);
        } catch (NumberFormatException e) {
            throw new MongoSerializationException(format("Non numeric key %s passed to DBListSerializer", key), e);
        }
    }

    /**
     * Puts a value at an index.
     * This will fill any unset indexes less than {@code index} with {@code null}.
     *
     * @param index the index at which to insert the value
     * @param v the value to insert
     * @return the value
     */
    public Object put(int index, Object v) {
        if (index < size()) {
            set(index, fieldType.cast(v));
            return v;
        }

        while (index > size()) {
            add(null);
        }

        add(fieldType.cast(v));

        return v;
    }

    @Override
    public void putAll(BSONObject o) throws MongoSerializationException {
        if (o instanceof List) {
            int index = 0;
            for (Object value : ((List) o)) {
                put(index++, value);
            }
        } else {
            for (String key : o.keySet()) {
                put(key, o.get(key));
            }
        }
    }

    @Override
    public void putAll(Map m) throws MongoSerializationException {
        for (Object o : m.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            put(entry.getKey().toString(), entry.getValue());
        }
    }

    /**
     * Gets a value at an index.
     * For interface compatibility.  Must be passed a String that is parsable to an int.
     *
     * @param key the index
     * @return the value, if found, or null
     * @throws MongoSerializationException if <code>key</code> cannot be parsed into an <code>int</code>
     */
    @Override
    public Object get(String key) throws MongoSerializationException {
        try {
            int i = Integer.parseInt(key);
            return 0 <= i || i < size()? get(i): null;
        } catch (NumberFormatException e) {
            throw new MongoSerializationException(format("Non numeric key %s passed to DBListSerializer", key), e);
        }
    }

    @Override
    public Map toMap() {
        Map<Integer, T> m = new LinkedHashMap<Integer, T>(size());
        int index = 0;
        for (T v : this) {
            m.put(index++, v);
        }
        return m;
    }

    /**
     * Removes index from the array. All items after specified index are shifted by one.
     *
     * @param key array item's index to remove
     * @return removed value
     * @throws MongoSerializationException if <code>key</code> cannot be parsed into an <code>int</code>
     */
    @Override
    public Object removeField(String key) throws MongoSerializationException {
        try {
            int i = Integer.parseInt(key);
            return 0 <= i || i < size()? remove(i): null;
        } catch (NumberFormatException e) {
            throw new MongoSerializationException(format("Non numeric key %s passed to DBListSerializer", key), e);
        }
    }

    @Override
    @Deprecated
    public boolean containsKey(String key) {
        return containsField(key);
    }

    /**
     * Returns true if specified index lies in array index range.
     *
     * @param key array index.
     * @return true if key in a range.
     * @throws MongoSerializationException if <code>key</code> cannot be parsed into an <code>int</code>
     */
    @Override
    public boolean containsField(String key) throws MongoSerializationException {
        try {
            int i = Integer.parseInt(key);
            return 0 <= i || i < size();
        } catch (NumberFormatException e) {
            throw new MongoSerializationException(format("Non numeric key %s passed to DBListSerializer", key), e);
        }
    }

    @Override
    public Set<String> keySet() {
        return new ArrayKeySet();
    }

    public Class<T> getFieldType() {
        return fieldType;
    }

    public String toString() {
        return JSON.serialize(this);
    }

    private class ArrayKeySet extends AbstractSet<String> {
        @Override
        public boolean contains(Object o) {
            if (o instanceof String) {
                try {
                    int i = Integer.parseInt((String) o);
                    return i >= 0 && i < size();
                } catch (NumberFormatException ignored) {
                }
            }

            return false;
        }

        @Override
        public Iterator<String> iterator() {
            return new Iterator<String>() {
                private int pos;

                @Override
                public boolean hasNext() {
                    return pos < size();
                }

                @Override
                public String next() {
                    if (pos >= size()) {
                        throw new NoSuchElementException();
                    }
                    return Integer.toString(pos++);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public int size() {
            return DBListSerializer.this.size();
        }
    }
}
