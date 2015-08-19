/*
 * Copyright (c) 2008-2015 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * @author Konstantin Lyamshin (2015-02-03 20:51)
 */
@JsonAdapter(GsonDBObject.AdapterFactory.class)
public class GsonDBObject implements DBObject {
    private static final Logger logger = LoggerFactory.getLogger(GsonDBObject.class);
    private static final Map<Class<?>, Map<String, Field>> fieldsCache = Collections.synchronizedMap(
        new WeakHashMap<Class<?>, Map<String, Field>>()
    );
    private final Map<String, Field> fields;
    private boolean partial;

    protected GsonDBObject() {
        this.fields = collectFields(getClass());
    }

    @Override
    public final boolean isPartialObject() {
        return partial;
    }

    @Override
    public final void markAsPartialObject() {
        this.partial = true;
    }

    @Override
    public final Set<String> keySet() {
        return fields.keySet();
    }

    @Override
    public final Object get(String key) {
        Field field = fields.get(key);
        try {
            return field != null? field.get(this): null;
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Can't access field " + key, e);
        }
    }

    @Override
    public final boolean containsKey(String key) {
        return containsField(key);
    }

    @Override
    public final boolean containsField(String key) {
        return fields.containsKey(key);
    }

    @Override
    public final Object put(String key, Object v) {
        Field field = fields.get(key);
        if (field == null) {
            throw new IllegalArgumentException("Class " + getClass().getName() + " have no property " + key);
        }

        try {
            field.set(this, v);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Can't access field " + key, e);
        }

        return null;
    }

    @Override
    public final Object removeField(String key) {
        Field field = fields.get(key);
        if (field != null) {
            try {
                field.set(this, null);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Can't access field " + key, e);
            }
        }
        return null;
    }

    @Override
    public final Map toMap() {
        BasicDBObject m = new BasicDBObject(fields.size());
        for (Map.Entry<String, Field> entry : fields.entrySet()) {
            try {
                Object value = entry.getValue().get(this);
                if (value != null) { // skip nulls
                    m.append(entry.getKey(), value);
                }
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Can't access field " + entry.getKey(), e);
            }
        }
        return m;
    }

    @Override
    public final void putAll(BSONObject object) {
        for (String key : object.keySet()) {
            put(key, object.get(key));
        }
    }

    @Override
    public final void putAll(Map m) {
        for (Object o : m.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            put(entry.getKey().toString(), entry.getValue());
        }
    }

    /**
     * @return JSON representation of current object.
     */
    public String toString() {
        return new Gson().toJson(this);
    }

    private static Map<String, Field> collectFields(Class<?> clazz) {
        Map<String, Field> cached = fieldsCache.get(clazz);
        if (cached == null) {
            // Inspect class hierarchy recursively
            ArrayList<Map.Entry<String, Field>> collector = new ArrayList<Map.Entry<String, Field>>();
            collectFields(clazz, collector);

            if (collector.isEmpty()) {
                throw new IllegalArgumentException("No serializable fields found for class " + clazz.getName());
            }
            if (collector.get(0).getKey().equals("_id")) {
                throw new IllegalArgumentException("No '_id' field defined for class " + clazz.getName() + ". Use @Id or @SerializedName.");
            }

            LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>(collector.size());
            for (Map.Entry<String, Field> entry : collector) {
                Field field = fields.put(entry.getKey(), entry.getValue());
                if (field != null) {
                    throw new IllegalArgumentException("Class " + clazz.getName() + " declares multiple fields named " + entry.getKey());
                }
            }

            fieldsCache.put(clazz, cached = Collections.unmodifiableMap(fields));
        }
        return cached;
    }

    private static void collectFields(Class<?> clazz, ArrayList<Map.Entry<String, Field>> collector) {
        if (clazz == GsonDBObject.class) {
            return; // Stop on GsonDBObject
        }

        int segment = collector.size();
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);

            // Skip transient and static fields
            if (Modifier.isTransient(field.getModifiers()) || Modifier.isStatic(field.getModifiers())) {
                continue;
            }

            SerializedName serializedName = field.getAnnotation(SerializedName.class);
            String fieldName = field.isAnnotationPresent(Id.class)? "_id":
                serializedName != null? serializedName.value(): field.getName();

            collector.add(new AbstractMap.SimpleEntry<String, Field>(fieldName, field));
        }

        // Append super object's fields
        collectFields(clazz.getSuperclass(), collector);

        // Sort current segment
        DBObjectFieldOrder fieldOrder = clazz.getAnnotation(DBObjectFieldOrder.class);
        Collections.sort(collector.subList(segment, collector.size()), new FieldComparator(fieldOrder));
    }

    /**
     * Internal comparator used for sorting GsonDBObject fields.
     * Order of fields can be specified by {@link DBObjectFieldOrder} annotation.
     * Field order is [_id, specifiedOrder, otherFields].
     */
    private static class FieldComparator implements Comparator<Map.Entry<String, Field>> {
        private final HashMap<String, Integer> order = new HashMap<String, Integer>();

        private FieldComparator(DBObjectFieldOrder fieldOrder) {
            if (fieldOrder != null) {
                String[] order = fieldOrder.value();
                for (int i = 0; i < order.length; i++) {
                    this.order.put(order[i], i);
                }
            }
            this.order.put("_id", -1);
        }

        @Override
        public int compare(Map.Entry<String, Field> o1, Map.Entry<String, Field> o2) {
            Integer n1 = order.get(o1.getKey());
            Integer n2 = order.get(o2.getKey());
            if (n1 == null && n2 == null) {
                return 0; // Unspecified fields is unordered
            }
            if (n2 == null) {
                return 1; // Ordered before unordered
            }
            if (n1 == null) {
                return -1; // Unordered after ordered
            }
            return n1 - n2;
        }
    }

    public static class AdapterFactory implements TypeAdapterFactory{
        @Override
        @SuppressWarnings("unchecked")
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            if (GsonDBObject.class.isAssignableFrom(type.getRawType())) {
                return (TypeAdapter<T>) new Adapter(gson, type.getRawType());
            }
            return null;
        }
    }

    public static class Adapter extends TypeAdapter<GsonDBObject> {
        private final Class<?> clazz;
        private final LinkedHashMap<String, TypeAdapter<Object>> adapters;

        public Adapter(Gson gson, Class<?> clazz) {
            Map<String, Field> fields = collectFields(clazz);
            this.clazz = clazz;
            this.adapters = new LinkedHashMap<String, TypeAdapter<Object>>(fields.size());
            for (Map.Entry<String, Field> entry : fields.entrySet()) {
                @SuppressWarnings("unchecked") TypeToken<Object> fieldType = (TypeToken<Object>) TypeToken.get(entry.getValue().getGenericType());
                this.adapters.put(entry.getKey(), gson.getAdapter(fieldType));
            }
        }

        @Override
        public void write(JsonWriter out, GsonDBObject value) throws IOException {
            if (value == null) {
                out.nullValue();
                return;
            }

            out.beginObject();
            for (Map.Entry<String, TypeAdapter<Object>> entry : adapters.entrySet()) {
                String key = entry.getKey();
                entry.getValue().write(out.name(key), value.get(key));
            }
            out.endObject();
        }

        @Override
        public GsonDBObject read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }

            GsonDBObject object;
            try {
                object = (GsonDBObject) clazz.newInstance();
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Can not create instance of " + clazz.getName(), e);
            } catch (InstantiationException e) {
                throw new IllegalStateException("Can not create instance of " + clazz.getName(), e.getCause());
            }

            in.beginObject();
            while (in.hasNext()) {
                String name = in.nextName();
                TypeAdapter<Object> adapter = adapters.get(name);
                if (adapter != null) {
                    Object value = adapter.read(in);
                    object.put(name, value);
                } else {
                    logger.error("Unexpected JSON field {} for class {}", name, clazz.getName());
                }
            }
            in.endObject();

            return object;
        }
    }
}
