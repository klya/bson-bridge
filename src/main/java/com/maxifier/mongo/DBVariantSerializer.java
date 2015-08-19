/*
 * Copyright (c) 2008-2013 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * DBVariantSerializer
 * <p/>
 * This class extends functionality of {@link DBObjectSerializer}
 * by holding open list of fields. Fields that not supported by
 * <em>DBObjectSerializer</em> contains in internal map.
 *
 * @author Konstantin Lyamshin (konstantin.lyamshin@maxifier.com) (2012-08-31 17:34)
 */
public class DBVariantSerializer extends DBObjectSerializer {
    private final Map<String, Object> values = new LinkedHashMap<String, Object>();

    @Override
    public Object get(String key) {
        if (delegate != null) {
            return delegate.get(key);
        }

        DBObjectField field = fields.get(key);
        if (field == null) {
            return values.get(key);
        }

        Object v = field.get(this);
        return v != NULL? v: null;
    }

    @Override
    public boolean containsField(String key) {
        if (delegate != null) {
            return delegate.containsField(key);
        }

        DBObjectField field = fields.get(key);
        return field != null? field.get(this) != null: values.containsKey(key);
    }

    @Override
    public Set<String> keySet() {
        if (delegate != null) {
            return delegate.keySet();
        }

        Set<String> keys = super.keySet();
        keys.addAll(values.keySet());
        return keys;
    }

    @Override
    public Map<String, Object> toMap() {
        if (delegate != null) {
            //noinspection unchecked
            return delegate.toMap();
        }

        Map<String, Object> map = super.toMap();
        map.putAll(values);
        return map;
    }

    @Override
    public Object put(String key, Object v) {
        if (delegate != null) {
            return delegate.put(key, v);
        }

        DBObjectField field = fields.get(key);
        if (field == null) {
            values.put(key, v);
        } else {
            field.set(this, v);
        }

        return v;
    }

    @Override
    public Object removeField(String key) {
        if (delegate != null) {
            return delegate.removeField(key);
        }

        DBObjectField field = fields.get(key);
        return field != null? field.set(this, null): values.remove(key);
    }
}