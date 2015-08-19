/*
 * Copyright (c) 2008-2013 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.BSONObject;
import org.bson.types.ObjectId;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;

/**
 * ValueDBObject
 * <p/>
 * Implements simple {@link DBObject} with one key-&gt;value pair.
 *
 * @author Konstantin Lyamshin (konstantin.lyamshin@maxifier.com) (2012-11-15 11:41)
 */
public class ValueDBObject implements DBObject {
    private static final Set<String> _ID = singleton("_id");
    private static final Set<String> $AVG = singleton("$avg");
    private static final Set<String> $INC = singleton("$inc");
    private static final Set<String> $SET = singleton("$set");
    private static final Set<String> $UNSET = singleton("$unset");
    private static final Set<String> $IN = singleton("$in");
    private static final Set<String> $GT = singleton("$gt");
    private static final Set<String> $GTE = singleton("$gte");
    private static final Set<String> $LT = singleton("$lt");
    private static final Set<String> $LTE = singleton("$lte");
    private static final Set<String> $NE = singleton("$ne");
    private static final Set<String> $EXISTS = singleton("$exists");
    private static final Set<String> $PULL = singleton("$pull");
    private static final Set<String> $PUSH = singleton("$push");
    private static final Set<String> $EACH = singleton("$each");
    private static final Set<String> $GROUP = singleton("$group");
    private static final Set<String> $MATCH = singleton("$match");
    private static final Set<String> $ELEM_MATCH = singleton("$elemMatch");

    private Set<String> keyset;
    private Object value;
    private boolean partial;

    /*
        Common mongoDB operations.
    */

    public static ValueDBObject v(String field, Object value) {
        return new ValueDBObject(field, value);
    }

    public static ValueDBObject elemMatch(String field, Object value) {
        return new ValueDBObject($ELEM_MATCH, v(field, value));
    }

    public static ValueDBObject id(ObjectId value) {
        return new ValueDBObject(_ID, value);
    }

    public static ValueDBObject in(Object... values) {
        return new ValueDBObject($IN, asList(values));
    }

    public static ValueDBObject in(Collection<?> values) {
        return new ValueDBObject($IN, values);
    }

    public static ValueDBObject avg(Object value) {
        return new ValueDBObject($AVG, value);
    }

    public static ValueDBObject group(Object value) {
        return new ValueDBObject($GROUP, value);
    }

    public static ValueDBObject match(Object value) {
        return new ValueDBObject($MATCH, value);
    }

    public static ValueDBObject gt(Object value) {
        return new ValueDBObject($GT, value);
    }

    public static ValueDBObject gte(Object value) {
        return new ValueDBObject($GTE, value);
    }

    public static ValueDBObject lt(Object value) {
        return new ValueDBObject($LT, value);
    }

    public static ValueDBObject lte(Object value) {
        return new ValueDBObject($LTE, value);
    }

    public static ValueDBObject ne(Object value) {
        return new ValueDBObject($NE, value);
    }

    public static ValueDBObject exists(boolean value) {
        return new ValueDBObject($EXISTS, value);
    }

    public static ValueDBObject inc(String field, Object amount) {
        return new ValueDBObject($INC, new ValueDBObject(field, amount));
    }

    public static ValueDBObject inc(DBObject fields) {
        return new ValueDBObject($INC, fields);
    }

    public static ValueDBObject set(String field, Object value) {
        return new ValueDBObject($SET, new ValueDBObject(field, value));
    }

    public static ValueDBObject set(DBObject fields) {
        return new ValueDBObject($SET, fields);
    }

    public static ValueDBObject unset(String... fields) {
        BasicDBObject q = new BasicDBObject();
        for (String field : fields) {
            q.append(field, "");
        }

        return new ValueDBObject($UNSET, q);
    }

    public static ValueDBObject unset(DBObject fields) {
        return new ValueDBObject($UNSET, fields);
    }

    public static ValueDBObject pull(String field, Object value) {
        return new ValueDBObject($PULL, new ValueDBObject(field, value));
    }

    public static ValueDBObject pull(DBObject fields) {
        return new ValueDBObject($PULL, fields);
    }

    public static ValueDBObject push(String field, Object value) {
        return new ValueDBObject($PUSH, v(field, value));
    }

    public static ValueDBObject push(String field, Object... values) {
        return new ValueDBObject($PUSH, v(field, each(values)));
    }

    public static ValueDBObject each(Object... values) {
        return new ValueDBObject($EACH, values);
    }

    /*
        Regular DBObject methods
    */

    public ValueDBObject(String key, Object value) {
        this.keyset = singleton(key);
        this.value = value;
    }

    private ValueDBObject(Set<String> keyset, Object value) {
        this.keyset = keyset;
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
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
    public Object put(String key, Object v) {
        if (!keyset.contains(key)) {
            throw new IllegalArgumentException("ValueDBObject doesn't support field " + key);
        }

        value = v;

        return v;
    }

    @Override
    public void putAll(BSONObject o) {
        for (String key : o.keySet()) {
            put(key, o.get(key));
        }
    }

    @Override
    public void putAll(Map m) {
        for (Object o : m.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            put((String) entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Object get(String key) {
        return keyset.contains(key)? value: null;
    }

    @Override
    public Map toMap() {
        return Collections.singletonMap(keyset.iterator().next(), value);
    }

    @Override
    public Object removeField(String key) {
        throw new UnsupportedOperationException("ValueDBObject doesn't supports removing keys");
    }

    @Override
    public boolean containsKey(String key) {
        return keyset.contains(key);
    }

    @Override
    public boolean containsField(String key) {
        return keyset.contains(key);
    }

    @Override
    public Set<String> keySet() {
        return keyset;
    }

    public BasicDBObject append(String key, Object value) {
        return new BasicDBObject(keyset.iterator().next(), this.value).append(key, value);
    }

    public String toString() {
        return String.format("{%s: %s}", keyset.iterator().next(), value);
    }
}
