/*
 * Copyright (c) 2008-2015 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.bson.BSONObject;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper of POJO objects to pass to {@link DBCollection} methods.
 * <p>Instances of {@code GsonWrapper} detected by collections configured by {@link GsonDBCollection}.</p>
 * <p>Usage example 1 (external POJO):<pre>
 *     class Pojo {
 *         String field;
 *     }
 *
 *     DBCollection collection = GsonDBCollection.configure(c, gson, Pojo.class);
 *
 *     Pojo p1 = new Pojo();
 *     collection.insert(new GsonWrapper(p1));
 *
 *     Pojo p2 = ((GsonWrapper) collection.findOne()).getPojo();
 * </pre></p>
 * <p>Usage example 2 (self-contained POJO):<pre>
 *     class Pojo extends GsonWrapper {
 *         String field;
 *
 *         Pojo() {
 *             super(this);
 *         }
 *     }
 *
 *     DBCollection collection = GsonDBCollection.configure(c, gson, Pojo.class);
 *
 *     Pojo p1 = new Pojo();
 *     collection.insert(p1);
 *
 *     Pojo p2 = (Pojo) collection.findOne();
 * </pre></p>
 *
 * @author Konstantin Lyamshin (2014-12-29 15:45)
 */
public class GsonWrapper implements DBObject {
    // Ugly hack for DBCollection implementation which requires presence of _id field
    private static final Map<String, Object> FAKEID = Collections.singletonMap("_id", new Object());
    private transient final Object pojo;
    private transient boolean partial;

    public static GsonWrapper of(Object pojo) {
        return pojo instanceof GsonWrapper? (GsonWrapper) pojo: new GsonWrapper(pojo);
    }

    public GsonWrapper(Object pojo) {
        this.pojo = pojo;
    }

    protected GsonWrapper() {
        this.pojo = this;
    }

    public Object getPojo() {
        return pojo;
    }

    @Override
    public void markAsPartialObject() {
        partial = true;
    }

    @Override
    public boolean isPartialObject() {
        return partial;
    }

    @Override
    public Object put(String key, Object v) {
        throw new UnsupportedOperationException("Can't change wrapper object");
    }

    @Override
    public void putAll(BSONObject o) {
        throw new UnsupportedOperationException("Can't change wrapper object");
    }

    @Override
    public void putAll(Map m) {
        throw new UnsupportedOperationException("Can't change wrapper object");
    }

    @Override
    public Object get(String key) {
        return FAKEID.get(key);
    }

    @Override
    public Map toMap() {
        return FAKEID;
    }

    @Override
    public Object removeField(String key) {
        throw new UnsupportedOperationException("Can't change wrapper object");
    }

    @Override
    public boolean containsKey(String s) {
        return containsField(s);
    }

    @Override
    public boolean containsField(String s) {
        return FAKEID.containsKey(s);
    }

    @Override
    public Set<String> keySet() {
        return FAKEID.keySet();
    }
}
