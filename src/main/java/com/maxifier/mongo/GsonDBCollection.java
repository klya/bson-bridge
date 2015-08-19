/*
 * Copyright (c) 2008-2014 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.mongodb.*;
import org.bson.BSONCallback;
import org.bson.BSONObject;
import org.bson.io.OutputBuffer;
import org.bson.types.Binary;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * {@link DBCollection} wrapper which works with <em>Gson</em> configured POJOs.
 *
 * @author Konstantin Lyamshin (2014-12-29 15:45)
 */
public class GsonDBCollection<T> {
    private final DBCollection dbc;
    private final Class<T> clazz;
    private final Gson gson;

    public GsonDBCollection(DBCollection dbc, Class<T> rootClass, Gson gson) {
        this.dbc = dbc;
        this.clazz = rootClass;
        this.gson = gson;
        dbc.setDBEncoderFactory(new DBEncoderFactory() {
            @Override
            public DBEncoder create() {
                return new GsonDBEncoder();
            }
        });
        dbc.setDBDecoderFactory(new DBDecoderFactory() {
            @Override
            public DBDecoder create() {
                return new GsonDBDecoder();
            }
        });
        dbc.setObjectClass(null);
    }

    public DBCollection getDBCollection() {
        return dbc;
    }

    // ---- Simple update ----------------------------------------------------------------------------------------------

    public WriteResult insert(T doc) {
        return dbc.insert(GsonWrapper.of(doc));
    }

    public WriteResult updateAll(DBObject query, DBObject update) {
        return dbc.update(query, update, false, true);
    }

    public WriteResult updateOne(DBObject query, DBObject update) {
        return dbc.update(query, update, false, false);
    }

    public WriteResult upsertOne(DBObject query, DBObject upsert) {
        return dbc.update(query, upsert, true, false);
    }

    public WriteResult removeAll(DBObject query) {
        return dbc.remove(query);
    }

    // ---- Query ------------------------------------------------------------------------------------------------------

    public T findOne(DBObject query) {
        GsonWrapper wrapper = (GsonWrapper) dbc.findOne(query);
        return wrapper != null? clazz.cast(wrapper.getPojo()): null;
    }

    public QueryBuilder find(DBObject query) {
        return new QueryBuilder(query);
    }

    public class QueryBuilder implements Iterable<T> {
        private final DBObject query;
        private DBObject fields;
        private DBObject sort;
        private QueryParams params;

        private QueryBuilder(DBObject query) {
            this.query = query;
        }

        public QueryBuilder fields(DBObject fields) {
            this.fields = fields;
            return this;
        }

        public QueryBuilder sort(DBObject sort){
            this.sort = sort;
            return this;
        }

        private QueryParams params() {
            return params == null? params = new QueryParams(): params;
        }

        public QueryBuilder skip(int skip) {
            params().skip = skip;
            return this;
        }

        public QueryBuilder limit(int limit) {
            params().limit = limit;
            return this;
        }

        public QueryBuilder batchSize(int batchSize) {
            params().batchSize = batchSize;
            return this;
        }

        public QueryBuilder timeout(long maxTime, TimeUnit timeUnit) {
            params().timeout = timeUnit.toMillis(maxTime);
            return this;
        }

        public QueryBuilder hint(DBObject indexKeys) {
            params().hintO = indexKeys;
            return this;
        }

        public QueryBuilder hint(String indexName) {
            params().hintS = indexName;
            return this;
        }

        public QueryBuilder snapshot() {
            params().snapshot = true;
            return this;
        }

        public QueryBuilder comment(String comment) {
            params().comment = comment;
            return this;
        }

        public QueryBuilder addSpecial(String name, Object o) {
            params().addSpecial(name, o);
            return this;
        }

        public DBObject andUpdateOne(DBObject update) {
            return andUpdateOne(update, false);
        }

        public DBObject andUpdateOne(DBObject update, boolean returnNew) {
            if (params != null) {
                throw new IllegalStateException("Query parameters not supported for findAndModify");
            }
            return dbc.findAndModify(query, fields, sort, false, update, returnNew, false);
        }

        public DBObject andUpsertOne(DBObject update) {
            return  andUpsertOne(update, false);
        }

        public DBObject andUpsertOne(DBObject update, boolean returnNew) {
            if (params != null) {
                throw new IllegalStateException("Query parameters not supported for findAndModify");
            }
            return dbc.findAndModify(query, fields, sort, false, update, returnNew, true);
        }

        public DBObject andRemoveOne() {
            if (params != null) {
                throw new IllegalStateException("Query parameters not supported for findAndModify");
            }
            return dbc.findAndModify(query, fields, sort, true, null, false, false);
        }

        @Override
        public Cursor iterator() {
            DBCursor cursor = dbc.find(query, fields);
            if (sort != null) {
                cursor.sort(sort);
            }
            if (params != null) {
                params.write(cursor);
            }
            return new Cursor(cursor);
        }

        public int count() {
            DBCursor cursor = dbc.find(query, fields);
            if (sort != null) {
                cursor.sort(sort);
            }
            if (params != null) {
                params.write(cursor);
            }
            return cursor.count();
        }
    }

    public class Cursor implements Iterator<T>, Closeable {
        private final DBCursor cursor;

        private Cursor(DBCursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public boolean hasNext() {
            return cursor.hasNext();
        }

        @Override
        public T next() {
            GsonWrapper wrapper = (GsonWrapper) cursor.next();
            return wrapper != null? clazz.cast(wrapper.getPojo()): null;
        }

        @Override
        public void remove() {
            cursor.remove();
        }

        @Override
        public void close() {
            cursor.close();
        }
    }

    private static class QueryParams {
        private int skip;
        private int limit;
        private int batchSize;
        private long timeout;
        private DBObject hintO;
        private String hintS;
        private String comment;
        private boolean snapshot;
        private BasicDBObject specials;

        private void addSpecial(String name, Object o) {
            if (specials == null) {
                specials = new BasicDBObject();
            }
            specials.put(name, o);
        }

        public void write(DBCursor cursor) {
            cursor
                .skip(skip)
                .limit(limit)
                .batchSize(batchSize)
                .maxTime(timeout, MILLISECONDS)
                .hint(hintO)
                .hint(hintS);
            if (comment != null) {
                cursor.comment(comment);
            }
            if (snapshot) {
                cursor.snapshot();
            }
            if (specials != null) {
                for (Map.Entry<String, Object> entry : specials.entrySet()) {
                    cursor.addSpecial(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    // ---- Bulk modification ------------------------------------------------------------------------------------------

    public BulkOperation<T> bulk(boolean ordered) {
        return new BulkOperation<T>(ordered? dbc.initializeOrderedBulkOperation(): dbc.initializeUnorderedBulkOperation());
    }

    public static class BulkOperation<T> {
        private final BulkWriteOperation op;

        public BulkOperation(BulkWriteOperation op) {
            this.op = op;
        }

        public boolean isOrdered() {
            return op.isOrdered();
        }

        public void insert(T doc) {
            op.insert(GsonWrapper.of(doc));
        }

        public Builder find(DBObject query) {
            return new Builder(op.find(query));
        }

        public BulkWriteResult execute() {
            return op.execute();
        }

        public static class Builder {
            private final BulkWriteRequestBuilder request;

            public Builder(BulkWriteRequestBuilder request) {
                this.request = request;
            }

            public void updateOne(DBObject update) {
                request.updateOne(update);
            }

            public void updateAll(DBObject update) {
                request.update(update);
            }

            public void upsertOne(DBObject update) {
                request.upsert().updateOne(update);
            }

            public void removeOne() {
                request.removeOne();
            }

            public void removeAll() {
                request.remove();
            }
        }
    }

    // ---- Encoding/Decoding ------------------------------------------------------------------------------------------

    /**
     * {@link DBEncoder} implementation which supports {@link GsonWrapper} serialization.
     * <p>Supports normal {@link DBObject} serialization, but adds special handling
     * for {@link GsonWrapper}. For such objects uses Gson streaming.</p>
     */
    @VisibleForTesting
    class GsonDBEncoder implements DBEncoder {
        private final GsonWriter writer = new GsonWriter();

        @Override
        public int writeObject(OutputBuffer outputBuffer, BSONObject document) {
            if (document == null) {
                throw new NullPointerException("Can't write top-level null document");
            }

            int startPosition = outputBuffer.getPosition();
            try {
                try {
                    writer.reset(outputBuffer);
                    writeValue(writer, document);
                } finally {
                    writer.reset(null);
                }
            } catch (IOException e) {
                throw new MongoException("Can't serialize DBObject", e);
            }

            return outputBuffer.getPosition() - startPosition;
        }

        private void writeValue(GsonWriter writer, Object o) throws IOException {
            BsonWriter bsonWriter = writer.getBsonWriter();
            if (o == null) {
                bsonWriter.nullValue();
            } else if (o instanceof GsonWrapper) {
                Object object = ((GsonWrapper) o).getPojo(); // TODO: may be check type here?
                gson.toJson(object, object.getClass(), writer);
            } else if (o instanceof Iterable) {
                bsonWriter.beginArray();
                for (Object value : (Iterable<?>) o) {
                    writeValue(writer, value);
                }
                bsonWriter.endArray();
            } else if (o instanceof BSONObject) {
                // TODO: does mongo calls custom encoders for queries?
                BSONObject bson = (BSONObject) o;
                bsonWriter.beginObject();
                for (String key : bson.keySet()) {
                    // TODO: handle $where here
                    bsonWriter.name(key);
                    writeValue(writer, bson.get(key));
                }
                bsonWriter.endObject();
            } else if (o instanceof Map) {
                Map<?, ?> m = (Map<?, ?>) o;
                bsonWriter.beginObject();
                for (Map.Entry<?, ?> entry : m.entrySet()) {
                    bsonWriter.name((String) entry.getKey());
                    writeValue(writer, entry.getValue());
                }
                bsonWriter.endObject();
            } else if (o instanceof byte[]) {
                bsonWriter.binaryValue(new Binary((byte[]) o));
            } else if (o.getClass().isArray()) {
                bsonWriter.beginArray();
                int len = Array.getLength(o);
                for (int i = 0; i < len; i++) {
                    writeValue(writer, Array.get(o, i));
                }
                bsonWriter.endArray();
            } else if (o instanceof GsonNullable) {
                GsonNullable<?> nullable = (GsonNullable<?>) o;
                if (nullable.isPresent()) {
                    writeValue(writer, nullable.get());
                } else {
                    bsonWriter.nullValue();
                }
            } else {
                bsonWriter.value(o);
            }
        }

        @Override
        public String toString() {
            return "GsonDBEncoder";
        }
    }

    /**
     * {@link DBDecoder} implementation which supports {@link GsonWrapper} deserialization.
     * <p>Deserializes data using Gson streams and stores them to GsonWrapper.</p>
     * <p>Type of destination object specified in constructor.</p>
     * <p>Decoding using callback is unsupported so callback-related methods throws UOE.</p>
     */
    @VisibleForTesting
    class GsonDBDecoder implements DBDecoder {
        private final GsonReader reader = new GsonReader(new BsonReader());

        @Override
        public DBCallback getDBCallback(DBCollection collection) {
            throw new UnsupportedOperationException("Parsing stream through callback is unsupported");
        }

        @Override
        public int decode(byte[] bytes, BSONCallback callback) {
            throw new UnsupportedOperationException("Parsing stream through callback is unsupported");
        }

        @Override
        public int decode(InputStream in, BSONCallback callback) throws IOException {
            throw new UnsupportedOperationException("Parsing stream through callback is unsupported");
        }

        @Override
        public DBObject decode(InputStream is, DBCollection collection) throws IOException {
            return readObject(is);
        }

        @Override
        public DBObject decode(byte[] bytes, DBCollection collection) {
            return readObject(bytes);
        }

        @Override
        public DBObject readObject(byte[] bytes) {
            try {
                return readObject(new ByteArrayInputStream(bytes));
            } catch (IOException e) {
                throw new IllegalStateException("Invalid BSON data", e);
            }
        }

        @Override
        public DBObject readObject(InputStream in) throws IOException {
            try {
                reader.reset(in);
                Object pojo = gson.fromJson(reader, clazz);
                return pojo instanceof DBObject? (DBObject) pojo: new GsonWrapper(pojo);
            } finally {
                reader.reset(null);
            }
        }

        @Override
        public String toString() {
            return String.format("GsonDBDecoder{for=%s}", clazz.getName());
        }
    }
}
