/*
 * Copyright (c) 2008-2014 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;

import static com.google.gson.stream.JsonToken.*;
import static com.maxifier.mongo.BsonReader.*;
import static com.maxifier.mongo.GsonAdapters.*;

/**
 * Gson specific reader that parses BSON stream to a text JSON stream.
 * <p>Works as wrapper for {@link BsonReader} which converts internal
 * BSON types to special JSON sequences. Special sequences detected by
 * {@code TypeAdapter}s declared in {@link GsonAdapters}.</p>
 * <p>Use {@link #reset(InputStream)} to reuse reader instance.</p>
 *
 * @see BsonReader
 * @see GsonAdapters
 * @author Konstantin Lyamshin (2014-12-19 10:45)
 */
public class GsonReader extends JsonReader implements GsonBuffer.Prefetchable {
    private final BsonReader bson;
    private final GsonBuffer.JsonBufferedReader buffer;
    private JsonToken peeked;

    public GsonReader(BsonReader bson) {
        super(GsonBuffer.NULL_READER);
        this.bson = bson;
        this.buffer = new GsonBuffer().reader();
    }

    public void reset(InputStream in) {
        this.bson.reset(in);
        this.buffer.buffer().clear();
        this.peeked = null;
    }

    @Override
    public void close() throws IOException {
        this.bson.close();
        this.buffer.buffer().clear();
        this.peeked = null;
    }

    // ---- Troubleshooting --------------------------------------------------------------------------------------------

    @Override
    public String getPath() {
        return bson.getPath();
    }

    @Override
    public String toString() {
        return String.format("GsonReader{%s(%s)}", bson, buffer);
    }

    // ---- JsonReader contract ----------------------------------------------------------------------------------------

    /**
     * Detects EOO w/o prefetching objects by {@link #peek()}.
     * This behaviour is needed because {@link #skipValue()} don't want go deep into objects.
     */
    @Override
    public boolean hasNext() throws IOException {
        if (!buffer.isEmpty()) {
            JsonToken token = buffer.peek();
            return token != END_OBJECT && token != END_ARRAY;
        }

        if (peeked != null) {
            return peeked != END_OBJECT && peeked != END_ARRAY;
        }

        return bson.hasNext();
    }

    @Override
    public JsonToken peek() throws IOException {
        if (!buffer.isEmpty()) {
            return buffer.peek();
        }

        if (peeked != null) {
            return peeked;
        }

        GsonBuffer.JsonBufferedWriter out = buffer.buffer().writer();
        switch (bson.peek()) {
            case P_BEGIN_OBJECT:
                // prefetch first field name
                bson.beginObject();
                out.beginObject();
                if (bson.peek() == P_NAME) {
                    out.name(bson.nextName());
                }
                return buffer.peek();

            case P_END_OBJECT:
                return peeked = END_OBJECT;

            case P_BEGIN_ARRAY:
                return peeked = BEGIN_ARRAY;

            case P_END_ARRAY:
                return peeked = END_ARRAY;

            case P_NAME:
                return peeked = NAME;

            case P_STRING:
                return peeked = STRING;

            case P_BOOLEAN:
                return peeked = BOOLEAN;

            case P_INT:
            case P_DOUBLE:
                return peeked = NUMBER;

            case P_NONE:
                return peeked = END_DOCUMENT;

            // fill system sequences

            case P_NULL:
                if (bson.isArray()) {
                    return peeked = NULL;
                }
                bson.nextNull();
                NULLABLE_DUMMY_ADAPTER.write(out, GsonNullable.absent());
                return buffer.peek();

            case P_LONG:
                LONG_ADAPTER.write(out, bson.nextLong());
                return buffer.peek();

            case P_OID:
                OBJECTID_ADAPTER.write(out, bson.nextObjectId());
                return buffer.peek();

            case P_DATE:
                DATE_ADAPTER.write(out, bson.nextDate());
                return buffer.peek();

            case P_BINARY:
                BINARY_ADAPTER.write(out, bson.nextBinary());
                return buffer.peek();

            case P_REGEX:
                REGEX_ADAPTER.write(out, bson.nextRegex());
                return buffer.peek();

            case P_CODE:
                CODE_ADAPTER.write(out, bson.nextCode());
                return buffer.peek();

            case P_TIMESTAMP:
                TIMESTAMP_ADAPTER.write(out, bson.nextTimestamp());
                return buffer.peek();

            case P_MINKEY:
                bson.nextMinKey();
                MINKEY_ADAPTER.write(out, new MinKey());
                return buffer.peek();

            case P_MAXKEY:
                bson.nextMaxKey();
                MAXKEY_ADAPTER.write(out, new MaxKey());
                return buffer.peek();

            default:
                throw new IllegalStateException("Unknown BsonReader state " + toString());
        }
    }

    @Nullable
    @Override
    public String peekObjectField() {
        return buffer.peekObjectField();
    }

    @Override
    public void beginObject() throws IOException {
        if (peek() != BEGIN_OBJECT) {
            throw new IllegalStateException("Expected Object but was " + toString());
        }
        assert !buffer.isEmpty(): "Object should always be buffered";
        buffer.beginObject();
    }

    @Override
    public void endObject() throws IOException {
        if (peek() != END_OBJECT) {
            throw new IllegalStateException("Expected EOO but was " + toString());
        }
        if (!buffer.isEmpty()) {
            buffer.endObject();
        } else {
            peeked = null;
            bson.endObject();
        }
    }

    @Override
    public void beginArray() throws IOException {
        if (peek() != BEGIN_ARRAY) {
            throw new IllegalStateException("Expected Array but was " + toString());
        }
        if (!buffer.isEmpty()) {
            buffer.beginArray();
        } else {
            peeked = null;
            bson.beginArray();
        }
    }

    @Override
    public void endArray() throws IOException {
        if (peek() != END_ARRAY) {
            throw new IllegalStateException("Expected EOO but was " + toString());
        }
        if (!buffer.isEmpty()) {
            buffer.endArray();
        } else {
            peeked = null;
            bson.endArray();
        }
    }

    @Override
    public String nextName() throws IOException {
        if (peek() != NAME) {
            throw new IllegalStateException("Expected Name but was " + toString());
        }
        if (!buffer.isEmpty()) {
            return buffer.nextName();
        }
        peeked = null;
        return bson.nextName();
    }

    @Override
    public void nextNull() throws IOException {
        if (peek() != NULL) {
            throw new IllegalStateException("Expected Null but was " + toString());
        }
        if (!buffer.isEmpty()) {
            buffer.nextNull();
        } else {
            peeked = null;
            bson.nextNull();
        }
    }

    @Override
    public String nextString() throws IOException {
        JsonToken token = peek();
        if (token == STRING) {
            if (!buffer.isEmpty()) {
                return buffer.nextString();
            }
            peeked = null;
            return bson.nextString();
        }

        if (token == NUMBER) {
            if (!buffer.isEmpty()) {
                return buffer.nextString();
            }

            peeked = null;

            int type = bson.peek();
            if (type == P_INT) {
                return Integer.toString(bson.nextInt());
            }
            if (type == P_DOUBLE) {
                return Double.toString(bson.nextDouble());
            }
            if (type == P_LONG) { // It's impossible but for uniformity:
                return Long.toString(bson.nextLong());
            }

            throw new IllegalStateException("Strange BsonReader state " + toString());
        }

        if (token == BOOLEAN) {
            if (!buffer.isEmpty()) {
                return buffer.nextString();
            }
            peeked = null;
            return bson.nextBoolean()? "true": "false";
        }

        throw new IllegalStateException("Expected String but was " + toString());
    }

    @Override
    public boolean nextBoolean() throws IOException {
        if (peek() != BOOLEAN) {
            throw new IllegalStateException("Expected Boolean but was " + toString());
        }
        if (!buffer.isEmpty()) {
            return buffer.nextBoolean();
        }
        peeked = null;
        return bson.nextBoolean();
    }

    @Override
    public double nextDouble() throws IOException {
        JsonToken token = peek();
        if (!buffer.isEmpty()) {
            return buffer.nextDouble();
        }

        if (token == NUMBER) {
            peeked = null;
            int type = bson.peek();
            if (type == P_INT) {
                return bson.nextInt();
            }
            if (type == P_DOUBLE) {
                return bson.nextDouble();
            }
            if (type == P_LONG) { // It's impossible but for uniformity:
                return bson.nextLong();
            }

            throw new IllegalStateException("Strange BsonReader state " + toString());
        }

        if (token == STRING) {
            peeked = null;
            return Double.parseDouble(bson.nextString());
        }

        throw new IllegalStateException("Expected Number but was " + toString());
    }

    @Override
    public long nextLong() throws IOException {
        JsonToken token = peek();
        if (!buffer.isEmpty()) {
            return buffer.nextLong();
        }

        if (token == NUMBER) {
            peeked = null;
            int type = bson.peek();
            if (type == P_INT) {
                return bson.nextInt();
            }
            if (type == P_DOUBLE) {
                return (long) bson.nextDouble();
            }
            if (type == P_LONG) { // It's impossible but for uniformity:
                return bson.nextLong();
            }

            throw new IllegalStateException("Strange BsonReader state " + toString());
        }

        if (token == STRING) {
            peeked = null;
            return Long.parseLong(bson.nextString());
        }

        throw new IllegalStateException("Expected Number but was " + toString());
    }

    @Override
    public int nextInt() throws IOException {
        JsonToken token = peek();
        if (!buffer.isEmpty()) {
            return buffer.nextInt();
        }

        if (token == NUMBER) {
            peeked = null;
            int type = bson.peek();
            if (type == P_INT) {
                return bson.nextInt();
            }
            if (type == P_DOUBLE) {
                return (int) bson.nextDouble();
            }
            if (type == P_LONG) { // It's impossible but for uniformity:
                return (int) bson.nextLong();
            }

            throw new IllegalStateException("Strange BsonReader state " + toString());
        }

        if (token == STRING) {
            peeked = null;
            return Integer.parseInt(bson.nextString());
        }

        throw new IllegalStateException("Expected Number but was " + toString());
    }

    @Override
    public void skipValue() throws IOException {
        if (buffer.isEmpty()) {
            peeked = null;
            bson.skipValue();
            return;
        }

        switch (buffer.peek()) {
            case BEGIN_OBJECT:
                beginObject();
                while (hasNext()) {
                    skipValue();
                }
                endObject();
                break;

            case BEGIN_ARRAY:
                beginArray();
                while (hasNext()) {
                    skipValue();
                }
                endArray();
                break;

            case NAME:
            case STRING:
            case NUMBER:
            case BOOLEAN:
            case NULL:
                buffer.skipValue();
                break;

            default:
                throw new IllegalStateException("Unexpected state: " + peek());
        }
    }
}
