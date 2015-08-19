/*
 * Copyright (c) 2008-2015 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayDeque;
import java.util.Iterator;

import static com.google.gson.stream.JsonToken.*;

/**
 * Buffer which holds a part of Json stream and supports
 * {@link JsonReader} and {@link JsonWriter} to access them.
 * <p>Buffer filled by connected Writer and emptied by connected Reader.
 * Access them by getters.</p>
 * <p>Reader supports special {@link Prefetchable} interface which gives ability
 * to read first object's field name in advance.</p>
 * <p>If you need to make any {@code JsonReader} Prefetchable
 * use {@link PrefetchableReader} wrapper.</p>
 *
 * @author Konstantin Lyamshin (2015-01-23 18:10)
 */
public final class GsonBuffer {
    private final ArrayDeque<Object> tokens = new ArrayDeque<Object>(10);
    private final JsonBufferedReader reader = new JsonBufferedReader();
    private final JsonBufferedWriter writer = new JsonBufferedWriter();

    public boolean isEmpty() {
        return tokens.isEmpty();
    }

    public void clear() {
        tokens.clear();
    }

    public JsonBufferedReader reader() {
        return reader;
    }

    public JsonBufferedWriter writer() {
        return writer;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String delimiter = "";
        for (Iterator<Object> it = tokens.iterator(); it.hasNext(); ) {
            JsonToken token = (JsonToken) it.next();
            switch (token) {
                case BEGIN_OBJECT:
                    sb.append('{');
                    delimiter = "";
                    break;

                case END_OBJECT:
                    sb.append('}');
                    delimiter = ", ";
                    break;

                case BEGIN_ARRAY:
                    sb.append('[');
                    delimiter = "";
                    break;

                case END_ARRAY:
                    sb.append(']');
                    delimiter = ", ";
                    break;

                case NAME:
                    sb.append(delimiter).append(it.hasNext()? it.next(): "<NULL>");
                    delimiter = ": ";
                    break;

                case STRING:
                case NUMBER:
                case BOOLEAN:
                case NULL:
                    sb.append(delimiter).append(it.hasNext()? it.next(): "<NULL>");
                    delimiter = ", ";
                    break;

                default:
                    sb.append("<UNKNOWN:").append(token.name()).append('>');
                    delimiter = "";
            }
        }
        return sb.toString();
    }

    /**
     * Gives ability to prefetch next object's first field name to detect system sequences.
     */
    public interface Prefetchable {
        /**
         * Prefetch first object field name.
         * @return field name, null if field not found
         */
        @Nullable
        String peekObjectField() throws IOException;
    }

    /**
     * {@link JsonWriter} implementation connected to a common buffer.
     */
    public final class JsonBufferedWriter extends JsonWriter {
        private JsonBufferedWriter() {
            super(NULL_WRITER);
        }

        public boolean isEmpty() {
            return tokens.isEmpty();
        }

        public GsonBuffer buffer() {
            return GsonBuffer.this;
        }

        @Override
        public boolean isLenient() {
            return true; // allow any values
        }

        @Override
        public JsonWriter beginObject() {
            tokens.add(BEGIN_OBJECT);
            return this;
        }

        @Override
        public JsonWriter endObject() {
            tokens.add(END_OBJECT);
            return this;
        }

        @Override
        public JsonWriter beginArray() {
            tokens.add(BEGIN_ARRAY);
            return this;
        }

        @Override
        public JsonWriter endArray() {
            tokens.add(END_ARRAY);
            return this;
        }

        @Override
        public JsonWriter name(String name) {
            tokens.add(NAME);
            tokens.add(name);
            return this;
        }

        @Override
        public JsonWriter nullValue() {
            tokens.add(NULL);
            tokens.add("null");
            return this;
        }

        @Override
        public JsonWriter value(String value) {
            tokens.add(STRING);
            tokens.add(value);
            return this;
        }

        @Override
        public JsonWriter value(boolean value) {
            tokens.add(BOOLEAN);
            tokens.add(value);
            return this;
        }

        @Override
        public JsonWriter value(double value) {
            tokens.add(NUMBER);
            tokens.add(value);
            return this;
        }

        @Override
        public JsonWriter value(long value) {
            tokens.add(NUMBER);
            tokens.add(value);
            return this;
        }

        @Override
        public JsonWriter value(Number value) {
            tokens.add(NUMBER);
            tokens.add(value);
            return this;
        }

        @Override
        public void flush() {
            // Do nothing
        }

        @Override
        public void close() {
            // Do nothing
        }

        @Override
        public String toString() {
            return GsonBuffer.this.toString();
        }
    }

    /**
     * {@link JsonReader} implementation connected to a common buffer.
     */
    public final class JsonBufferedReader extends JsonReader implements Prefetchable {
        private JsonBufferedReader() {
            super(NULL_READER);
        }

        public boolean isEmpty() {
            return tokens.isEmpty();
        }

        public GsonBuffer buffer() {
            return GsonBuffer.this;
        }

        @Override
        public JsonToken peek() {
            JsonToken token = (JsonToken) tokens.peek();
            return token != null? token: END_DOCUMENT;
        }

        @Override
        @Nullable
        public String peekObjectField() {
            Iterator<Object> it = tokens.iterator();
            if (it.hasNext() && it.next() == BEGIN_OBJECT) {
                if (it.hasNext() && it.next() == NAME) {
                    return (String) it.next();
                }
            }
            return null;
        }

        @Override
        public boolean hasNext() {
            Object token = tokens.peek();
            return token != END_OBJECT && token != END_ARRAY;
        }

        @Override
        public void beginObject() {
            if (tokens.peek() != BEGIN_OBJECT) {
                throw new IllegalStateException("Expected BEGIN_OBJECT but was: " + tokens.peek());
            }
            tokens.remove();
        }

        @Override
        public void endObject() {
            if (tokens.peek() != END_OBJECT) {
                throw new IllegalStateException("Expected END_OBJECT but was: " + tokens.peek());
            }
            tokens.remove();
        }

        @Override
        public void beginArray() {
            if (tokens.peek() != BEGIN_ARRAY) {
                throw new IllegalStateException("Expected BEGIN_ARRAY but was: " + tokens.peek());
            }
            tokens.remove();
        }

        @Override
        public void endArray() {
            if (tokens.peek() != END_ARRAY) {
                throw new IllegalStateException("Expected END_ARRAY but was: " + tokens.peek());
            }
            tokens.remove();
        }

        @Override
        public String nextName() {
            if (tokens.peek() != NAME) {
                throw new IllegalStateException("Expected NAME but was: " + tokens.peek());
            }
            tokens.remove();
            return (String) tokens.remove();
        }

        @Override
        public void nextNull() {
            if (tokens.peek() != NULL) {
                throw new IllegalStateException("Expected NULL but was: " + tokens.peek());
            }
            tokens.remove();
            tokens.remove();
        }

        @Override
        public String nextString() {
            Object peek = tokens.peek();
            if (peek == STRING) {
                tokens.remove();
                return (String) tokens.remove();
            }
            if (peek == NUMBER) {
                tokens.remove();
                return tokens.remove().toString();
            }
            if (peek == BOOLEAN) {
                tokens.remove();
                return (Boolean) tokens.remove()? "true": "false";
            }
            throw new IllegalStateException("Expected STRING but was: " + peek);
        }

        @Override
        public boolean nextBoolean() {
            if (tokens.peek() != BOOLEAN) {
                throw new IllegalStateException("Expected BOOLEAN but was: " + tokens.peek());
            }
            tokens.remove();
            return (Boolean) tokens.remove();
        }

        @Override
        public double nextDouble() {
            Object peek = tokens.peek();
            if (peek == NUMBER) {
                tokens.remove();
                return ((Number) tokens.remove()).doubleValue();
            }
            if (peek == STRING) {
                tokens.remove();
                return Double.parseDouble((String) tokens.remove());
            }
            throw new IllegalStateException("Expected NUMBER but was: " + peek);
        }

        @Override
        public long nextLong() {
            Object peek = tokens.peek();
            if (peek == NUMBER) {
                tokens.remove();
                return ((Number) tokens.remove()).longValue();
            }
            if (peek == STRING) {
                tokens.remove();
                return Long.parseLong((String) tokens.remove());
            }
            throw new IllegalStateException("Expected NUMBER but was: " + peek);
        }

        @Override
        public int nextInt() {
            Object peek = tokens.peek();
            if (peek == NUMBER) {
                tokens.remove();
                return ((Number) tokens.remove()).intValue();
            }
            if (peek == STRING) {
                tokens.remove();
                return Integer.parseInt((String) tokens.remove());
            }
            throw new IllegalStateException("Expected NUMBER but was: " + peek);
        }

        public Number nextNumber() {
            Object peek = tokens.peek();
            if (peek == NUMBER) {
                tokens.remove();
                return (Number) tokens.remove();
            }
            if (peek == STRING) {
                tokens.remove();
                String value = (String) tokens.remove();
                try {
                    return Long.valueOf(value);
                } catch (NumberFormatException ignored) {
                    return Double.valueOf(value); // Propagate NumberFormatException
                }
            }
            throw new IllegalStateException("Expected NUMBER but was: " + peek);
        }

        @Override
        public void skipValue() {
            switch (peek()) {
                case BEGIN_OBJECT:
                case BEGIN_ARRAY:
                case END_OBJECT:
                case END_ARRAY:
                    throw new UnsupportedOperationException("Buffer contains uncompleted object sequences which can't be skipped");

                case NAME:
                case STRING:
                case NUMBER:
                case BOOLEAN:
                case NULL:
                    tokens.remove();
                    tokens.remove();
                    break;

                case END_DOCUMENT:
                    throw new IllegalStateException("Can not skip unfinished object: " + toString());

                default:
                    throw new IllegalStateException("Unexpected state: " + peek());
            }
        }

        @Override
        public void close() {
            // Do nothing
        }

        @Override
        public String getPath() {
            return "";
        }

        @Override
        public String toString() {
            return GsonBuffer.this.toString();
        }
    }

    /**
     * {@link JsonReader} wrapper which supports {@link Prefetchable} interface.
     */
    public static final class PrefetchableReader extends JsonReader implements Prefetchable {
        private final JsonReader reader;
        private boolean prefetchedObject;
        private String prefetchedName;

        public PrefetchableReader(JsonReader reader) {
            super(NULL_READER);
            this.reader = reader;
        }

        @Override
        public void close() throws IOException {
            reader.close();
            prefetchedObject = false;
            prefetchedName = null;
        }

        @Override
        public String toString() {
            return String.format("Prefetcher{%s(%s)}", reader,
                prefetchedObject? "BEGIN_OBJECT": prefetchedName != null? prefetchedName: ""
            );
        }

        @Override
        public String getPath() {
            return reader.getPath();
        }

        @Override
        public boolean hasNext() throws IOException {
            return isPrefetched() || reader.hasNext();
        }

        @Override
        public JsonToken peek() throws IOException {
            if (prefetchedObject) {
                return BEGIN_OBJECT;
            }
            if (prefetchedName != null) {
                return NAME;
            }
            return reader.peek();
        }

        @Nullable
        @Override
        public String peekObjectField() throws IOException {
            if (prefetchedObject && prefetchedName != null) {
                return prefetchedName;
            }
            if (reader.peek() == BEGIN_OBJECT) {
                reader.beginObject();
                prefetchedObject = true;
                if (reader.peek() == NAME) {
                    prefetchedName = reader.nextName();
                    assert prefetchedName != null: "Field name can't be null";
                    return prefetchedName;
                }
            }
            return null;
        }

        private boolean isPrefetched() {
            return prefetchedObject || prefetchedName != null;
        }

        @Override
        public void beginObject() throws IOException {
            if (prefetchedObject) {
                prefetchedObject = false;
            } else if (prefetchedName != null) {
                throw new IllegalStateException("Expected BEGIN_OBJECT, but got " + toString());
            } else {
                reader.beginObject();
            }
        }

        @Override
        public void endObject() throws IOException {
            if (isPrefetched()) {
                throw new IllegalStateException("Expected END_OBJECT, but got " + toString());
            }
            reader.endObject();
        }

        @Override
        public void beginArray() throws IOException {
            if (isPrefetched()) {
                throw new IllegalStateException("Expected BEGIN_ARRAY, but got " + toString());
            }
            reader.beginArray();
        }

        @Override
        public void endArray() throws IOException {
            if (isPrefetched()) {
                throw new IllegalStateException("Expected END_ARRAY, but got " + toString());
            }
            reader.endArray();
        }

        @Override
        public String nextName() throws IOException {
            if (prefetchedObject) {
                throw new IllegalStateException("Expected NAME, but got " + toString());
            }
            if (prefetchedName != null) {
                String name = prefetchedName;
                prefetchedName = null;
                return name;
            }
            return reader.nextName();
        }

        @Override
        public String nextString() throws IOException {
            if (isPrefetched()) {
                throw new IllegalStateException("Expected STRING, but got " + toString());
            }
            return reader.nextString();
        }

        @Override
        public boolean nextBoolean() throws IOException {
            if (isPrefetched()) {
                throw new IllegalStateException("Expected BOOLEAN, but got " + toString());
            }
            return reader.nextBoolean();
        }

        @Override
        public void nextNull() throws IOException {
            if (isPrefetched()) {
                throw new IllegalStateException("Expected NULL, but got " + toString());
            }
            reader.nextNull();
        }

        @Override
        public double nextDouble() throws IOException {
            if (isPrefetched()) {
                throw new IllegalStateException("Expected NUMBER, but got " + toString());
            }
            return reader.nextDouble();
        }

        @Override
        public long nextLong() throws IOException {
            if (isPrefetched()) {
                throw new IllegalStateException("Expected NUMBER, but got " + toString());
            }
            return reader.nextLong();
        }

        @Override
        public int nextInt() throws IOException {
            if (isPrefetched()) {
                throw new IllegalStateException("Expected NUMBER, but got " + toString());
            }
            return reader.nextInt();
        }

        @Override
        public void skipValue() throws IOException {
            if (prefetchedObject) {
                // Skip object already fetched
                prefetchedObject = false;
                if (prefetchedName != null) {
                    reader.skipValue();
                    prefetchedName = null;
                }
                while (reader.hasNext()) {
                    reader.nextName();
                    reader.skipValue();
                }
                reader.endObject();
                return;
            }
            if (prefetchedName != null) {
                throw new IllegalStateException("Expected VALUE, but got " + toString());
            }
            reader.skipValue();
        }
    }

    // ---- Internals --------------------------------------------------------------------------------------------------

    static final Writer NULL_WRITER = new Writer() {
        @Override
        public void write(char[] cbuf, int off, int len) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void flush() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return "GsonBuffer.NULL_WRITER";
        }
    };

    static final Reader NULL_READER = new Reader() {
        @Override
        public int read(char[] cbuf, int off, int len) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return "GsonBuffer.NULL_READER";
        }
    };
}
