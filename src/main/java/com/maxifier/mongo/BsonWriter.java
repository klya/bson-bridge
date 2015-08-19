/*
 * Copyright (c) 2008-2014 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import org.bson.BSON;
import org.bson.io.OutputBuffer;
import org.bson.types.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Low-level BSON writer which consumes pure BSON stream.
 * <p>Use {@link #reset(OutputBuffer)} to reuse writer instance.</p>
 *
 * @author Konstantin Lyamshin (2014-12-22 19:24)
 */
public class BsonWriter {
    private static final String OBJECT = ".";
    private static final String ARRAY = "#";

    private OutputBuffer out;

    private String[] path = new String[16];
    private int[] offsets = new int[16];
    private int stack = -1;

    // ---- Object initialization and troubleshooting ------------------------------------------------------------------

    public void reset(@Nullable OutputBuffer out) {
        this.out = out;
        this.stack = -1;
    }

    public void flush() throws IOException {
        if (out != null) {
            out.flush();
        }
    }

    public void close() throws IOException {
        if (out != null) {
            out.close();
            reset(null);
        }
    }

    public String getPath() {
        if (stack < 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder("$");
        for (int i = 0; i <= stack; i++) {
            String p = path[i];
            //noinspection StringEquality
            if (p != ARRAY) {
                sb.append(p);
            } else if (i < stack) {
                sb.append("[").append(path[++i]).append("]");
            } else {
                sb.append("[]");
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return String.format("BsonWriter{@%d:%s}", out != null? out.getPosition(): -1, getPath());
    }

    // ---- Streaming interface ----------------------------------------------------------------------------------------

    public void beginObject() {
        if (out == null) {
            throw new IllegalStateException("Stream is closed: " + toString());
        }

        if (stack < 0) { // root object
            int offset = out.getPosition();
            out.writeInt(-1); // size unknown
            push(OBJECT, offset);
            return;
        }

        String name = getName();
        out.write(BSON.OBJECT);
        out.writeCString(name);
        int offset = out.getPosition();
        out.writeInt(-1); // size unknown
        push(OBJECT, offset);
    }

    public void endObject() {
        if (!isObject()) {
            throw new IllegalStateException("Value required after name: " + toString());
        }
        out.write(BSON.EOO);
        out.backpatchSize(out.getPosition() - pop());
        if (stack < 0) {
            reset(null); // stream finished
        } else {
            next();
        }
    }

    public void beginArray() {
        String name = getName();
        out.write(BSON.ARRAY);
        out.writeCString(name);
        int offset = out.getPosition();
        out.writeInt(-1); // size unknown
        push(ARRAY, offset);
        push("0", out.getPosition()); // initial index
    }

    public void endArray() {
        if (!isArray()) {
            throw new IllegalStateException("Not an array: " + toString());
        }
        pop(); // array index
        out.write(BSON.EOO);
        out.backpatchSize(out.getPosition() - pop());
        next();
    }

    public void name(String name) {
        if (!isObject()) {
            throw new IllegalStateException("Not an object: " + toString());
        }
        if (name.equals(OBJECT) || name.equals(ARRAY)) {
            throw new IllegalArgumentException("Invalid name for property: " + name);
        }
        push(name, out.getPosition());
    }

    public void nullValue() {
        String name = getName();
        out.write(BSON.NULL);
        out.writeCString(name);
        next();
    }

    public void stringValue(String value) {
        String name = getName();
        out.write(BSON.STRING);
        out.writeCString(name);
        out.writeString(value);
        next();
    }

    public void intValue(int value) {
        String name = getName();
        out.write(BSON.NUMBER_INT);
        out.writeCString(name);
        out.writeInt(value);
        next();
    }

    public void longValue(long value) {
        String name = getName();
        out.write(BSON.NUMBER_LONG);
        out.writeCString(name);
        out.writeLong(value);
        next();
    }

    public void doubleValue(double value) {
        String name = getName();
        out.write(BSON.NUMBER);
        out.writeCString(name);
        out.writeDouble(value);
        next();
    }

    public void booleanValue(boolean value) {
        String name = getName();
        out.write(BSON.BOOLEAN);
        out.writeCString(name);
        out.write(value? 1: 0);
        next();
    }

    public void dateValue(Date value) {
        String name = getName();
        out.write(BSON.DATE);
        out.writeCString(name);
        out.writeLong(value.getTime());
        next();
    }

    public void objectIdValue(ObjectId value) {
        String name = getName();
        out.write(BSON.OID);
        out.writeCString(name);
        out.write(value.toByteArray());
        next();
    }

    public void binaryValue(Binary value) {
        String name = getName();
        out.write(BSON.BINARY);
        out.writeCString(name);
        out.writeInt(value.length());
        out.write(value.getType());
        out.write(value.getData());
        next();
    }

    public void binaryValue(byte[] value) {
        String name = getName();
        out.write(BSON.BINARY);
        out.writeCString(name);
        out.writeInt(value.length);
        out.write(BSON.B_GENERAL);
        out.write(value);
        next();
    }

    public void uuidValue(UUID value) {
        String name = getName();
        out.write(BSON.BINARY);
        out.writeCString(name);
        out.writeInt(16);
        out.write(BSON.B_UUID); // ARRGH deprecated value
        out.writeLong(value.getMostSignificantBits());
        out.writeLong(value.getLeastSignificantBits());
        next();
    }

    public void regexValue(Pattern value) {
        String name = getName();
        out.write(BSON.REGEX);
        out.writeCString(name);
        out.writeCString(value.pattern());
        out.writeCString(BSON.regexFlags(value.flags()));
        next();
    }

    public void codeValue(Code value) {
        String name = getName();
        out.write(BSON.CODE);
        out.writeCString(name);
        out.writeString(value.getCode());
        next();
    }

    public void timestampValue(BSONTimestamp value) {
        String name = getName();
        out.write(BSON.TIMESTAMP);
        out.writeCString(name);
        out.writeInt(value.getInc());
        out.writeInt(value.getTime());
        next();
    }

    public void minkeyValue() {
        String name = getName();
        out.write(BSON.MINKEY);
        out.writeCString(name);
        next();
    }

    public void maxkeyValue() {
        String name = getName();
        out.write(BSON.MAXKEY);
        out.writeCString(name);
        next();
    }

    // ---- Typeless values --------------------------------------------------------------------------------------------

    /**
     * Detect Number type and write a value to the buffer
     */
    public void numberValue(Number n) {
        if (n instanceof Integer || n instanceof Short || n instanceof Byte || n instanceof AtomicInteger) {
            intValue(n.intValue());
        } else if (n instanceof Long || n instanceof AtomicLong) {
            longValue(n.longValue());
        } else if (n instanceof Double || n instanceof Float) {
            doubleValue(n.doubleValue());
        } else {
            throw new IllegalArgumentException("Invalid Number class " + n.getClass().getName());
        }
    }

    /**
     * Detect BSON type and write a value to the buffer
     */
    public void value(Object o) {
        if (o == null) {
            nullValue();
        } else if (o instanceof Number) {
            numberValue((Number) o);
        } else if (o instanceof String) {
            stringValue(o.toString());
        } else if (o instanceof Date) {
            dateValue((Date) o);
        } else if (o instanceof ObjectId) {
            objectIdValue((ObjectId) o);
        } else if (o instanceof Boolean) {
            booleanValue((Boolean) o);
        } else if (o instanceof Character) {
            stringValue(o.toString());
        } else if (o instanceof Pattern) {
            regexValue((Pattern) o);
        } else if (o instanceof Binary) {
            binaryValue((Binary) o);
        } else if (o instanceof byte[]) {
            binaryValue((byte[]) o);
        } else if (o instanceof UUID) {
            uuidValue((UUID) o);
        } else if (o instanceof Symbol) {
            stringValue(((Symbol) o).getSymbol());
        } else if (o instanceof BSONTimestamp) {
            timestampValue((BSONTimestamp) o);
        } else if (o instanceof Code) {
            codeValue((Code) o);
        } else if (o instanceof MinKey) {
            minkeyValue();
        } else if (o instanceof MaxKey) {
            maxkeyValue();
        } else {
            throw new IllegalArgumentException("Unknown field type " + o.getClass());
        }
    }

    // ---- State manipulation -----------------------------------------------------------------------------------------

    /**
     * Drops current path frame
     *
     * @return path item
     */
    String dropValue() {
        String name = path[stack];
        out.truncateToPosition(pop());
        return name;
    }

    boolean isObject() {
        if (stack < 0) {
            throw new IllegalStateException("Stream is closed: " + toString());
        }
        //noinspection StringEquality
        return path[stack] == OBJECT;
    }

    boolean isArray() {
        if (stack < 0) {
            throw new IllegalStateException("Stream is closed: " + toString());
        }
        //noinspection StringEquality
        return stack > 0 && path[stack - 1] == ARRAY;
    }

    String getName() {
        if (isObject()) {
            throw new IllegalStateException("Field name or array expected: " + toString());
        }
        return path[stack];
    }

    /**
     * Switches to the next field.
     */
    private void next() {
        if (!isArray()) {
            pop();
            return;
        }

        String item = path[stack];
        int digit = Character.digit(item.charAt(item.length() - 1), 10) + 1;
        path[stack] = digit < 10
            ? item.substring(0, item.length() - 1) + Character.forDigit(digit, 10)
            : Integer.toString(Integer.parseInt(item) + 1);
        offsets[stack] = out.getPosition();
    }

    /**
     * Pushes path item to stack and saves its position in buffer.
     */
    private void push(String item, int offset) {
        if (++stack == path.length) {
            int size = path.length * 3 / 2 + 1;
            path = Arrays.copyOf(path, size);
            offsets = Arrays.copyOf(offsets, size);
        }

        path[stack] = item;
        offsets[stack] = offset;
    }

    /**
     * Pops top path item from stack and patches sizes if it was array or object.
     */
    private int pop() {
        path[stack] = null;
        return offsets[stack--];
    }
}
