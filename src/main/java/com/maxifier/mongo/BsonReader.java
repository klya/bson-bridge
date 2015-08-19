/*
 * Copyright (c) 2008-2014 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.google.common.annotations.VisibleForTesting;
import org.bson.BSON;
import org.bson.io.Bits;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.ObjectId;

import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Low-level BSON reader which produces pure BSON stream.
 * <p>Use {@link #reset(InputStream)} to reuse reader instance.</p>
 *
 * @author Konstantin Lyamshin (2014-12-22 19:24)
 */
public class BsonReader {
    public static final int P_NONE = 0;
    public static final int P_BEGIN_OBJECT = 1;
    public static final int P_END_OBJECT = 2;
    public static final int P_BEGIN_ARRAY = 3;
    public static final int P_END_ARRAY = 4;
    public static final int P_NAME = 5;
    public static final int P_NULL = 6;
    public static final int P_STRING = 7;
    public static final int P_BOOLEAN = 8;
    public static final int P_INT = 9;
    public static final int P_LONG = 10;
    public static final int P_DOUBLE = 11;
    public static final int P_OID = 12;
    public static final int P_DATE = 13;
    public static final int P_BINARY = 14;
    public static final int P_REGEX = 15;
    public static final int P_CODE = 16;
    public static final int P_TIMESTAMP = 17;
    public static final int P_MINKEY = 18;
    public static final int P_MAXKEY = 19;

    private static final String OBJECT = ".";
    private static final String ARRAY = "#";

    private String[] path = new String[16];
    private int[] limiters = new int[16];
    private int stack = -1;

    private int peeked;

    private InputStream in;
    private int pos;

    private byte[] buf = new byte[4]; // shared byte buffer
    private StringBuilder sb = new StringBuilder(); // shared char buffer (for strings)

    // ---- Object initialization and troubleshooting ------------------------------------------------------------------

    public void reset(@Nullable InputStream in) {
        this.in = in;
        this.pos = 0;
        this.stack = -1;
        this.peeked = P_NONE;
    }

    public void close() throws IOException {
        if (in != null) {
            in.close();
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
        return String.format("BsonReader{%s@%d:%s}", getState(), pos, getPath());
    }

    private String getState() {
        switch (peeked) {
            case P_NONE: return "NONE";
            case P_BEGIN_OBJECT: return "BEGIN_OBJECT";
            case P_END_OBJECT: return "END_OBJECT";
            case P_BEGIN_ARRAY: return "BEGIN_ARRAY";
            case P_END_ARRAY: return "END_ARRAY";
            case P_NAME: return "NAME";
            case P_NULL: return "NULL";
            case P_STRING: return "STRING";
            case P_BOOLEAN: return "BOOLEAN";
            case P_INT: return "INT";
            case P_LONG: return "LONG";
            case P_DOUBLE: return "DOUBLE";
            case P_OID: return "OID";
            case P_DATE: return "DATE";
            case P_BINARY: return "BINARY";
            case P_REGEX: return "REGEX";
            case P_CODE: return "CODE";
            case P_TIMESTAMP: return "TIMESTAMP";
            case P_MINKEY: return "MINKEY";
            case P_MAXKEY: return "MAXKEY";
            default: return Integer.toString(peeked);
        }
    }

    // ---- Streaming interface ----------------------------------------------------------------------------------------

    public boolean hasNext() throws IOException {
        int token = peek();
        return token != P_END_OBJECT && token != P_END_ARRAY;
    }

    public int peek() throws IOException {
        if (stack < 0) {
            return in != null? P_BEGIN_OBJECT: P_NONE; // boundaries
        }

        //noinspection StringEquality
        if (path[stack] == OBJECT) {
            if (peeked == P_NONE) {
                peeked = getType(readByte(), P_END_OBJECT); // read field type
            }
            return peeked != P_END_OBJECT? P_NAME: P_END_OBJECT;
        }

        //noinspection StringEquality
        if (path[stack] == ARRAY) {
            if (peeked == P_NONE) {
                peeked = getType(readByte(), P_END_ARRAY); // read field type
                if (peeked != P_END_ARRAY) {
                    push(readCString(), -1); // prefetch array index
                }
            }
            return peeked;
        }

        if (peeked == P_NONE) {
            throw new IllegalStateException("Field type expected: " + toString());
        }

        return peeked;
    }

    boolean isArray() {
        //noinspection StringEquality
        return stack > 0 && (path[stack] == ARRAY || path[stack - 1] == ARRAY);
    }

    /**
     * Parses BSON type
     */
    private static int getType(byte type, int eoo) throws IOException {
        switch (type) {
            case BSON.EOO: return eoo;
            case BSON.OBJECT: return P_BEGIN_OBJECT;
            case BSON.ARRAY: return P_BEGIN_ARRAY;
            case BSON.NULL: return P_NULL;
            case BSON.STRING: return P_STRING;
            case BSON.NUMBER: return P_DOUBLE;
            case BSON.NUMBER_INT: return P_INT;
            case BSON.NUMBER_LONG: return P_LONG;
            case BSON.BOOLEAN: return P_BOOLEAN;
            case BSON.OID: return P_OID;
            case BSON.DATE: return P_DATE;
            case BSON.BINARY: return P_BINARY;
            case BSON.REGEX: return P_REGEX;
            case BSON.CODE: return P_CODE;
            case BSON.SYMBOL: return P_STRING;
            case BSON.TIMESTAMP: return P_TIMESTAMP;
            case BSON.MINKEY: return P_MINKEY;
            case BSON.MAXKEY: return P_MAXKEY;
            default: throw new IllegalStateException("Unsupported BSON type: " + type);
        }
    }

    /**
     * Pushes path item to stack and initialize limiter
     */
    private void push(String item, int size) {
        if (++stack == path.length) {
            int length = path.length * 3 / 2 + 1;
            path = Arrays.copyOf(path, length);
            limiters = Arrays.copyOf(limiters, length);
        }

        if (size < 0) { // copy limiter from parent
            limiters[stack] = limiters[stack - 1];
        } else {
            limiters[stack] = pos + size;
        }

        path[stack] = item;
    }

    /**
     * Pops top path item from stack and returns previous limiter pos
     */
    private int pop() {
        peeked = P_NONE;
        path[stack] = null;
        return limiters[stack--];
    }

    public void beginObject() throws IOException {
        if (peek() != P_BEGIN_OBJECT) {
            throw new IllegalStateException("Expected Object but was " + toString());
        }
        int limiter = readInt() - 4; // 4 bytes already read
        push(OBJECT, limiter);
        peeked = P_NONE;
    }

    public void endObject() throws IOException {
        if (peek() != P_END_OBJECT) {
            throw new IllegalStateException("Expected EOO but was " + toString());
        }
        int limiter = pop();
        if (pos != limiter) {
            throw new IllegalStateException("Wrong Object bounds at " + pos + ", expected " + limiter + ":" + toString());
        }
        if (stack < 0) {
            reset(null); // stream finished
        } else {
            pop(); // finish field
        }
    }

    public void beginArray() throws IOException {
        if (peek() != P_BEGIN_ARRAY) {
            throw new IllegalStateException("Expected Array but was " + toString());
        }
        int limiter = readInt() - 4; // 4 bytes already read
        push(ARRAY, limiter);
        peeked = P_NONE;
    }

    public void endArray() throws IOException {
        if (peek() != P_END_ARRAY) {
            throw new IllegalStateException("Expected EOO but was " + toString());
        }
        int limiter = pop();
        if (pos != limiter) {
            throw new IllegalStateException("Wrong Object bounds at " + pos + ", expected " + limiter + ":" + toString());
        }
        pop(); // finish field
    }

    public String nextName() throws IOException {
        if (peek() != P_NAME) {
            throw new IllegalStateException("Expected Name but was " + toString());
        }
        String field = readCString();
        push(field, -1);
        return field;
    }

    public void nextNull() throws IOException {
        if (peek() != P_NULL) {
            throw new IllegalStateException("Expected Null but was " + toString());
        }
        pop(); // finish field
    }

    public String nextString() throws IOException {
        if (peek() != P_STRING) {
            throw new IllegalStateException("Expected String but was " + toString());
        }

        String s = readString();

        pop(); // finish field

        return s;
    }

    public boolean nextBoolean() throws IOException {
        if (peek() != P_BOOLEAN) {
            throw new IllegalStateException("Expected Boolean but was " + toString());
        }

        boolean b = readByte() > 0;

        pop(); // finish field

        return b;
    }

    public double nextDouble() throws IOException {
        if (peek() != P_DOUBLE) {
            throw new IllegalStateException("Expected Double but was " + toString());
        }

        long l = readLong();

        pop(); // finish field

        return Double.longBitsToDouble(l);
    }

    public int nextInt() throws IOException {
        if (peek() != P_INT) {
            throw new IllegalStateException("Expected Integer but was " + toString());
        }
        int i = readInt();

        pop(); // finish field

        return i;
    }

    public long nextLong() throws IOException {
        if (peek() != P_LONG) {
            throw new IllegalStateException("Expected Long but was " + toString());
        }

        long l = readLong();

        pop(); // finish field

        return l;
    }

    public ObjectId nextObjectId() throws IOException {
        if (peek() != P_OID) {
            throw new IllegalStateException("Expected ObjectId but was " + toString());
        }

        byte[] b = readBytes(12);

        pop(); // finish field

        return new ObjectId(b);
    }

    public Date nextDate() throws IOException {
        if (peek() != P_DATE) {
            throw new IllegalStateException("Expected Date but was " + toString());
        }

        long l = readLong();

        pop(); // finish field

        return new Date(l);
    }

    public UUID nextUUID() throws IOException {
        if (peek() != P_BINARY) {
            throw new IllegalStateException("Expected Binary but was " + toString());
        }

        int length = readInt();
        if (length != 16) {
            throw new IllegalStateException("Invalid UUID length, expected 16 but was " + length + " " + toString());
        }

        byte subtype = readByte();
        if (subtype != 0x03 && subtype != 0x04) {
            throw new IllegalStateException("Invalid UUID subtype, expected 0x04 but was " + subtype + " " + toString());
        }

        long mostSigBits = readLong();
        long leastSigBits = readLong();

        pop(); // finish field

        return new UUID(mostSigBits, leastSigBits);
    }

    public Binary nextBinary() throws IOException {
        if (peek() != P_BINARY) {
            throw new IllegalStateException("Expected Binary but was " + toString());
        }

        int length = readInt();
        byte type = readByte();
        byte[] data = readBytes(length);

        pop(); // finish field

        return new Binary(type, data);
    }

    public Pattern nextRegex() throws IOException {
        if (peek() != P_REGEX) {
            throw new IllegalStateException("Expected Regex but was " + toString());
        }

        String pattern = readCString();
        String flags = readCString();

        pop(); // finish field

        //noinspection MagicConstant
        return Pattern.compile(pattern, BSON.regexFlags(flags));
    }

    public Code nextCode() throws IOException {
        if (peek() != P_CODE) {
            throw new IllegalStateException("Expected Code but was " + toString());
        }

        String code = readString();

        pop(); // finish field

        return new Code(code);
    }

    public BSONTimestamp nextTimestamp() throws IOException {
        if (peek() != P_TIMESTAMP) {
            throw new IllegalStateException("Expected Timestamp but was " + toString());
        }
        int inc = readInt();
        int time = readInt();

        pop(); // finish field

        return new BSONTimestamp(time, inc);
    }

    public void nextMinKey() throws IOException {
        if (peek() != P_MINKEY) {
            throw new IllegalStateException("Expected MinKey but was " + toString());
        }

        pop(); // finish field
    }

    public void nextMaxKey() throws IOException {
        if (peek() != P_MAXKEY) {
            throw new IllegalStateException("Expected MaxKey but was " + toString());
        }

        pop(); // finish field
    }

    public void skipValue() throws IOException {
        switch (peek()) {
            case P_BEGIN_OBJECT:
            case P_BEGIN_ARRAY:
                skipBytes(readInt() - 4); // 4 bytes already read
                pop(); // finish document
                break;

            case P_NAME:
                nextName();
                break;

            case P_BOOLEAN:
                skipBytes(1);
                pop(); // finish field
                break;

            case P_INT:
                skipBytes(4);
                pop(); // finish field
                break;

            case P_LONG:
            case P_DOUBLE:
            case P_DATE:
            case P_TIMESTAMP:
                skipBytes(8);
                pop(); // finish field
                break;

            case P_CODE:
            case P_STRING:
                skipBytes(readInt());
                pop(); // finish field
                break;

            case P_BINARY:
                int length = readInt();
                readByte(); // subtype
                skipBytes(length);
                pop(); // finish field
                break;

            case P_OID:
                skipBytes(12);
                pop(); // finish field
                break;

            case P_REGEX:
                readCString();
                readCString();
                pop(); // finish field
                break;

            case P_NULL:
            case P_MINKEY:
            case P_MAXKEY:
                pop(); // finish field
                break;

            default:
                throw new IllegalStateException("Expected Value but was " + toString());
        }
    }

    // ---- Low level routines -----------------------------------------------------------------------------------------

    private byte readByte() throws IOException {
        ensure(1);
        int b = in.read();
        if (b < 0) {
            throw new EOFException("Unexpected end of stream: " + toString());
        }
        pos += 1;
        return (byte) (b & 0xFF);
    }

    private int readInt() throws IOException {
        ensure(4);
        int i = Bits.readInt(in, buf);
        pos += 4;
        return i;
    }

    private long readLong() throws IOException {
        ensure(8);
        long l = Bits.readLong(in, buf);
        pos += 8;
        return l;
    }

    @VisibleForTesting
    String readString() throws IOException {
        int len = readInt();
        ensure(len);
        Bits.readFully(in, buf, len);
        pos += len;
        if (buf[len - 1] != 0) {
            throw new IllegalStateException("Invalid string value, no trailing zero: " + toString());
        }
        return parseUTF8(buf, len - 1);
    }

    @VisibleForTesting
    String readCString() throws IOException {
        for (int len = 0; true; len++) {
            if (len == buf.length) {
                buf = Arrays.copyOf(buf, buf.length * 3 / 2);
            }
            if ((buf[len] = readByte()) == 0) {
                return parseUTF8(buf, len);
            }
        }
    }

    private byte[] readBytes(int length) throws IOException {
        if (stack >= 0 && pos + length > limiters[stack]) { // check limits
            throw new IllegalStateException("Trying to read data beyond document size: " + toString());
        }
        byte[] buf = new byte[length];
        Bits.readFully(in, buf);
        pos += length;
        return buf;
    }

    private void skipBytes(int length) throws IOException {
        if (stack >= 0 && pos + length > limiters[stack]) { // check limits
            throw new IllegalStateException("Trying to read data beyond document size: " + toString());
        }
        int toSkip = length;
        while (toSkip > 0) {
            long skiped = in.skip(toSkip);
            if (skiped < 0) {
                throw new EOFException();
            }
            pos += skiped;
            toSkip -= skiped;
        }
    }

    private void ensure(int length) {
        if (stack >= 0 && pos + length > limiters[stack]) { // check limits
            throw new IllegalStateException("Trying to read data beyond document size: " + toString());
        }
        if (buf.length < length) {
            buf = new byte[length];
        }
    }

    private String parseUTF8(byte[] buf, int len) {
        sb.setLength(0);
        for (int i = 0; i < len; ) {
            int cp = buf[i++];
            if ((cp & 0x80) == 0) { // single byte
                sb.append((char) (cp & 0x7F));
            } else if ((cp & 0xE0) == 0xC0) { // two bytes
                sb.append((char) ((cp & 0x1F) << 6 | checkUTF8(buf, len, i++)));
            } else if ((cp & 0xF0) == 0xE0) { // three bytes
                sb.appendCodePoint((cp & 0x0F) << 12 | checkUTF8(buf, len, i++) << 6 | checkUTF8(buf, len, i++));
            } else if ((cp & 0xF8) == 0xF0) { // four bytes
                sb.appendCodePoint((cp & 0x07) << 18 | checkUTF8(buf, len, i++) << 12 | checkUTF8(buf, len, i++) << 6 | checkUTF8(buf, len, i++));
            } else {
                throw new IllegalArgumentException("Invalid UTF8 string: " + toString());
            }
        }
        return sb.toString();
    }

    private int checkUTF8(byte[] buf, int len, int i) {
        if (i < len) {
            int c = buf[i];
            if ((c & 0xC0) == 0x80) {
                return c & 0x3F;
            }
        }
        throw new IllegalArgumentException("Invalid UTF8 string: " + toString());
    }
}
