/*
 * Copyright (c) 2008-2014 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import org.bson.BSON;
import org.bson.types.*;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

import static com.google.gson.stream.JsonToken.BEGIN_OBJECT;
import static com.google.gson.stream.JsonToken.NAME;
import static java.lang.String.format;

/**
 * Utility class which holds Mongo-specific {@link TypeAdapter}s.
 * <p>{@link Gson} object passed to the {@link GsonDBCollection} must be configured
 * using this adapters to preserve types within JSON to BSON convertions.</p>
 * <p>Use {@link #configure(GsonBuilder)} to install all adapters properly.</p>
 *
 * @see com.maxifier.mongo.GsonDBCollection
 * @author Konstantin Lyamshin (2014-12-08 21:08)
 */
public final class GsonAdapters {
    public static final String F_NULLABLE = "$null";
    public static final String F_LONG = "$numberLong";
    public static final String F_OBJECTID = "$oid";
    public static final String F_DATE = "$date";
    public static final String F_UUID = "$uuid";
    public static final String F_BINARY = "$binary";
    public static final String F_BINARY_TYPE = "$type";
    public static final String F_REGEX = "$regex";
    public static final String F_REGEX_OPTIONS = "$options";
    public static final String F_TIMESTAMP = "$timestamp";
    public static final String F_TIMESTAMP_T = "t";
    public static final String F_TIMESTAMP_I = "i";
    public static final String F_CODE = "$code";
//    public static final String F_CODE_SCOPE = "$scope"; TODO: hasn't implemented yet
    public static final String F_MINKEY = "$minKey";
    public static final String F_MAXKEY = "$maxKey";

    public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    private GsonAdapters() { }

    public static GsonBuilder configure(GsonBuilder builder) {
        return builder
            .registerTypeAdapter(ObjectId.class, OBJECTID_ADAPTER)
            .registerTypeAdapter(Long.class, LONG_ADAPTER)
            .registerTypeAdapter(long.class, LONG_ADAPTER)
            .registerTypeAdapter(Date.class, DATE_ADAPTER)
            .registerTypeAdapter(UUID.class, UUID_ADAPTER)
            .registerTypeAdapter(byte[].class, BYTES_ADAPTER)
            .registerTypeAdapter(Binary.class, BINARY_ADAPTER)
            .registerTypeAdapter(Pattern.class, REGEX_ADAPTER)
            .registerTypeAdapter(BSONTimestamp.class, TIMESTAMP_ADAPTER)
            .registerTypeAdapter(Code.class, CODE_ADAPTER)
            .registerTypeAdapter(MinKey.class, MINKEY_ADAPTER)
            .registerTypeAdapter(MaxKey.class, MAXKEY_ADAPTER)
            .registerTypeAdapterFactory(NULLABLE_ADAPTER_FACTORY);
    }

    public static final TypeAdapter<Long> LONG_ADAPTER = new TypeAdapter<Long>() {
        @Override
        public void write(JsonWriter out, Long value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.beginObject()
                    .name(F_LONG).value(value)
                    .endObject();
            }
        }

        @Override
        public Long read(JsonReader in) throws IOException {
            JsonToken peek = in.peek();
            if (peek == JsonToken.NULL) {
                in.nextNull();
                return null;
            }

            if (peek == JsonToken.NUMBER || peek == JsonToken.STRING) {
                return in.nextLong();
            }

            in.beginObject();
            String name = in.nextName();
            if (!name.equals(F_LONG)) {
                throw new JsonSyntaxException(format("Unexpected Long field %s at %s", name, in.getPath()));
            }
            Long value = in.nextLong();
            in.endObject();
            return value;
        }

        @Override
        public String toString() {
            return "LONG_ADAPTER";
        }
    };
    public static final TypeAdapter<ObjectId> OBJECTID_ADAPTER = new TypeAdapter<ObjectId>() {
        @Override
        public void write(JsonWriter out, ObjectId value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.beginObject()
                    .name(F_OBJECTID).value(value.toHexString())
                    .endObject();
            }
        }

        @Override
        public ObjectId read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }

            in.beginObject();
            String name = in.nextName();
            if (!name.equals(F_OBJECTID)) {
                throw new JsonSyntaxException(format("Unexpected ObjectId field %s at %s", name, in.getPath()));
            }
            ObjectId oid = new ObjectId(in.nextString());
            in.endObject();
            return oid;
        }

        @Override
        public String toString() {
            return "OBJECTID_ADAPTER";
        }
    };
    public static final TypeAdapter<Date> DATE_ADAPTER = new TypeAdapter<Date>() {
        private final ThreadLocal<DateFormat> cache = new ThreadLocal<DateFormat>();

        @Override
        public void write(JsonWriter out, Date value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else if (out instanceof GsonBuffer.JsonBufferedWriter) {
                // internally use value w/o conversions
                out.beginObject()
                    .name(F_DATE).value(value.getTime())
                    .endObject();
            } else {
                // externally use symbolic format
                out.beginObject()
                    .name(F_DATE).value(format(value))
                    .endObject();
            }
        }

        @Override
        public Date read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }

            in.beginObject();
            String name = in.nextName();
            if (!name.equals(F_DATE)) {
                throw new JsonSyntaxException(String.format("Unexpected Date field %s at %s", name, in.getPath()));
            }
            Date date = parse(in);
            in.endObject();
            return date;
        }

        private String format(Date value) {
            DateFormat format = cache.get();
            if (format == null) {
                format = new SimpleDateFormat(DATE_FORMAT, Locale.US);
                cache.set(format);
            }
            return format.format(value);
        }

        private Date parse(JsonReader in) throws IOException {
            String value = in.nextString();
            if (value.indexOf('T') < 0) {
                // looks like numeric timestamp
                try {
                    return new Date(Long.parseLong(value));
                } catch (NumberFormatException ignored) {
                }
            }

            // looks like symbolic timestamp
            DateFormat format = cache.get();
            if (format == null) {
                format = new SimpleDateFormat(DATE_FORMAT, Locale.US);
                cache.set(format);
            }
            ParsePosition pos = new ParsePosition(0);
            Date date = format.parse(value, pos);
            if (pos.getIndex() != value.length()) {
                throw new JsonSyntaxException(String.format("Invalid Date value '%s' at %d near to%s", value, pos.getErrorIndex(), in.getPath()));
            }
            return date;
        }

        @Override
        public String toString() {
            return "DATE_ADAPTER";
        }
    };
    public static final TypeAdapter<UUID> UUID_ADAPTER = new TypeAdapter<UUID>() {
        @Override
        public void write(JsonWriter out, UUID value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.beginObject()
                    .name(F_UUID).value(value.toString())
                    .endObject();
            }
        }

        @Override
        public UUID read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }

            in.beginObject();
            String name = in.nextName();
            if (!name.equals(F_UUID)) {
                throw new JsonSyntaxException(format("Unexpected UUID field %s at %s", name, in.getPath()));
            }
            UUID uuid = UUID.fromString(in.nextString());
            in.endObject();
            return uuid;
        }

        @Override
        public String toString() {
            return "UUID_ADAPTER";
        }
    };
    public static final TypeAdapter<byte[]> BYTES_ADAPTER = new TypeAdapter<byte[]>() {
        @Override
        public void write(JsonWriter out, byte[] value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.beginObject()
                    .name(F_BINARY).value(Base64Codec.encode(value))
                    .name(F_BINARY_TYPE).value(BSON.B_GENERAL)
                    .endObject();
            }
        }

        @Override
        public byte[] read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }

            in.beginObject();
            String name = in.nextName();
            if (!name.equals(F_BINARY)) {
                throw new JsonSyntaxException(format("Unexpected Binary field %s at %s", name, in.getPath()));
            }
            byte[] bytes = Base64Codec.decode(in.nextString());
            if (in.peek() == NAME) {
                name = in.nextName();
                if (!name.equals(F_BINARY_TYPE)) {
                    throw new JsonSyntaxException(format("Unexpected Binary field %s at %s", name, in.getPath()));
                }
                in.skipValue(); // skip type
            }
            in.endObject();
            return bytes;
        }

        @Override
        public String toString() {
            return "BYTES_ADAPTER";
        }
    };
    public static final TypeAdapter<Binary> BINARY_ADAPTER = new TypeAdapter<Binary>() {
        @Override
        public void write(JsonWriter out, Binary value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.beginObject()
                    .name(F_BINARY).value(Base64Codec.encode(value.getData()))
                    .name(F_BINARY_TYPE).value(value.getType())
                    .endObject();
            }
        }

        @Override
        public Binary read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }

            in.beginObject();
            String name = in.nextName();
            if (!name.equals(F_BINARY)) {
                throw new JsonSyntaxException(format("Unexpected Binary field %s at %s", name, in.getPath()));
            }
            byte[] bytes = Base64Codec.decode(in.nextString());
            int type = BSON.B_GENERAL;
            if (in.peek() == NAME) {
                name = in.nextName();
                if (!name.equals(F_BINARY_TYPE)) {
                    throw new JsonSyntaxException(format("Unexpected Binary field %s at %s", name, in.getPath()));
                }
                type = in.nextInt();
            }
            in.endObject();
            return new Binary((byte) type, bytes);
        }

        @Override
        public String toString() {
            return "BINARY_ADAPTER";
        }
    };
    public static final TypeAdapter<Pattern> REGEX_ADAPTER = new TypeAdapter<Pattern>() {
        @Override
        public void write(JsonWriter out, Pattern value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.beginObject();
                out.name(F_REGEX).value(value.pattern());
                if (value.flags() != 0) {
                    out.name(F_REGEX_OPTIONS).value(BSON.regexFlags(value.flags()));
                }
                out.endObject();
            }
        }

        @Override
        public Pattern read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }

            in.beginObject();
            String name = in.nextName();
            if (!name.equals(F_REGEX)) {
                throw new JsonSyntaxException(format("Unexpected Regex field %s at %s", name, in.getPath()));
            }
            String regex = in.nextString();
            int flags = 0;
            if (in.peek() == NAME) {
                name = in.nextName();
                if (!name.equals(F_REGEX_OPTIONS)) {
                    throw new JsonSyntaxException(format("Unexpected Regex field %s at %s", name, in.getPath()));
                }
                flags = BSON.regexFlags(in.nextString());
            }
            in.endObject();
            //noinspection MagicConstant
            return Pattern.compile(regex, flags);
        }

        @Override
        public String toString() {
            return "REGEX_ADAPTER";
        }
    };
    public static final TypeAdapter<BSONTimestamp> TIMESTAMP_ADAPTER = new TypeAdapter<BSONTimestamp>() {
        @Override
        public void write(JsonWriter out, BSONTimestamp value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.beginObject().name(F_TIMESTAMP);
                out.beginObject()
                    .name(F_TIMESTAMP_T).value(value.getTime())
                    .name(F_TIMESTAMP_I).value(value.getInc())
                    .endObject();
                out.endObject();
            }
        }

        @Override
        public BSONTimestamp read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }

            in.beginObject();
            String name = in.nextName();
            if (!name.equals(F_TIMESTAMP)) {
                throw new JsonSyntaxException(format("Unexpected Timestamp field %s at %s", name, in.getPath()));
            }
            in.beginObject();

            name = in.nextName();
            if (!name.equals(F_TIMESTAMP_T)) {
                throw new JsonSyntaxException(format("Unexpected Timestamp field %s at %s", name, in.getPath()));
            }
            int t = in.nextInt();

            name = in.nextName();
            if (!name.equals(F_TIMESTAMP_I)) {
                throw new JsonSyntaxException(format("Unexpected Timestamp field %s at %s", name, in.getPath()));
            }
            int i = in.nextInt();

            in.endObject();
            in.endObject();

            return new BSONTimestamp(t, i);
        }

        @Override
        public String toString() {
            return "TIMESTAMP_ADAPTER";
        }
    };
    public static final TypeAdapter<Code> CODE_ADAPTER = new TypeAdapter<Code>() {
        @Override
        public void write(JsonWriter out, Code value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                if (value instanceof CodeWScope) {
                    throw new UnsupportedOperationException("CodeWScope isn't supported yet");
                }
                out.beginObject();
                out.name(F_CODE).value(value.getCode());
                out.endObject();
            }
        }

        @Override
        public Code read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }

            in.beginObject();
            String name = in.nextName();
            if (!name.equals(F_CODE)) {
                throw new JsonSyntaxException(format("Unexpected Code field %s at %s", name, in.getPath()));
            }
            Code code = new Code(in.nextString());
            in.endObject();
            return code;
        }

        @Override
        public String toString() {
            return "CODE_ADAPTER";
        }
    };
    public static final TypeAdapter<MinKey> MINKEY_ADAPTER = new TypeAdapter<MinKey>() {
        @Override
        public void write(JsonWriter out, MinKey value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.beginObject()
                    .name(F_MINKEY).value(1)
                    .endObject();
            }
        }

        @Override
        public MinKey read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }

            in.beginObject();
            String name = in.nextName();
            if (!name.equals(F_MINKEY)) {
                throw new JsonSyntaxException(format("Unexpected MinKey field %s at %s", name, in.getPath()));
            }
            in.skipValue();
            in.endObject();
            return new MinKey();
        }

        @Override
        public String toString() {
            return "MINKEY_ADAPTER";
        }
    };
    public static final TypeAdapter<MaxKey> MAXKEY_ADAPTER = new TypeAdapter<MaxKey>() {
        @Override
        public void write(JsonWriter out, MaxKey value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.beginObject()
                    .name(F_MAXKEY).value(1)
                    .endObject();
            }
        }

        @Override
        public MaxKey read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }

            in.beginObject();
            String name = in.nextName();
            if (!name.equals(F_MAXKEY)) {
                throw new JsonSyntaxException(format("Unexpected ObjectId field %s at %s", name, in.getPath()));
            }
            in.skipValue();
            in.endObject();
            return new MaxKey();
        }

        @Override
        public String toString() {
            return "MAXKEY_ADAPTER";
        }
    };
    public static final TypeAdapterFactory NULLABLE_ADAPTER_FACTORY = new TypeAdapterFactory() {
        @Override
        @SuppressWarnings("unchecked") // checked by reflection
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            if (GsonNullable.class.isAssignableFrom(type.getRawType())) {
                if (!(type.getType() instanceof ParameterizedType)) {
                    throw new IllegalArgumentException("Type parameter not specified for GsonNullable type " + type);
                }
                Type nullableType = ((ParameterizedType) type.getType()).getActualTypeArguments()[0];
                TypeAdapter<?> delegate = gson.getAdapter(TypeToken.get(nullableType));
                return new GsonNullableTypeAdapter(delegate);
            }
            return null;
        }

        @Override
        public String toString() {
            return "NULLABLE_ADAPTER_FACTORY";
        }
    };

    @SuppressWarnings("unchecked")
    static final TypeAdapter<GsonNullable<?>> NULLABLE_DUMMY_ADAPTER = new GsonNullableTypeAdapter(null);

    public static class GsonNullableTypeAdapter<T> extends TypeAdapter<GsonNullable<T>> {
        private final TypeAdapter<T> delegate;

        public GsonNullableTypeAdapter(TypeAdapter<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(JsonWriter out, GsonNullable<T> value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else if (value.isPresent()) {
                delegate.write(out, value.get());
            } else {
                out.beginObject()
                    .name(F_NULLABLE).value(1)
                    .endObject();
            }
        }

        @Override
        public GsonNullable<T> read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return GsonNullable.absent();
            }

            if (in.peek() != BEGIN_OBJECT) { // Null pattern not found
                return GsonNullable.fromNullable(delegate.read(in));
            }

            if (in instanceof GsonBuffer.Prefetchable) {
                GsonBuffer.Prefetchable prefetcher = (GsonBuffer.Prefetchable) in;
                if (F_NULLABLE.equals(prefetcher.peekObjectField())) {
                    return readNull(in); // Null pattern found
                }
                return GsonNullable.fromNullable(delegate.read(in));
            }

            // Creates Prefetcher manually
            GsonBuffer.PrefetchableReader prefetcher = new GsonBuffer.PrefetchableReader(in);
            if (F_NULLABLE.equals(prefetcher.peekObjectField())) {
                return readNull(prefetcher); // Null pattern found
            }
            return GsonNullable.fromNullable(delegate.read(prefetcher));
        }

        private GsonNullable<T> readNull(JsonReader in) throws IOException {
            in.beginObject();
            String name = in.nextName();
            if (!name.equals(F_NULLABLE)) {
                throw new IllegalStateException(format("Unexpected Nullable field %s at %s", name, in.getPath()));
            }
            in.skipValue();
            in.endObject();
            return GsonNullable.absent();
        }

        @Override
        public String toString() {
            return "GsonNullable{for=" + delegate + "}";
        }
    }

    /**
     * Utility class which helps to parse {@link JsonReader} streams.
     * <p>Replacement for Java7 string switch syntax.</p>
     * <p>Usage example:
     * <pre>
     * String field1 = null;
     * Integer field2 = null;
     * Double field3 = null;
     * FieldMatcher m = new FieldMatcher("field1", "field2", "field3"); // natural field order
     * in.beginObject();
     * while (in.hasNext()) {
     *     switch (m.match(in.nextName())) {
     *         case 0: field1 = in.nextString(); break;
     *         case 1: field2 = in.nextInt(); break;
     *         case 2: field3 = in.nextDouble(); break;
     *         default: in.skipValue();
     *     }
     * }
     * in.endObject();
     * </pre></p>
     */
    public static class FieldMatcher {
        private final String[] fields;
        private int start;

        public FieldMatcher(String... fields) {
            this.fields = fields;
        }

        public int match(String field) throws IOException {
            int i = start;
            do {
                int p = i++;
                if (i == fields.length) {
                    i = 0; // loop indices
                }
                if (fields[p].equals(field)) {
                    start = i;
                    return p;
                }
            } while (i != start);

            // not found
            return -1;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < fields.length; i++) {
                sb.append(',').append(fields[i]);
                if (start == i) {
                    sb.append('*');
                }
            }
            return '[' + sb.substring(1) + ']';
        }
    }

    /**
     * <p>Provides Base64 encoding and decoding.</p>
     * <p>This class implements Base64 encoding</p>
     * <p>Thanks to Apache Commons project. This class refactored from org.apache.commons.codec.binary</p>
     * <p>Original Thanks to <a href="http://svn.apache.org/repos/asf/webservices/commons/trunk/modules/util/">commons</a> project in
     * ws.apache.org for this code. </p>
     */
    static final class Base64Codec {

        private static final int BYTES_PER_UNENCODED_BLOCK = 3;
        private static final int BYTES_PER_ENCODED_BLOCK = 4;

        /**
         * Mask used to extract 6 bits, used when encoding
         */
        private static final int SixBitMask = 0x3f;

        /**
         * padding char
         */
        private static final byte PAD = '=';

        /**
         * This array is a lookup table that translates 6-bit positive integer index values into their "Base64 Alphabet"
         * equivalents as specified in Table 1 of RFC 2045.
         */
        private static final byte[] EncodeTable = {'A', 'B', 'C', 'D', 'E', 'F',
            'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
            'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
            'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
            't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', '+', '/'};

        private static final int[] DecodeTable = new int[128];

        static {
            for (int i = 0; i < EncodeTable.length; i++) {
                DecodeTable[EncodeTable[i]] = i;
            }
        }

        private Base64Codec() {
        }

        /**
         * Translates the specified Base64 string into a byte array.
         *
         * @param s the Base64 string (not null)
         * @return the byte array (not null)
         */
        public static byte[] decode(String s) {
            int delta = s.endsWith("==") ? 2 : s.endsWith("=") ? 1 : 0;
            byte[] buffer = new byte[s.length() * BYTES_PER_UNENCODED_BLOCK / BYTES_PER_ENCODED_BLOCK - delta];
            int mask = 0xFF;
            int pos = 0;
            for (int i = 0; i < s.length(); i += BYTES_PER_ENCODED_BLOCK) {
                int c0 = DecodeTable[s.charAt(i)];
                int c1 = DecodeTable[s.charAt(i + 1)];
                buffer[pos++] = (byte) (((c0 << 2) | (c1 >> 4)) & mask);
                if (pos >= buffer.length) {
                    return buffer;
                }
                int c2 = DecodeTable[s.charAt(i + 2)];
                buffer[pos++] = (byte) (((c1 << 4) | (c2 >> 2)) & mask);
                if (pos >= buffer.length) {
                    return buffer;
                }
                int c3 = DecodeTable[s.charAt(i + 3)];
                buffer[pos++] = (byte) (((c2 << 6) | c3) & mask);
            }
            return buffer;
        }

        /**
         * Translates the specified byte array into Base64 string.
         *
         * @param in the byte array (not null)
         * @return the translated Base64 string (not null)
         */
        public static String encode(byte[] in) {

            int modulus = 0;
            int bitWorkArea = 0;
            int numEncodedBytes = (in.length / BYTES_PER_UNENCODED_BLOCK) * BYTES_PER_ENCODED_BLOCK
                + ((in.length % BYTES_PER_UNENCODED_BLOCK == 0) ? 0 : 4);

            byte[] buffer = new byte[numEncodedBytes];
            int pos = 0;

            for (int b : in) {
                modulus = (modulus + 1) % BYTES_PER_UNENCODED_BLOCK;

                if (b < 0)
                    b += 256;

                bitWorkArea = (bitWorkArea << 8) + b; //  BITS_PER_BYTE
                if (0 == modulus) { // 3 bytes = 24 bits = 4 * 6 bits to extract
                    buffer[pos++] = EncodeTable[(bitWorkArea >> 18) & SixBitMask];
                    buffer[pos++] = EncodeTable[(bitWorkArea >> 12) & SixBitMask];
                    buffer[pos++] = EncodeTable[(bitWorkArea >> 6) & SixBitMask];
                    buffer[pos++] = EncodeTable[bitWorkArea & SixBitMask];
                }
            }

            switch (modulus) { // 0-2
                case 1: // 8 bits = 6 + 2
                    buffer[pos++] = EncodeTable[(bitWorkArea >> 2) & SixBitMask]; // top 6 bits
                    buffer[pos++] = EncodeTable[(bitWorkArea << 4) & SixBitMask]; // remaining 2
                    buffer[pos++] = PAD;
                    buffer[pos] = PAD; // Last entry no need to ++
                    break;

                case 2: // 16 bits = 6 + 6 + 4
                    buffer[pos++] = EncodeTable[(bitWorkArea >> 10) & SixBitMask];
                    buffer[pos++] = EncodeTable[(bitWorkArea >> 4) & SixBitMask];
                    buffer[pos++] = EncodeTable[(bitWorkArea << 2) & SixBitMask];
                    buffer[pos] = PAD; // Last entry no need to ++
                    break;
            }

            return new String(buffer);
        }
    }
}
