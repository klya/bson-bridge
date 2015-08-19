/*
 * Copyright (c) 2008-2014 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.google.gson.*;
import com.google.gson.internal.bind.TypeAdapters;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.mongodb.BasicDBObject;
import org.bson.types.*;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.StringReader;
import java.io.Writer;
import java.lang.reflect.ParameterizedType;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import static com.maxifier.mongo.GsonAdapters.*;

/**
 * @author Konstantin Lyamshin (2014-12-08 21:48)
 */
public class GsonAdaptersTest extends org.testng.Assert {
    private Gson gson;

    @BeforeClass
    public void setUp() throws Exception {
        gson = configure(new GsonBuilder()).create();
    }

    @Test
    public void testObjectId() throws Exception {
        TypeAdapter<ObjectId> adpt = OBJECTID_ADAPTER;
        ObjectId oid = new ObjectId("CAFEBABE0000000000000000");

        // serializing
        JsonObject tree = adpt.toJsonTree(oid).getAsJsonObject();
        assertEquals(tree.entrySet().size(), 1);
        assertEquals(tree.getAsJsonPrimitive("$oid").getAsString(), oid.toHexString());
        assertTrue(adpt.toJsonTree(null).isJsonNull());

        // deserializing
        assertEquals(adpt.fromJsonTree(tree), oid);
        assertEquals(adpt.fromJsonTree(JsonNull.INSTANCE), null);

        // test gson configuration
        assertEquals(gson.toJsonTree(oid, ObjectId.class), tree);
        assertEquals(gson.fromJson(tree, ObjectId.class), oid);
    }

    @Test
    public void testLong() throws Exception {
        TypeAdapter<Long> adpt = LONG_ADAPTER;
        Long l = 0xCAFEBABECAFEBABEL;

        // serializing
        JsonObject tree = adpt.toJsonTree(l).getAsJsonObject();
        assertEquals(tree.entrySet().size(), 1);
        assertEquals(tree.getAsJsonPrimitive("$numberLong").getAsLong(), l.longValue());
        assertTrue(adpt.toJsonTree(null).isJsonNull());

        // deserializing
        assertEquals(adpt.fromJsonTree(tree), l);
        assertEquals(adpt.fromJsonTree(JsonNull.INSTANCE), null);

        // test gson configuration
        assertEquals(gson.toJsonTree(l, Long.class), tree);
        assertEquals(gson.toJsonTree(l, Long.TYPE), tree);
        assertEquals(gson.fromJson(tree, Long.class), l);
        assertEquals(gson.fromJson(tree, long.class), l);
    }

    @Test
    public void testDate() throws Exception {
        DateFormat format = new SimpleDateFormat(DATE_FORMAT, Locale.US);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getDefault());
        calendar.set(2014, Calendar.DECEMBER, 9, 11, 30, 17);
        calendar.set(Calendar.MILLISECOND, 28);
        Date dt = new Date(calendar.getTimeInMillis());

        TypeAdapter<Date> adpt = DATE_ADAPTER;

        // serializing symbolic
        JsonObject treeS = adpt.toJsonTree(dt).getAsJsonObject();
        assertEquals(treeS.entrySet().size(), 1);
        assertEquals(treeS.getAsJsonPrimitive("$date").getAsString(), format.format(dt));

        // serializing number (detected automatically)
        GsonBuffer buffer = new GsonBuffer();
        adpt.write(buffer.writer(), dt);
        JsonObject treeN = new JsonParser().parse(buffer.reader()).getAsJsonObject();
        assertEquals(treeN.entrySet().size(), 1);
        assertEquals(treeN.getAsJsonPrimitive("$date").getAsLong(), dt.getTime());

        assertTrue(adpt.toJsonTree(null).isJsonNull());

        // deserializing
        assertEquals(adpt.fromJsonTree(treeN), dt);
        assertEquals(adpt.fromJsonTree(treeS), dt);
        assertEquals(adpt.fromJsonTree(JsonNull.INSTANCE), null);

        // test gson configuration
        assertEquals(gson.toJsonTree(dt, Date.class), treeS);
        assertEquals(gson.fromJson(treeS, Date.class), dt);
        assertEquals(gson.fromJson(treeN, Date.class), dt);
    }

    @Test
    public void testUUID() throws Exception {
        TypeAdapter<UUID> adpt = UUID_ADAPTER;
        UUID uuid = UUID.fromString("cafebabe-0000-0000-0000-000000000000");

        // serializing
        JsonObject tree = adpt.toJsonTree(uuid).getAsJsonObject();
        assertEquals(tree.entrySet().size(), 1);
        assertEquals(tree.getAsJsonPrimitive("$uuid").getAsString(), uuid.toString());
        assertTrue(adpt.toJsonTree(null).isJsonNull());

        // deserializing
        assertEquals(adpt.fromJsonTree(tree), uuid);
        assertEquals(adpt.fromJsonTree(JsonNull.INSTANCE), null);

        // test gson configuration
        assertEquals(gson.toJsonTree(uuid, UUID.class), tree);
        assertEquals(gson.fromJson(tree, UUID.class), uuid);
    }

    @Test
    public void testMinKey() throws Exception {
        TypeAdapter<MinKey> adpt = MINKEY_ADAPTER;
        MinKey key = new MinKey();

        // serializing
        JsonObject tree = adpt.toJsonTree(key).getAsJsonObject();
        assertEquals(tree.entrySet().size(), 1);
        assertEquals(tree.getAsJsonPrimitive("$minKey").getAsInt(), 1);
        assertTrue(adpt.toJsonTree(null).isJsonNull());

        // deserializing
        assertEquals(adpt.fromJsonTree(tree), key);
        assertEquals(adpt.fromJsonTree(JsonNull.INSTANCE), null);

        // test gson configuration
        assertEquals(gson.toJsonTree(key, MinKey.class), tree);
        assertEquals(gson.fromJson(tree, MinKey.class), key);
    }

    @Test
    public void testMaxKey() throws Exception {
        TypeAdapter<MaxKey> adpt = MAXKEY_ADAPTER;
        MaxKey key = new MaxKey();

        // serializing
        JsonObject tree = adpt.toJsonTree(key).getAsJsonObject();
        assertEquals(tree.entrySet().size(), 1);
        assertEquals(tree.getAsJsonPrimitive("$maxKey").getAsInt(), 1);
        assertTrue(adpt.toJsonTree(null).isJsonNull());

        // deserializing
        assertEquals(adpt.fromJsonTree(tree), key);
        assertEquals(adpt.fromJsonTree(JsonNull.INSTANCE), null);

        // test gson configuration
        assertEquals(gson.toJsonTree(key, MaxKey.class), tree);
        assertEquals(gson.fromJson(tree, MaxKey.class), key);
    }

    @Test
    public void testBinary() throws Exception {
        TypeAdapter<Binary> adpt = BINARY_ADAPTER;
        Binary bn = new Binary("gson2mongo".getBytes());

        // serializing
        JsonObject tree = adpt.toJsonTree(bn).getAsJsonObject();
        assertEquals(tree.entrySet().size(), 2);
        assertEquals(tree.getAsJsonPrimitive("$binary").getAsString(), "Z3NvbjJtb25nbw==");
        assertEquals(tree.getAsJsonPrimitive("$type").getAsInt(), 0);
        assertTrue(adpt.toJsonTree(null).isJsonNull());

        // deserializing
        assertEquals(adpt.fromJsonTree(tree), bn);
        assertEquals(adpt.fromJsonTree(JsonNull.INSTANCE), null);

        // test gson configuration
        assertEquals(gson.toJsonTree(bn, Binary.class), tree);
        assertEquals(gson.fromJson(tree, Binary.class), bn);
    }

    @Test
    public void testBytes() throws Exception {
        TypeAdapter<byte[]> adpt = BYTES_ADAPTER;
        byte[] buf = "gson2mongo".getBytes();

        // serializing
        JsonObject tree = adpt.toJsonTree(buf).getAsJsonObject();
        assertEquals(tree.entrySet().size(), 2);
        assertEquals(tree.getAsJsonPrimitive("$binary").getAsString(), "Z3NvbjJtb25nbw==");
        assertEquals(tree.getAsJsonPrimitive("$type").getAsInt(), 0);
        assertTrue(adpt.toJsonTree(null).isJsonNull());

        // deserializing
        assertEquals(adpt.fromJsonTree(tree), buf);
        assertEquals(adpt.fromJsonTree(JsonNull.INSTANCE), null);

        // test gson configuration
        assertEquals(gson.toJsonTree(buf, byte[].class), tree);
        assertEquals(gson.fromJson(tree, byte[].class), buf);
    }

    @Test
    public void testPattern() throws Exception {
        TypeAdapter<Pattern> adpt = REGEX_ADAPTER;
        Pattern pt0 = Pattern.compile("\\w+.*");
        Pattern pt1 = Pattern.compile("\\d+-\\w*", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

        // serializing
        JsonObject tree0 = adpt.toJsonTree(pt0).getAsJsonObject();
        JsonObject tree1 = adpt.toJsonTree(pt1).getAsJsonObject();
        assertEquals(tree0.entrySet().size(), 1);
        assertEquals(tree0.getAsJsonPrimitive("$regex").getAsString(), "\\w+.*");
        assertEquals(tree1.entrySet().size(), 2);
        assertEquals(tree1.getAsJsonPrimitive("$regex").getAsString(), "\\d+-\\w*");
        assertEquals(tree1.getAsJsonPrimitive("$options").getAsString(), "im");
        assertTrue(adpt.toJsonTree(null).isJsonNull());

        // deserializing
        assertEquals(adpt.fromJsonTree(tree0).pattern(), pt0.pattern());
        assertEquals(adpt.fromJsonTree(tree0).flags(), pt0.flags());
        assertEquals(adpt.fromJsonTree(tree1).pattern(), pt1.pattern());
        assertEquals(adpt.fromJsonTree(tree1).flags(), pt1.flags());
        assertEquals(adpt.fromJsonTree(JsonNull.INSTANCE), null);

        // test gson configuration
        assertEquals(gson.toJsonTree(pt0, Pattern.class), tree0);
        assertEquals(gson.fromJson(tree0, Pattern.class).pattern(), pt0.pattern());
    }

    @Test
    public void testCode() throws Exception {
        TypeAdapter<Code> adpt = CODE_ADAPTER;
        Code code = new Code("emit(this._id)");

        // serializing
        JsonObject tree = adpt.toJsonTree(code).getAsJsonObject();
        assertEquals(tree.entrySet().size(), 1);
        assertEquals(tree.getAsJsonPrimitive("$code").getAsString(), code.getCode());
        assertTrue(adpt.toJsonTree(null).isJsonNull());

        // deserializing
        assertEquals(adpt.fromJsonTree(tree), code);
        assertEquals(adpt.fromJsonTree(JsonNull.INSTANCE), null);

        // Detect CodeWScope
        try {
            adpt.toJsonTree(new CodeWScope("", new BasicDBObject()));
            fail();
        } catch (UnsupportedOperationException ignored) {
        }

        // Don't skip scope
        JsonObject treeS = new JsonObject();
        treeS.add(F_CODE, new JsonPrimitive(""));
        treeS.add("$scope", new JsonPrimitive(""));
        try {
            adpt.fromJsonTree(treeS);
            fail();
        } catch (IllegalStateException ignored) {
        }

        // test gson configuration
        assertEquals(gson.toJsonTree(code, Code.class), tree);
        assertEquals(gson.fromJson(tree, Code.class), code);
    }

    @Test
    public void testBSONTimestamp() throws Exception {
        TypeAdapter<BSONTimestamp> adpt = TIMESTAMP_ADAPTER;
        BSONTimestamp ts = new BSONTimestamp(0xCAFEBABE, 17);

        // serializing
        JsonObject tree = adpt.toJsonTree(ts).getAsJsonObject();
        assertEquals(tree.entrySet().size(), 1);
        JsonObject t = tree.getAsJsonObject("$timestamp");
        assertEquals(t.entrySet().size(), 2);
        assertEquals(t.getAsJsonPrimitive("t").getAsInt(), ts.getTime());
        assertEquals(t.getAsJsonPrimitive("i").getAsInt(), ts.getInc());
        assertTrue(adpt.toJsonTree(null).isJsonNull());

        // deserializing
        assertEquals(adpt.fromJsonTree(tree), ts);
        assertEquals(adpt.fromJsonTree(JsonNull.INSTANCE), null);

        // test gson configuration
        assertEquals(gson.toJsonTree(ts, BSONTimestamp.class), tree);
        assertEquals(gson.fromJson(tree, BSONTimestamp.class), ts);
    }

    @Test
    public void testGetNullableType() throws Exception {
        TypeToken<GsonNullable<Writer>> expected1 = new TypeToken<GsonNullable<Writer>>() {};
        TypeToken<GsonNullable<Writer>> actual1 = GsonNullable.getNullableType(TypeToken.get(Writer.class));
        assertEquals(actual1, expected1);

        TypeToken<GsonNullable<String>> expected2 = new TypeToken<GsonNullable<String>>() {};
        Comparable<String> comparable = new Comparable<String>() {
            @Override
            public int compareTo(String o) {
                return 0;
            }
        };
        @SuppressWarnings("unchecked")
        TypeToken<String> stringToken = (TypeToken<String>) TypeToken.get(
            ((ParameterizedType) comparable.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[0]
        );
        TypeToken<GsonNullable<String>> actual2 = GsonNullable.getNullableType(stringToken);
        assertEquals(actual2, expected2);
    }

    @Test
    public void testNullable() throws Exception {
        TypeAdapter<GsonNullable<String>> nullableS = new GsonNullableTypeAdapter<String>(TypeAdapters.STRING);
        TypeAdapter<GsonNullable<Long>> nullableL = new GsonNullableTypeAdapter<Long>(LONG_ADAPTER);

        // serializing string
        JsonElement elemS = nullableS.toJsonTree(GsonNullable.of("test"));
        assertEquals(elemS.getAsJsonPrimitive().getAsString(), "test");
        JsonObject treeS = nullableS.toJsonTree(GsonNullable.<String>absent()).getAsJsonObject();
        assertEquals(treeS.entrySet().size(), 1);
        assertEquals(treeS.getAsJsonPrimitive("$null").getAsInt(), 1);
        assertTrue(nullableS.toJsonTree(null).isJsonNull());

        // serializing long
        JsonObject elemL = nullableL.toJsonTree(GsonNullable.of(777L)).getAsJsonObject();
        assertEquals(elemL.entrySet().size(), 1);
        assertEquals(elemL.getAsJsonPrimitive("$numberLong").getAsLong(), 777L);
        JsonObject treeL = nullableL.toJsonTree(GsonNullable.<Long>absent()).getAsJsonObject();
        assertEquals(treeL.entrySet().size(), 1);
        assertEquals(treeL.getAsJsonPrimitive("$null").getAsInt(), 1);
        assertTrue(nullableL.toJsonTree(null).isJsonNull());

        // deserializing string
        assertEquals(nullableS.fromJsonTree(JsonNull.INSTANCE), GsonNullable.<String>absent());
        assertEquals(nullableS.fromJsonTree(elemS), GsonNullable.of("test"));
        assertEquals(nullableS.fromJsonTree(treeS), GsonNullable.<String>absent());

        // deserializing long
        assertEquals(nullableL.fromJsonTree(JsonNull.INSTANCE), GsonNullable.<Long>absent());
        assertEquals(nullableL.fromJsonTree(elemL), GsonNullable.of(777L));
        assertEquals(nullableL.fromJsonTree(treeL), GsonNullable.<Long>absent());

        // special buffer handling
        GsonBuffer buffer = new GsonBuffer();
        TypeAdapters.JSON_ELEMENT.write(buffer.writer(), treeL);
        assertEquals(nullableL.read(buffer.reader()), GsonNullable.<Long>absent());
    }

    @Test(dependsOnMethods = "testNullable")
    public void testNullableFactory() throws Exception {
        TypeToken<GsonNullable<String>> tokenS = new TypeToken<GsonNullable<String>>(){};
        TypeAdapter<GsonNullable<String>> nullableS = gson.getAdapter(tokenS);
        assertNotNull(nullableS);
        JsonElement elemS = nullableS.toJsonTree(GsonNullable.of("test"));
        assertEquals(elemS.getAsJsonPrimitive().getAsString(), "test");
        JsonObject treeS = nullableS.toJsonTree(GsonNullable.<String>absent()).getAsJsonObject();
        assertEquals(treeS.getAsJsonPrimitive("$null").getAsInt(), 1);
        assertEquals(nullableS.fromJsonTree(elemS), GsonNullable.of("test"));
        assertEquals(nullableS.fromJsonTree(treeS), GsonNullable.<String>absent());

        TypeToken<GsonNullable<Long>> tokenL = new TypeToken<GsonNullable<Long>>(){};
        TypeAdapter<GsonNullable<Long>> nullableL = gson.getAdapter(tokenL);
        JsonObject elemL = nullableL.toJsonTree(GsonNullable.of(777L)).getAsJsonObject();
        assertEquals(elemL.getAsJsonPrimitive("$numberLong").getAsLong(), 777L);
        JsonObject treeL = nullableL.toJsonTree(GsonNullable.<Long>absent()).getAsJsonObject();
        assertEquals(treeL.getAsJsonPrimitive("$null").getAsInt(), 1);
        assertEquals(nullableL.fromJsonTree(elemL), GsonNullable.of(777L));
        assertEquals(nullableL.fromJsonTree(treeL), GsonNullable.<Long>absent());
    }

    @Test
    public void testFieldMatcher() throws Exception {
        JsonReader in = new JsonReader(new StringReader(
            "{\"xxx\": null, \"field3\": -7.7, \"unk\": 0, \"field1\": \"val1\"}"
        ));

        String field1 = null;
        Integer field2 = null;
        Double field3 = null;
        FieldMatcher m = new FieldMatcher("field1", "field2", "field3"); // natural field order
        in.beginObject();
        while (in.hasNext()) {
            switch (m.match(in.nextName())) {
                case 0: field1 = in.nextString(); break;
                case 1: field2 = in.nextInt(); break;
                case 2: field3 = in.nextDouble(); break;
                default: in.skipValue();
            }
        }
        in.endObject();

        assertEquals(field1, "val1");
        assertEquals(field2, null);
        assertEquals(field3, -7.7);
    }
}