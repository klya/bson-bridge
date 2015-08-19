/*
 * Copyright (c) 2008-2015 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.mongodb.*;
import org.bson.BSON;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.*;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Pattern;

import static com.maxifier.mongo.BsonReader.*;

/**
 * @author Konstantin Lyamshin (2015-01-22 16:50)
 */
public class BsonReaderTest extends org.testng.Assert {
    private BsonReader getReader(DBObject dbObject) {
        OutputBuffer buffer = new BasicOutputBuffer();
        DBEncoder encoder = new DefaultDBEncoder();
        encoder.writeObject(buffer, dbObject);
        BsonReader reader = new BsonReader();
        reader.reset(new ByteArrayInputStream(buffer.toByteArray()));
        return reader;
    }

    @Test
    public void testReadPrimitives() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder();
        db.add("_id", new ObjectId("8484848488484848FFFFEEEE")); // _id first because of DefaultDBEncoder impl
        db.add("nl", null);
        db.add("str", "sval");
        db.add("b", false);
        db.add("int", -77);
        db.add("lng", -999999999999999L);
        db.add("dbll", 44e15);
        db.add("date", new Date(0x9494949949L));
        db.add("bnary", new Binary((byte) 0x88, "najksld8oqw38cmcwq".getBytes()));
//        db.add("uid", UUID.fromString("791238482390-4890-2340-9902-8921390489238498")); TODO: uncomment
        db.add("rg", Pattern.compile("\\d{1,5}", Pattern.CASE_INSENSITIVE));
        db.add("cd", new Code("for (var a in this) alert(a);"));
        db.add("tm", new BSONTimestamp(74747874, 494848));
        db.add("mn", new MinKey());
        db.add("mx", new MaxKey());

        BsonReader reader = getReader(db.get());
        assertEquals(reader.getPath(), "");
        assertReader(reader, P_BEGIN_OBJECT, null, "$.");
        assertReader(reader, P_NAME, "_id", "$._id");
        assertReader(reader, P_OID, new ObjectId("8484848488484848FFFFEEEE"), "$.");
        assertReader(reader, P_NAME, "nl", "$.nl");
        assertReader(reader, P_NULL, null, "$.");
        assertReader(reader, P_NAME, "str", "$.str");
        assertReader(reader, P_STRING, "sval", "$.");
        assertReader(reader, P_NAME, "b", "$.b");
        assertReader(reader, P_BOOLEAN, false, "$.");
        assertReader(reader, P_NAME, "int", "$.int");
        assertReader(reader, P_INT, -77, "$.");
        assertReader(reader, P_NAME, "lng", "$.lng");
        assertReader(reader, P_LONG, -999999999999999L, "$.");
        assertReader(reader, P_NAME, "dbll", "$.dbll");
        assertReader(reader, P_DOUBLE, 44e15, "$.");
        assertReader(reader, P_NAME, "date", "$.date");
        assertReader(reader, P_DATE, new Date(0x9494949949L), "$.");
        assertReader(reader, P_NAME, "bnary", "$.bnary");
        assertReader(reader, P_BINARY, new Binary((byte) 0x88, "najksld8oqw38cmcwq".getBytes()), "$.");
//        assertReader(reader, P_NAME, "uid", "uid");
//        assertReader(reader, P_UUID, UUID.fromString("791238482390-4890-2340-9902-8921390489238498"), "");
        assertReader(reader, P_NAME, "rg", "$.rg");
        assertReader(reader, P_REGEX, Pattern.compile("\\d{1,5}", Pattern.CASE_INSENSITIVE), "$.");
        assertReader(reader, P_NAME, "cd", "$.cd");
        assertReader(reader, P_CODE, new Code("for (var a in this) alert(a);"), "$.");
        assertReader(reader, P_NAME, "tm", "$.tm");
        assertReader(reader, P_TIMESTAMP, new BSONTimestamp(74747874, 494848), "$.");
        assertReader(reader, P_NAME, "mn", "$.mn");
        assertReader(reader, P_MINKEY, null, "$.");
        assertReader(reader, P_NAME, "mx", "$.mx");
        assertReader(reader, P_MAXKEY, null, "$.");
        assertReader(reader, P_END_OBJECT, null, "");
        assertEquals(reader.peek(), P_NONE);
    }

    private static void assertReader(BsonReader reader, int state, Object value, String path) throws IOException {
        assertEquals(reader.peek(), state);
        switch (reader.peek()) {
            case P_BEGIN_OBJECT: reader.beginObject(); break;
            case P_END_OBJECT: reader.endObject(); break;
            case P_BEGIN_ARRAY: reader.beginArray(); break;
            case P_END_ARRAY: reader.endArray(); break;
            case P_NAME: assertEquals(reader.nextName(), value); break;
            case P_NULL: reader.nextNull(); break;
            case P_STRING: assertEquals(reader.nextString(), value); break;
            case P_BOOLEAN: assertEquals(reader.nextBoolean(), value); break;
            case P_INT: assertEquals(reader.nextInt(), value); break;
            case P_LONG: assertEquals(reader.nextLong(), value); break;
            case P_DOUBLE: assertEquals(reader.nextDouble(), value); break;
            case P_OID: assertEquals(reader.nextObjectId(), value); break;
            case P_DATE: assertEquals(reader.nextDate(), value); break;
            case P_BINARY: assertEquals(reader.nextBinary(), value); break;
            case P_CODE: assertEquals(reader.nextCode(), value); break;
            case P_TIMESTAMP: assertEquals(reader.nextTimestamp(), value); break;
            case P_MINKEY: reader.nextMinKey(); break;
            case P_MAXKEY: reader.nextMaxKey(); break;
            case P_REGEX:
                Pattern rg = reader.nextRegex();
                assertEquals(rg.pattern(), ((Pattern) value).pattern());
                assertEquals(rg.flags(), ((Pattern) value).flags());
                break;

            default: fail("Unexpected state " + reader.peek());
        }
        reader.peek(); // Prefetch path
        assertEquals(reader.getPath(), path);
    }

    @Test
    public void testReadNested() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder();
        db.add("_id", new ObjectId("8484848488484848FFFFEEEE")); // _id first because of DefaultDBEncoder impl
        db.push("o1");
        db.add("s1", "str1");
        db.push("o2");
        db.add("s2", "str2");
        db.pop();
        db.add("a1", Arrays.asList("str3", "str4", Arrays.asList("str5")));
        db.pop();
        db.add("a2", Arrays.asList(new BasicDBObject("sa", "str6")));

        // {_id: xxx, o1: {s1: str1, o2: {s2: str2}, a1: [str3, str4, [str5]]}, a2: [{sa: str6}]}

        BsonReader reader = getReader(db.get());
        assertEquals(reader.getPath(), "");
        assertReader(reader, P_BEGIN_OBJECT, null, "$.");
        assertReader(reader, P_NAME, "_id", "$._id");
        assertReader(reader, P_OID, new ObjectId("8484848488484848FFFFEEEE"), "$.");
        assertReader(reader, P_NAME, "o1", "$.o1");
        assertReader(reader, P_BEGIN_OBJECT, null, "$.o1.");
        assertReader(reader, P_NAME, "s1", "$.o1.s1");
        assertReader(reader, P_STRING, "str1", "$.o1.");
        assertReader(reader, P_NAME, "o2", "$.o1.o2");
        assertReader(reader, P_BEGIN_OBJECT, null, "$.o1.o2.");
        assertReader(reader, P_NAME, "s2", "$.o1.o2.s2");
        assertReader(reader, P_STRING, "str2", "$.o1.o2.");
        assertReader(reader, P_END_OBJECT, null, "$.o1.");
        assertReader(reader, P_NAME, "a1", "$.o1.a1");
        assertReader(reader, P_BEGIN_ARRAY, null, "$.o1.a1[0]");
        assertReader(reader, P_STRING, "str3", "$.o1.a1[1]");
        assertReader(reader, P_STRING, "str4", "$.o1.a1[2]");
        assertReader(reader, P_BEGIN_ARRAY, null, "$.o1.a1[2][0]");
        assertReader(reader, P_STRING, "str5", "$.o1.a1[2][]");
        assertReader(reader, P_END_ARRAY, null, "$.o1.a1[]");
        assertReader(reader, P_END_ARRAY, null, "$.o1.");
        assertReader(reader, P_END_OBJECT, null, "$.");
        assertReader(reader, P_NAME, "a2", "$.a2");
        assertReader(reader, P_BEGIN_ARRAY, null, "$.a2[0]");
        assertReader(reader, P_BEGIN_OBJECT, null, "$.a2[0].");
        assertReader(reader, P_NAME, "sa", "$.a2[0].sa");
        assertReader(reader, P_STRING, "str6", "$.a2[0].");
        assertReader(reader, P_END_OBJECT, null, "$.a2[]");
        assertReader(reader, P_END_ARRAY, null, "$.");
        assertReader(reader, P_END_OBJECT, null, "");
        assertEquals(reader.peek(), P_NONE);
    }

    @Test
    public void testSkipPrimitives() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder();
        db.add("_id", new ObjectId("8484848488484848FFFFEEEE")); // _id first because of DefaultDBEncoder impl
        db.add("nl", null);
        db.add("str", "sval");
        db.add("?", "sval");
        db.add("b", false);
        db.add("int", -77);
        db.add("lng", -999999999999999L);
        db.add("dbll", 44e15);
        db.add("date", new Date(0x9494949949L));
        db.add("bnary", new Binary((byte) 0x88, "najksld8oqw38cmcwq".getBytes()));
//        db.add("uid", UUID.fromString("791238482390-4890-2340-9902-8921390489238498")); TODO: uncomment
        db.add("rg", Pattern.compile("\\d{1,5}", Pattern.CASE_INSENSITIVE));
        db.add("cd", new Code("for (var a in this) alert(a);"));
        db.add("tm", new BSONTimestamp(74747874, 494848));
        db.add("mn", new MinKey());
        db.add("mx", new MaxKey());

        BsonReader reader = getReader(db.get());
        assertEquals(reader.getPath(), "");
        assertReader(reader, P_BEGIN_OBJECT, null, "$.");
        assertReader(reader, P_NAME, "_id", "$._id");
        assertEquals(reader.peek(), P_OID); reader.skipValue();
        assertReader(reader, P_NAME, "nl", "$.nl");
        assertEquals(reader.peek(), P_NULL); reader.skipValue();
        assertReader(reader, P_NAME, "str", "$.str");
        assertEquals(reader.peek(), P_STRING); reader.skipValue();
        assertEquals(reader.peek(), P_NAME); reader.skipValue();
        assertEquals(reader.peek(), P_STRING); reader.skipValue();
        assertReader(reader, P_NAME, "b", "$.b");
        assertEquals(reader.peek(), P_BOOLEAN); reader.skipValue();
        assertReader(reader, P_NAME, "int", "$.int");
        assertEquals(reader.peek(), P_INT); reader.skipValue();
        assertReader(reader, P_NAME, "lng", "$.lng");
        assertEquals(reader.peek(), P_LONG); reader.skipValue();
        assertReader(reader, P_NAME, "dbll", "$.dbll");
        assertEquals(reader.peek(), P_DOUBLE); reader.skipValue();
        assertReader(reader, P_NAME, "date", "$.date");
        assertEquals(reader.peek(), P_DATE); reader.skipValue();
        assertReader(reader, P_NAME, "bnary", "$.bnary");
        assertEquals(reader.peek(), P_BINARY); reader.skipValue();
//        assertReader(reader, P_NAME, "uid", "uid");
//        assertEquals(reader.peek(), P_UUID); reader.skipValue();
        assertReader(reader, P_NAME, "rg", "$.rg");
        assertEquals(reader.peek(), P_REGEX); reader.skipValue();
        assertReader(reader, P_NAME, "cd", "$.cd");
        assertEquals(reader.peek(), P_CODE); reader.skipValue();
        assertReader(reader, P_NAME, "tm", "$.tm");
        assertEquals(reader.peek(), P_TIMESTAMP); reader.skipValue();
        assertReader(reader, P_NAME, "mn", "$.mn");
        assertEquals(reader.peek(), P_MINKEY); reader.skipValue();
        assertReader(reader, P_NAME, "mx", "$.mx");
        assertEquals(reader.peek(), P_MAXKEY); reader.skipValue();
        assertReader(reader, P_END_OBJECT, null, "");
        assertEquals(reader.peek(), P_NONE);
    }

    @Test
    public void testSkipDocuments() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder()
            .add("_id", new ObjectId("8484848488484848FFFFEEEE")) // _id first because of DefaultDBEncoder impl
            .push("o1").add("s1", "str1").add("s2", new BasicDBObject("s", "str2")).pop()
            .push("o2").push("o3").add("s3", "str3").pop().pop()
            .push("o4").add("a1", Arrays.asList("as1", "as2")).pop()
            .add("a2", Arrays.asList("s3", "s4"))
            .add("a3", Arrays.asList(new BasicDBObject("s6", "str6")));

        // {_id: xxx, o1: {s1: str1, s2: {s: str2}}, o2: {o3: {s3: str3}}, o4: {a1: [as1, as2]}, a2: [s3, s4], a3: [{s6: str6}]}

        BsonReader reader = getReader(db.get());
        assertEquals(reader.peek(), P_BEGIN_OBJECT); reader.beginObject();
        assertEquals(reader.peek(), P_NAME);
        assertEquals(reader.nextName(), "_id"); reader.skipValue();
        assertEquals(reader.peek(), P_NAME);
        assertEquals(reader.nextName(), "o1"); reader.skipValue();
        assertEquals(reader.peek(), P_NAME);
        assertEquals(reader.nextName(), "o2"); reader.beginObject();
        assertEquals(reader.peek(), P_NAME);
        assertEquals(reader.nextName(), "o3"); reader.skipValue();
        assertEquals(reader.peek(), P_END_OBJECT); reader.endObject();
        assertEquals(reader.peek(), P_NAME);
        assertEquals(reader.nextName(), "o4"); reader.skipValue();
        assertEquals(reader.peek(), P_NAME);
        assertEquals(reader.nextName(), "a2"); reader.skipValue();
        assertEquals(reader.peek(), P_NAME);
        assertEquals(reader.nextName(), "a3"); reader.skipValue();
        assertEquals(reader.peek(), P_END_OBJECT); reader.endObject();
        assertEquals(reader.peek(), P_NONE);
    }

    @Test
    public void testReadWrong() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder()
            .add("_id", new ObjectId("8484848488484848FFFFEEEE")) // _id first because of DefaultDBEncoder impl
            .push("o1").pop()
            .add("a1", Arrays.asList())
            .add("nl", null)
            .add("s", "sval")
            .add("long", -77L);

        // {_id: xxx, o1: {}, a1: [], nl: null, s: sval, long: -77}

        BsonReader reader = getReader(db.get());
        reader.beginObject();
        assertEquals(reader.nextName(), "_id");
        try { reader.beginArray(); fail(); } catch (IllegalStateException ignored) {}
        reader.nextObjectId();
        assertEquals(reader.nextName(), "o1");
        try { reader.nextString(); fail(); } catch (IllegalStateException ignored) {}
        reader.beginObject();
        reader.endObject();
        assertEquals(reader.nextName(), "a1");
        try { reader.nextNull(); fail(); } catch (IllegalStateException ignored) {}
        reader.beginArray();
        reader.endArray();
        assertEquals(reader.nextName(), "nl");
        try { reader.nextInt(); fail(); } catch (IllegalStateException ignored) {}
        reader.nextNull();
        assertEquals(reader.nextName(), "s");
        try { reader.nextObjectId(); fail(); } catch (IllegalStateException ignored) {}
        reader.nextString();
        assertEquals(reader.nextName(), "long");
        try { reader.beginObject(); fail(); } catch (IllegalStateException ignored) {}
        reader.nextLong();
        reader.endObject();
    }

    @Test
    public void testSkipWrong() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder()
            .add("_id", new ObjectId("8484848488484848FFFFEEEE")) // _id first because of DefaultDBEncoder impl
            .push("o1").add("s1", "str1").pop()
            .add("a1", Arrays.asList("as1", "as2"));

        // {_id: xxx, o1: {s1: str1}, a1: [as1, as2]}

        BsonReader reader = getReader(db.get());
        reader.beginObject();
        reader.skipValue();
        reader.skipValue();
        assertEquals(reader.nextName(), "o1");
        reader.beginObject();
        assertEquals(reader.nextName(), "s1");
        assertEquals(reader.nextString(), "str1");
        try { reader.skipValue(); fail("Skip should fail!"); } catch (IllegalStateException ignored) {}
        reader.endObject();
        assertEquals(reader.nextName(), "a1");
        reader.beginArray();
        reader.skipValue();
        assertEquals(reader.nextString(), "as2");
        try { reader.skipValue(); fail("Skip should fail!"); } catch (IllegalStateException ignored) {}
        reader.endArray();
        try { reader.skipValue(); fail("Skip should fail!"); } catch (IllegalStateException ignored) {}
        reader.endObject();
        try { reader.skipValue(); fail("Skip should fail!"); } catch (IllegalStateException ignored) {}
        assertEquals(reader.peek(), P_NONE);
    }

    @Test
    public void testReadUnstarted() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder()
            .add("_id", new ObjectId("8484848488484848FFFFEEEE")); // _id first because of DefaultDBEncoder impl

        // {_id: xxx}

        BsonReader reader = getReader(db.get());
        try { reader.endObject(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.beginArray(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.endArray(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.nextName(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.nextNull(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.nextString(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.nextInt(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.nextObjectId(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
    }

    @Test
    public void testDocumentRestart() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder()
            .add("_id", new ObjectId("8484848488484848FFFFEEEE")); // _id first because of DefaultDBEncoder impl

        // {_id: xxx}

        BsonReader reader = getReader(db.get());
        reader.beginObject();
        reader.nextName();
        reader.nextObjectId();
        reader.endObject();
        assertEquals(reader.peek(), P_NONE);

        try { reader.beginObject(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.endObject(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.beginArray(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.endArray(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.nextName(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.nextNull(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.nextString(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.nextInt(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
        try { reader.nextObjectId(); fail("Read should fail"); } catch (IllegalStateException ignored) {}
    }

    @Test
    public void testOmitName() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder()
            .add("_id", new ObjectId("8484848488484848FFFFEEEE")) // _id first because of DefaultDBEncoder impl
            .push("o1").pop()
            .add("a1", Arrays.asList())
            .add("nl", null)
            .add("s", "sval")
            .add("int", -77);

        BsonReader reader = getReader(db.get());
        reader.beginObject();
        try { reader.nextObjectId(); fail("Read should fail!"); } catch (IllegalStateException ignored) {}
        assertEquals(reader.nextName(), "_id");
        reader.nextObjectId();
        try { reader.beginObject(); fail("Read should fail!"); } catch (IllegalStateException ignored) {}
        assertEquals(reader.nextName(), "o1");
        reader.beginObject();
        reader.endObject();
        try { reader.beginArray(); fail("Read should fail!"); } catch (IllegalStateException ignored) {}
        assertEquals(reader.nextName(), "a1");
        reader.beginArray();
        reader.endArray();
        try { reader.nextNull(); fail("Read should fail!"); } catch (IllegalStateException ignored) {}
        assertEquals(reader.nextName(), "nl");
        reader.nextNull();
        try { reader.nextString(); fail("Read should fail!"); } catch (IllegalStateException ignored) {}
        assertEquals(reader.nextName(), "s");
        reader.nextString();
        try { reader.nextInt(); fail("Read should fail!"); } catch (IllegalStateException ignored) {}
        assertEquals(reader.nextName(), "int");
        reader.nextInt();
        reader.endObject();
        assertEquals(reader.peek(), P_NONE);
    }

    @Test
    public void testArrayObjectMismatch() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder()
            .add("_id", new ObjectId("8484848488484848FFFFEEEE")) // _id first because of DefaultDBEncoder impl
            .push("o1").add("a1", Arrays.asList()).pop()
            .add("a2", Arrays.asList(new BasicDBObject()));

        // {_id: xxx, o1: {a1: []}, a2: [{}]}

        BsonReader reader = getReader(db.get());

        reader.beginObject();
        reader.nextName();
        reader.nextObjectId();
        assertEquals(reader.nextName(), "o1");
        try { reader.beginArray(); fail(); } catch (IllegalStateException ignored) {}
        reader.beginObject();
        assertEquals(reader.nextName(), "a1");
        try { reader.beginObject(); fail(); } catch (IllegalStateException ignored) {}
        reader.beginArray();
        try { reader.endObject(); fail(); } catch (IllegalStateException ignored) {}
        reader.endArray();
        try { reader.endArray(); fail(); } catch (IllegalStateException ignored) {}
        reader.endObject();
        assertEquals(reader.nextName(), "a2");
        try { reader.beginObject(); fail(); } catch (IllegalStateException ignored) {}
        reader.beginArray();
        try { reader.beginArray(); fail(); } catch (IllegalStateException ignored) {}
        reader.beginObject();
        try { reader.endArray(); fail(); } catch (IllegalStateException ignored) {}
        reader.endObject();
        try { reader.endObject(); fail(); } catch (IllegalStateException ignored) {}
        reader.endArray();
        reader.endObject();
        assertEquals(reader.peek(), P_NONE);
    }

    @Test
    public void testReadAfterEOF1() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder()
            .add("_id", new ObjectId("8484848488484848FFFFEEEE")) // _id first because of DefaultDBEncoder impl
            .add("s", "");

        // {_id: xxx, s: "quitelong }

        OutputBuffer buffer = new BasicOutputBuffer();
        DBEncoder encoder = new DefaultDBEncoder();
        encoder.writeObject(buffer, db.get());
        BsonReader reader = new BsonReader();
        reader.reset(new ByteArrayInputStream(buffer.toByteArray(), 0, buffer.size() - 1));

        reader.beginObject();
        reader.nextName();
        reader.skipValue();
        reader.nextName();
        reader.nextString();
        try { reader.endObject(); fail(); } catch (EOFException ignored) {}
    }

    @Test
    public void testReadAfterEOF2() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder()
            .add("_id", new ObjectId("8484848488484848FFFFEEEE")) // _id first because of DefaultDBEncoder impl
            .add("s", "");

        // {_id: xxx, s: "quitelong }

        OutputBuffer buffer = new BasicOutputBuffer();
        DBEncoder encoder = new DefaultDBEncoder();
        encoder.writeObject(buffer, db.get());
        BsonReader reader = new BsonReader();
        reader.reset(new ByteArrayInputStream(buffer.toByteArray(), 0, buffer.size() - 2));

        reader.beginObject();
        reader.nextName();
        reader.skipValue();
        reader.nextName();
        try { reader.nextString(); fail(); } catch (EOFException ignored) {}
    }

    @Test
    public void testReadOverlimitName() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder()
            .add("_id", new ObjectId("8484848488484848FFFFEEEE")) // _id first because of DefaultDBEncoder impl
            .push("o").add("brokenName", null).pop()
            .add("s", "quitelong");

        // {_id: xxx, o: {brokenName: null}, s: "quitelong" }

        OutputBuffer buffer = new BasicOutputBuffer();
        DBEncoder encoder = new DefaultDBEncoder();
        encoder.writeObject(buffer, db.get());

        byte[] bytes = buffer.toByteArray();
        byte[] name = "brokenName".getBytes("UTF-8");
        int idx = indexOf(bytes, name);

        // Remove \0 terminator
        assertEquals(bytes[idx + name.length], 0);
        bytes[idx + name.length] = (byte) '!';

        BsonReader reader = new BsonReader();
        reader.reset(new ByteArrayInputStream(bytes));

        reader.beginObject();
        reader.nextName();
        reader.nextObjectId();
        assertEquals(reader.nextName(), "o");
        reader.beginObject();
        reader.nextName();
        try {
            reader.endObject();
            fail();
        } catch (IllegalStateException ignored) {
        }
    }
    
    @Test
    public void testReadOverlimitString() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder()
            .add("_id", new ObjectId("8484848488484848FFFFEEEE")) // _id first because of DefaultDBEncoder impl
            .push("o").add("v", "brokenValue").pop()
            .add("s", "quitelong");

        // {_id: xxx, o: {v: brokenValue}, s: "quitelong" }

        OutputBuffer buffer = new BasicOutputBuffer();
        DBEncoder encoder = new DefaultDBEncoder();
        encoder.writeObject(buffer, db.get());

        byte[] bytes = buffer.toByteArray();
        byte[] name = "brokenValue".getBytes("UTF-8");
        int idx = indexOf(bytes, name);

        // Patch value len
        assertEquals(bytes[idx - 4], name.length + 1);
        bytes[idx - 4] += 2; // Advance on 2 because of one-byte space available for EOD

        BsonReader reader = new BsonReader();
        reader.reset(new ByteArrayInputStream(bytes));

        reader.beginObject();
        reader.nextName();
        reader.nextObjectId();
        reader.nextName();
        reader.beginObject();
        assertEquals(reader.nextName(), "v");
        try {
            reader.nextString();
            fail();
        } catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void testReadOverlimitInt() throws Exception {
        BasicDBObjectBuilder db = new BasicDBObjectBuilder()
            .add("_id", new ObjectId("8484848488484848FFFFEEEE")) // _id first because of DefaultDBEncoder impl
            .push("o").add("brokenValue", null).pop()
            .add("s", "quitelong");

        // {_id: xxx, o: {brokenValue: null}, s: "quitelong" }

        OutputBuffer buffer = new BasicOutputBuffer();
        DBEncoder encoder = new DefaultDBEncoder();
        encoder.writeObject(buffer, db.get());

        byte[] bytes = buffer.toByteArray();
        byte[] name = "brokenValue".getBytes("UTF-8");
        int idx = indexOf(bytes, name);

        // Patch value len
        assertEquals(bytes[idx - 1], BSON.NULL);
        bytes[idx - 1] = BSON.NUMBER_INT;

        BsonReader reader = new BsonReader();
        reader.reset(new ByteArrayInputStream(bytes));

        reader.beginObject();
        reader.nextName();
        reader.nextObjectId();
        reader.nextName();
        reader.beginObject();
        assertEquals(reader.nextName(), "brokenValue");
        try {
            reader.nextInt();
            fail();
        } catch (IllegalStateException ignored) {
        }
    }

    private static int indexOf(byte[] array, byte[] target) {
        outer:
        for (int i = 0; i < array.length - target.length + 1; i++) {
            for (int j = 0; j < target.length; j++) {
                if (array[i + j] != target[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    @Test
    public void testUTF8Parse() throws Exception {
        // xxxx xxxx A B C D E F
        // 8421 8421 0 1 2 3 4 5
        BsonReader reader = new BsonReader();
        reader.reset(new ByteArrayInputStream(
            "\tЯ б \uD83D\uDE21\0".getBytes("UTF-8")
        ));
        assertEquals(reader.readCString(), "\tЯ б \uD83D\uDE21");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUTF8NoPreface() throws Exception {
        BsonReader reader = new BsonReader();
        reader.reset(toStream(0x20, 0xA0, 0x20, 0x00));
        reader.readCString();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUTF8NoConclusion() throws Exception {
        BsonReader reader = new BsonReader();
        reader.reset(toStream(0x20, 0xCB, 0x20, 0x00));
        reader.readCString();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUTF8EOS() throws Exception {
        BsonReader reader = new BsonReader();
        reader.reset(toStream(0x20, 0xEB, 0xA0, 0x00));
        reader.readCString();
    }

    private static InputStream toStream(int... values) {
        byte[] b = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            b[i] = (byte) values[i];
        }
        return new ByteArrayInputStream(b);
    }
}