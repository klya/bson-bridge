/*
 * Copyright (c) 2008-2015 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.mongodb.DBCollection;
import com.mongodb.DBDecoder;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBDecoder;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * @author Konstantin Lyamshin (2015-02-09 11:56)
 */
@SuppressWarnings("AssertEqualsBetweenInconvertibleTypesTestNG")
public class BsonWriterTest extends org.testng.Assert {
    private OutputBuffer buffer;
    private BsonWriter writer;

    @BeforeMethod
    public void setUp() throws Exception {
        buffer = new BasicOutputBuffer();
        writer = new BsonWriter();
        writer.reset(buffer);
    }

    @Test
    public void testWrite() throws Exception {
        assertEquals(writer.getPath(), "");
        writer.beginObject(); assertEquals(writer.getPath(), "$.");
        writer.name("nl"); assertEquals(writer.getPath(), "$.nl");
        writer.nullValue(); assertEquals(writer.getPath(), "$.");
        writer.name("str"); assertEquals(writer.getPath(), "$.str");
        writer.stringValue("sval"); assertEquals(writer.getPath(), "$.");
        writer.name("i"); assertEquals(writer.getPath(), "$.i");
        writer.intValue(10); assertEquals(writer.getPath(), "$.");
        writer.name("l"); assertEquals(writer.getPath(), "$.l");
        writer.longValue(-1); assertEquals(writer.getPath(), "$.");
        writer.name("n"); assertEquals(writer.getPath(), "$.n");
        writer.doubleValue(1e27); assertEquals(writer.getPath(), "$.");
        writer.name("dt"); assertEquals(writer.getPath(), "$.dt");
        writer.dateValue(new Date(1423488392317L)); assertEquals(writer.getPath(), "$.");
        writer.name("arr"); assertEquals(writer.getPath(), "$.arr");
        writer.beginArray(); assertEquals(writer.getPath(), "$.arr[0]");
        writer.objectIdValue(new ObjectId("C0DE4F00D100750900D11111")); assertEquals(writer.getPath(), "$.arr[1]");
        writer.binaryValue(new Binary("cryptodat" .getBytes("UTF-8"))); assertEquals(writer.getPath(), "$.arr[2]");
        writer.binaryValue("cryptodat2".getBytes("UTF-8")); assertEquals(writer.getPath(), "$.arr[3]");
        writer.endArray(); assertEquals(writer.getPath(), "$.");
        writer.name("uuid"); assertEquals(writer.getPath(), "$.uuid");
        writer.uuidValue(UUID.fromString("00020400-0000-0000-C000-000000000046")); assertEquals(writer.getPath(), "$.");
        writer.name("regex"); assertEquals(writer.getPath(), "$.regex");
        writer.regexValue(Pattern.compile("ba\\d(0-D)", Pattern.MULTILINE)); assertEquals(writer.getPath(), "$.");
        writer.name("cd"); assertEquals(writer.getPath(), "$.cd");
        writer.codeValue(new Code("{println('badcode');")); assertEquals(writer.getPath(), "$.");
        writer.name("strange"); assertEquals(writer.getPath(), "$.strange");
        writer.beginObject(); assertEquals(writer.getPath(), "$.strange.");
        writer.name("tm"); assertEquals(writer.getPath(), "$.strange.tm");
        writer.timestampValue(new BSONTimestamp(1423488556, 7)); assertEquals(writer.getPath(), "$.strange.");
        writer.name("mn"); assertEquals(writer.getPath(), "$.strange.mn");
        writer.minkeyValue(); assertEquals(writer.getPath(), "$.strange.");
        writer.name("mx"); assertEquals(writer.getPath(), "$.strange.mx");
        writer.maxkeyValue(); assertEquals(writer.getPath(), "$.strange.");
        writer.endObject(); assertEquals(writer.getPath(), "$.");
        writer.name("b"); assertEquals(writer.getPath(), "$.b");
        writer.booleanValue(false); assertEquals(writer.getPath(), "$.");
        writer.endObject(); assertEquals(writer.getPath(), "");

        DBDecoder decoder = new DefaultDBDecoder();
        DBObject o = decoder.decode(buffer.toByteArray(), (DBCollection) null);
        assertEquals(o.containsField("nl"), true);
        assertEquals(o.get("nl"), null);
        assertEquals(o.get("str"), "sval");
        assertEquals(o.get("i"), 10);
        assertEquals(o.get("l"), -1L);
        assertEquals(o.get("n"), 1e27);
        assertEquals(o.get("dt"), new Date(1423488392317L));
        List<?> arr = (List<?>) o.get("arr");
        assertNotNull(arr);
        assertEquals(arr.size(), 3);
        assertEquals(arr.get(0), new ObjectId("C0DE4F00D100750900D11111"));
        assertEquals(arr.get(1), "cryptodat".getBytes("UTF-8"));
        assertEquals(arr.get(2), "cryptodat2".getBytes("UTF-8"));
        assertEquals(o.get("uuid"), UUID.fromString("00020400-0000-0000-C000-000000000046"));
        Pattern regex = (Pattern) o.get("regex");
        assertNotNull(regex);
        assertEquals(regex.pattern(), "ba\\d(0-D)");
        assertEquals(regex.flags(), Pattern.MULTILINE);
        assertEquals(o.get("cd"), new Code("{println('badcode');"));
        DBObject strange = (DBObject) o.get("strange");
        assertNotNull(strange);
        assertEquals(strange.keySet().size(), 3);
        assertEquals(strange.get("tm"), new BSONTimestamp(1423488556, 7));
        assertEquals(strange.get("mn"), new MinKey());
        assertEquals(strange.get("mx"), new MaxKey());
        assertEquals(o.get("b"), false);
    }

    @Test(dependsOnMethods = "testWrite")
    public void testWriteTypeless() throws Exception {
        assertEquals(writer.getPath(), "");
        writer.beginObject();
        writer.name("vals");
        writer.beginArray();
        writer.value(null);
        writer.value(-1);
        writer.value(-10L);
        writer.value(-1e27);
        writer.value(new Date(1423488392317L));
        writer.value(new ObjectId("C0DE4F00D100750900D11111"));
        writer.value("cryptodat3".getBytes("UTF-8"));
        writer.value(new Binary("cryptodat4".getBytes("UTF-8")));
        writer.value(UUID.fromString("00020400-0000-0000-C000-000000000046"));
        writer.value(Pattern.compile("ba\\d\\(0-D3", Pattern.CASE_INSENSITIVE));
        writer.value(new Code("{println('badcode');"));
        writer.value(new BSONTimestamp(1423488556, 7));
        writer.value(new MinKey());
        writer.value(new MaxKey());
        writer.value(true);
        writer.endArray();
        writer.name("nums");
        writer.beginArray();
        writer.numberValue((byte) 64);
        writer.numberValue((short) 1024);
        writer.numberValue(77777777777L);
        writer.numberValue(7e7f);
        writer.numberValue(7e77);
        writer.endArray();
        writer.endObject();

        DBDecoder decoder = new DefaultDBDecoder();
        DBObject o = decoder.decode(buffer.toByteArray(), (DBCollection) null);
        List<?> a = (List<?>) o.get("vals");
        assertNotNull(a);
        assertEquals(a.size(), 15);
        assertEquals(a.get(0), null);
        assertEquals(a.get(1), -1);
        assertEquals(a.get(2), -10L);
        assertEquals(a.get(3), -1e27);
        assertEquals(a.get(4), new Date(1423488392317L));
        assertEquals(a.get(5), new ObjectId("C0DE4F00D100750900D11111"));
        assertEquals(a.get(6), "cryptodat3".getBytes("UTF-8"));
        assertEquals(a.get(7), "cryptodat4".getBytes("UTF-8"));
        assertEquals(a.get(8), UUID.fromString("00020400-0000-0000-C000-000000000046"));
        assertEquals(((Pattern) a.get(9)).pattern(), "ba\\d\\(0-D3");
        assertEquals(((Pattern) a.get(9)).flags(), Pattern.CASE_INSENSITIVE);
        assertEquals(a.get(10), new Code("{println('badcode');"));
        assertEquals(a.get(11), new BSONTimestamp(1423488556, 7));
        assertEquals(a.get(12), new MinKey());
        assertEquals(a.get(13), new MaxKey());
        assertEquals(a.get(14), true);
        List<?> nums = (List<?>) o.get("nums");
        assertNotNull(nums);
        assertEquals(nums.size(), 5);
        assertEquals(nums.get(0), 64);
        assertEquals(nums.get(1), 1024);
        assertEquals(nums.get(2), 77777777777L);
        assertEquals(nums.get(3), 7e7);
        assertEquals(nums.get(4), 7e77);
    }

    @Test
    public void testSkipName() throws Exception {
        writer.beginObject();
        try { writer.beginObject(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.beginArray(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.endArray(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.nullValue(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.stringValue(""); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.intValue(0); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.longValue(1); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.doubleValue(2); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.booleanValue(true); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.dateValue(new Date()); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.objectIdValue(ObjectId.get()); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.binaryValue(new Binary(new byte[0])); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.binaryValue(new byte[0]); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.uuidValue(UUID.randomUUID()); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.regexValue(Pattern.compile("")); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.codeValue(new Code("")); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.timestampValue(new BSONTimestamp()); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.minkeyValue(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.maxkeyValue(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        writer.endObject();
    }

    @Test
    public void testSkipRoot() throws Exception {
        try { writer.endObject(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.beginArray(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.endArray(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.nullValue(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.stringValue(""); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.intValue(0); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.longValue(1); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.doubleValue(2); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.booleanValue(true); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.dateValue(new Date()); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.objectIdValue(ObjectId.get()); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.binaryValue(new Binary(new byte[0])); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.binaryValue(new byte[0]); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.uuidValue(UUID.randomUUID()); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.regexValue(Pattern.compile("")); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.codeValue(new Code("")); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.timestampValue(new BSONTimestamp()); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.minkeyValue(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.maxkeyValue(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
    }

    @Test
    public void testUnexpectedName() throws Exception {
        try { writer.name("n1"); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        writer.beginObject();
        writer.name("arr");
        writer.beginArray();
        try { writer.name("n2"); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        writer.endArray();
        writer.endObject();
        try { writer.name("n3"); fail("Write should fail"); } catch (IllegalStateException ignored) {}
    }

    @Test
    public void testEOOMismatch() throws Exception {
        writer.beginObject();
        writer.name("o");
        writer.beginObject();
        try { writer.endArray(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        writer.endObject();
        writer.name("a");
        writer.beginArray();
        try { writer.endObject(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        writer.endArray();
        writer.endObject();
        try { writer.endArray(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.endObject(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
    }

    @Test
    public void testBadName() throws Exception {
        writer.beginObject();
        try { writer.name("."); fail("Write should fail"); } catch (IllegalArgumentException ignored) {}
        writer.name("obj.field");
        writer.nullValue();
        try { writer.name("#"); fail("Write should fail"); } catch (IllegalArgumentException ignored) {}
        writer.name("arr.#.field");
        writer.nullValue();
    }

    @Test
    public void testRestart() throws Exception {
        writer.beginObject();
        writer.endObject();
        try { writer.beginObject(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.endObject(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.beginArray(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.endArray(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.name("n"); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.nullValue(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.stringValue(""); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.intValue(0); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.longValue(1); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.doubleValue(2); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.booleanValue(true); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.dateValue(new Date()); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.objectIdValue(ObjectId.get()); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.binaryValue(new Binary(new byte[0])); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.binaryValue(new byte[0]); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.uuidValue(UUID.randomUUID()); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.regexValue(Pattern.compile("")); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.codeValue(new Code("")); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.timestampValue(new BSONTimestamp()); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.minkeyValue(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
        try { writer.maxkeyValue(); fail("Write should fail"); } catch (IllegalStateException ignored) {}
    }
}