/*
 * Copyright (c) 2008-2014 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.*;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.mockito.Mockito.mock;

/**
 * @author Konstantin Lyamshin (2014-12-29 17:39)
 */
@SuppressWarnings("AssertEqualsBetweenInconvertibleTypesTestNG")
public class GsonDBCollectionTest extends org.testng.Assert {
    private GsonDBCollection<TestGsonVO> dbc;

    @BeforeClass
    public void setUp() throws Exception {
        Gson gson = GsonAdapters.configure(new GsonBuilder())
            .registerTypeAdapterFactory(new TestGsonVO.TestGsonAdapterFactory())
            .registerTypeAdapter(TestGsonVO.TestGsonSerialized.class, new TestGsonVO.TestGsonSerializer())
            .create();
        dbc = new GsonDBCollection<TestGsonVO>(mock(DBCollection.class), TestGsonVO.class, gson);
    }

    @DataProvider
    private Object[][] provideVOs() {
        return new Object[][] {
            {new TestGsonVO().initBasic()},
            {new TestGsonVO().initVO1()},
            {new TestGsonVO().initVO2()}
        };
    }

    @Test(dataProvider = "provideVOs")
    public void testDecode(TestGsonVO vo) throws Exception {
        OutputBuffer buffer = new BasicOutputBuffer();
        DBEncoder encoder = new DefaultDBEncoder();
        encoder.writeObject(buffer, vo.toBson());

        DBDecoder decoder = dbc.new GsonDBDecoder();
        DBObject bson = decoder.decode(buffer.toByteArray(), (DBCollection) null);
        TestGsonVO o = (TestGsonVO) ((GsonWrapper) bson).getPojo();
        vo.assertGson(o);
    }

    @Test(dataProvider = "provideVOs")
    public void testEncode(TestGsonVO vo) throws Exception {
        OutputBuffer buffer = new BasicOutputBuffer();
        DBEncoder encoder = dbc.new GsonDBEncoder();
        encoder.writeObject(buffer, new GsonWrapper(vo));

        DBDecoder decoder = new DefaultDBDecoder();
        DBObject o = decoder.decode(buffer.toByteArray(), (DBCollection) null);
        vo.assertBson(o);
    }

    @Test(dataProvider = "provideVOs")
    public void testBson(TestGsonVO vo) throws Exception {
        DBObject bson = vo.toBson();

        OutputBuffer expected = new BasicOutputBuffer();
        new DefaultDBEncoder().writeObject(expected, bson);

        OutputBuffer buffer = new BasicOutputBuffer();
        DBEncoder encoder = dbc.new GsonDBEncoder();
        encoder.writeObject(buffer, bson);
        assertBytes(buffer.toByteArray(), expected.toByteArray());

        DefaultDBDecoder decoder = new DefaultDBDecoder();
        DBObject result = decoder.decode(buffer.toByteArray(), (DBCollection) null);
        assertEquals(result, bson);
    }

    @Test(dataProvider = "provideVOs")
    public void testMixed(TestGsonVO vo) throws Exception {
        DBObject bson = new BasicDBObject("$set", new GsonWrapper(vo));
        OutputBuffer buffer = new BasicOutputBuffer();

        DBEncoder encoder = dbc.new GsonDBEncoder();
        encoder.writeObject(buffer, bson);

        DBDecoder decoder = new DefaultDBDecoder();
        DBObject o = decoder.decode(buffer.toByteArray(), (DBCollection) null);

        assertEquals(o.keySet().size(), 1);
        vo.assertBson((DBObject) o.get("$set"));
    }

    public static void assertBytes(byte[] actual, byte[] expected) {
        if (!Arrays.equals(actual, expected)) {

            StringBuilder sb = new StringBuilder("Arrays is different");

            sb.append("\nActual:   ");
            for (byte a : actual) {
                sb.append(Integer.toHexString(a >> 4 & 0xF)).append(Integer.toHexString(a & 0xF));
            }

            sb.append("\nExpected: ");
            for (byte e : expected) {
                sb.append(Integer.toHexString(e >> 4 & 0xF)).append(Integer.toHexString(e & 0xF));
            }

            sb.append("\n          ");
            for (int i = 0; i < actual.length && i < expected.length; i++) {
                sb.append(actual[i] == expected[i]? "  ": "^^");
            }

            fail(sb.toString());
        }
    }
}