/*
 * Copyright (c) 2008-2013 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.*;

/**
 * DBObjectSerializerTest
 *
 * @author Konstantin Lyamshin (konstantin.lyamshin@maxifier.com) (2012-08-27 21:37)
 */
public class DBObjectSerializerTest {
	@DBObjectFieldOrder({ "tfld", "sfld", "ifld", "dfld", "nfld", "ofld" })
	public static class TestCollection1 extends DBObjectSerializer {
		private Date tfld;
		private String sfld;
		private Integer ifld;
		private Double dfld;
		private Object nfld;
		private TestCollection2 ofld;
		private List<TestCollection2> list;

		public ObjectId get_id() {
			return (ObjectId) _id;
		}
		public void set_id(ObjectId _id) {
			this._id = _id;
		}

		// This must be skipped
		public ObjectId get_Id() {
			return (ObjectId) _id;
		}
		// This must be skipped
		public void setId(ObjectId _id) {
			this._id = _id;
		}
		public Date getTfld() {
			return tfld;
		}
		public void setTfld(Date tfld) {
			this.tfld = tfld;
		}
		public String getSfld() {
			return sfld;
		}
		public void setSfld(String sfld) {
			this.sfld = sfld;
		}
		public Integer getIfld() {
			return ifld;
		}
		public void setIfld(Integer ifld) {
			this.ifld = ifld;
		}
		public Double getDfld() {
			return dfld;
		}
		public void setDfld(Double dfld) {
			this.dfld = dfld;
		}
		public Object getNfld() {
			return nfld;
		}
		public void setNfld(Object nfld) {
			this.nfld = nfld;
		}
		public TestCollection2 getOfld() {
			return ofld;
		}
		public void setOfld(TestCollection2 ofld) {
			this.ofld = ofld;
		}
		public List<TestCollection2> getList() {
			return list;
		}
		public void setList(List<TestCollection2> list) {
			this.list = list;
		}

		private String indexed;

		public String getIndexed() {
			return indexed;
		}
		public String getIndexed(int index) {
			return indexed;
		}
		public void setIndexed(String indexed) {
			this.indexed = indexed;
		}
		public void setIndexed(int index, String indexed) {
			this.indexed = indexed;
		}

	}

	public static class TestCollection2 extends DBObjectSerializer {
		private Object nfld;
		private TestCollection3 ofld;
		private List<TestCollection3> list;

		public Object getNfld() {
			return nfld;
		}
		public void setNfld(Object nfld) {
			this.nfld = nfld;
		}
		public TestCollection3 getOfld() {
			return ofld;
		}
		public void setOfld(TestCollection3 ofld) {
			this.ofld = ofld;
		}
		public List<TestCollection3> getList() {
			return list;
		}
		public void setList(List<TestCollection3> list) {
			this.list = list;
		}
	}

	public static class TestCollection3 extends DBObjectSerializer {
		private String name;

		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
	}

	@Test
	public void testConfigureCollection() throws Exception {
		DBCollection mock = mock(DBCollection.class);
		DBObjectSerializer.configureCollection(mock, TestCollection1.class);
		verify(mock).setObjectClass(TestCollection1.class);
		verify(mock).setDBDecoderFactory(DBObjectDecoder.FACTORY);
	}

	@Test
	public void testMarkAsPartialObject() throws Exception {
		TestCollection1 collection1 = new TestCollection1();
		assertFalse(collection1.isPartialObject());
		collection1.markAsPartialObject();
		assertTrue(collection1.isPartialObject());
	}

	@Test
	public void testPut() throws Exception {
		Calendar cal = Calendar.getInstance();
		cal.setTimeZone(TimeZone.getTimeZone("GMT"));
		cal.setTimeInMillis(0);
		cal.set(2012, Calendar.DECEMBER, 20);

		TestCollection3 c31 = new TestCollection3();
		c31.setName("c31");

		TestCollection3 c32 = new TestCollection3();
		c32.setName("c32");

		TestCollection2 c21 = new TestCollection2();
		c21.setOfld(c31);

		TestCollection2 c22 = new TestCollection2();
		c22.setOfld(c32);

		TestCollection1 c = new TestCollection1();
		c.setId(new ObjectId(0x7777, 0x5555, 0x3333));
		c.setTfld(cal.getTime());
		c.setSfld("testcol");
		c.setIfld(11);
		c.setDfld(33.33);
		c.setOfld(c21);
		c.setList(Arrays.asList(c22, c21));

		assertEquals(c.toString(), ("{ '_id' : { '$oid' : '000077770000555500003333'} , " +
			"'tfld' : { '$date' : '2012-12-20T00:00:00.000Z'} , 'sfld' : 'testcol' , 'ifld' : 11 , 'dfld' : 33.33 , " +
			"'ofld' : { 'ofld' : { 'name' : 'c31'}} , " +
			"'list' : [ { 'ofld' : { 'name' : 'c32'}} , { 'ofld' : { 'name' : 'c31'}}]}").replace('\'', '"'));
	}

	@Test(expectedExceptions = MongoSerializationException.class)
	public void testPutNonexistent() throws Exception {
		TestCollection1 c = new TestCollection1();
		c.setId(new ObjectId(0x7777, 0x5555, 0x3333));
		c.put("boooom", new Object());
	}

	@Test
	public void testPutDBObject() throws Exception {
		Calendar cal = Calendar.getInstance();
		cal.setTimeZone(TimeZone.getTimeZone("GMT"));
		cal.setTimeInMillis(0);
		cal.set(2012, Calendar.DECEMBER, 20);

		TestCollection3 c31 = new TestCollection3();
		c31.setName("c31");

		TestCollection2 c21 = new TestCollection2();
		c21.setOfld(c31);

		BasicDBObject dbObject1 = new BasicDBObject();
		dbObject1.put("_id", new ObjectId(0x7777, 0x5555, 0x3333));
		dbObject1.put("tfld", cal.getTime());
		dbObject1.put("sfld", "testcol");
		dbObject1.put("ifld", 11);
		dbObject1.put("dfld", 33.33);
		dbObject1.put("ofld", c21);

		DBObjectSerializer dbos1 = new TestCollection1();
		dbos1.putAll((DBObject) dbObject1);

		assertEquals(dbos1.toString(), ("{ '_id' : { '$oid' : '000077770000555500003333'} , " +
            "'tfld' : { '$date' : '2012-12-20T00:00:00.000Z'} , 'sfld' : 'testcol' , 'ifld' : 11 , 'dfld' : 33.33 , " +
            "'ofld' : { 'ofld' : { 'name' : 'c31'}}}").replace('\'', '"'));

		DBObjectSerializer dbos2 = new TestCollection1();
		dbos2.putAll(dbos1);

		assertEquals(dbos2.toString(), dbos1.toString());
	}

	@Test
	public void testPutMap() throws Exception {
		Calendar cal = Calendar.getInstance();
		cal.setTimeZone(TimeZone.getTimeZone("GMT"));
		cal.setTimeInMillis(0);
		cal.set(2012, Calendar.DECEMBER, 20);

		TestCollection3 c31 = new TestCollection3();
		c31.setName("c31");

		TestCollection2 c21 = new TestCollection2();
		c21.setOfld(c31);

		HashMap<String, Object> dbObject = new HashMap<String, Object>();
		dbObject.put("_id", new ObjectId(0x7777, 0x5555, 0x3333));
		dbObject.put("tfld", cal.getTime());
		dbObject.put("sfld", "testcol");
		dbObject.put("ifld", 11);
		dbObject.put("dfld", 33.33);
		dbObject.put("ofld", c21);

		DBObjectSerializer dbos = new TestCollection1();
		dbos.putAll(dbObject);

		assertEquals(dbos.toString(), ("{ '_id' : { '$oid' : '000077770000555500003333'} , " +
			"'tfld' : { '$date' : '2012-12-20T00:00:00.000Z'} , 'sfld' : 'testcol' , 'ifld' : 11 , 'dfld' : 33.33 , " +
			"'ofld' : { 'ofld' : { 'name' : 'c31'}}}").replace('\'', '"'));
	}

	@Test
	public void testGet() throws Exception {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(0);
		cal.set(2012, Calendar.DECEMBER, 20);

		TestCollection3 c31 = new TestCollection3();
		c31.setName("c31");

		TestCollection3 c32 = new TestCollection3();
		c32.setName("c32");

		TestCollection2 c21 = new TestCollection2();
		c21.setOfld(c31);

		TestCollection2 c22 = new TestCollection2();
		c22.setOfld(c32);

		TestCollection1 c = new TestCollection1();
		c.setId(new ObjectId(0x7777, 0x5555, 0x3333));
		c.setTfld(cal.getTime());
		c.setSfld("testcol");
		c.setIfld(11);
		c.setDfld(33.33);
		c.setOfld(c21);
		c.setList(Arrays.asList(c22, c21));

		assertEquals(c.get("_id"), new ObjectId(0x7777, 0x5555, 0x3333));
		assertEquals(c.get("tfld"), cal.getTime());
		assertEquals(c.get("sfld"), "testcol");
		assertEquals(c.get("ifld"), 11);
		assertEquals(c.get("dfld"), 33.33);
		assertSame(c.get("ofld"), c21);
		assertNull(c.get("booom"));
	}

	@Test
	public void testToMap() throws Exception {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(0);
		cal.set(2012, Calendar.DECEMBER, 20);

		TestCollection3 c31 = new TestCollection3();
		c31.setName("c31");

		TestCollection3 c32 = new TestCollection3();
		c32.setName("c32");

		TestCollection2 c21 = new TestCollection2();
		c21.setOfld(c31);

		TestCollection2 c22 = new TestCollection2();
		c22.setOfld(c32);

		TestCollection1 c = new TestCollection1();
		c.setId(new ObjectId(0x7777, 0x5555, 0x3333));
		c.setTfld(cal.getTime());
		c.setSfld("testcol");
		c.setIfld(11);
		c.setDfld(33.33);
		c.setOfld(c21);
		c.setList(Arrays.asList(c22, c21));

		Map m = c.toMap();
		assertEquals(m.get("_id"), new ObjectId(0x7777, 0x5555, 0x3333));
		assertEquals(m.get("tfld"), cal.getTime());
		assertEquals(m.get("sfld"), "testcol");
		assertEquals(m.get("ifld"), 11);
		assertEquals(m.get("dfld"), 33.33);
		assertSame(m.get("ofld"), c21);
	}

	@Test
	public void testRemoveField() throws Exception {
		TestCollection1 c = new TestCollection1();
		c.setSfld("testcol");
		assertEquals(c.get("sfld"), "testcol");
		c.removeField("sfld");
		assertNull(c.get("sfld"));
	}

	@Test
	public void testContainsField() throws Exception {
		TestCollection1 c = new TestCollection1();
		c.setSfld("testcol");
		c.setIfld(11);
		c.setDfld(33.33);
		assertTrue(c.containsField("sfld"));
		assertTrue(c.containsField("ifld"));
		assertTrue(c.containsField("dfld"));
		assertFalse(c.containsField("nfld")); // null field
		assertFalse(c.containsField("field"));
	}

	@Test
	public void testKeySet() throws Exception {
		TestCollection1 c = new TestCollection1();
		c.setSfld("testcol");
		c.setIfld(11);
		c.setDfld(33.33);
		assertEquals(c.keySet(), Arrays.asList("sfld", "ifld", "dfld"));
	}

	@Test
	public void testDelegation() throws Exception {
		DBObject delegate = mock(DBObject.class);
		TestCollection1 c = new TestCollection1();
		c.markAsPartialObject();
		c.setDelegate(delegate);
		c.put("p1", null);
		c.putAll((Map) null);
		c.putAll((DBObject) null);
		c.get("p2");
		c.toMap();
		c.removeField("p3");
		c.containsField("p4");
		c.keySet();
		verify(delegate).markAsPartialObject();
		verify(delegate).put("p1", null);
		verify(delegate).putAll((Map) null);
		verify(delegate).putAll((DBObject) null);
		verify(delegate).get("p2");
		verify(delegate).toMap();
		verify(delegate).removeField("p3");
		verify(delegate).containsField("p4");
		verify(delegate).keySet();
	}

	@Test
	public void testNulls() throws Exception {
		TestCollection1 c = new TestCollection1();
		c.setNfld(TestCollection1.NULL);
		assertNull(c.get("nfld"));
		assertEquals(c.toMap().toString(), "{ 'nfld' :  null }".replace('\'', '"'));
		assertTrue(c.containsField("nfld"));
		assertEquals(c.keySet().toString(), "[nfld]");
	}
}
