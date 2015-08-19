/*
 * Copyright (c) 2008-2013 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.*;

/**
 * DBVariantSerializerTest
 *
 * @author Konstantin Lyamshin (konstantin.lyamshin@maxifier.com) (2012-09-01 15:30)
 */
public class DBVariantSerializerTest {
	@DBObjectFieldOrder({ "name", "tags", "bugs" })
	public static class Variant extends DBVariantSerializer {
		private String name;
		private List<?> tags;
		private String bugs;
		private Object nulls;

		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public List<?> getTags() {
			return tags;
		}
		public void setTags(List<?> tags) {
			this.tags = tags;
		}
		public String getBugs() {
			return bugs;
		}
		public void setBugs(String bugs) {
			this.bugs = bugs;
		}
		public Object getNulls() {
			return nulls;
		}
		public void setNulls(Object nulls) {
			this.nulls = nulls;
		}
	}

	@Test
	public void testPut() throws Exception {
		BasicDBList tags = new BasicDBList();
		tags.add("one");
		tags.add("two");
		tags.add("thr");

		Variant v = new Variant();
		v.put("name", "variant");
		v.put("comment", new BasicDBObject("author", "bob"));
		v.put("date", "20.12.2012");
		v.put("bugs", "none");
		v.put("tags", tags);
		assertEquals(v.toString(), "{ 'name' : 'variant' , 'tags' : [ 'one' , 'two' , 'thr'] , 'bugs' : 'none' , 'comment' : { 'author' : 'bob'} , 'date' : '20.12.2012'}".replace('\'', '"'));
		assertEquals(v.getName(), "variant");
		assertEquals(v.getTags().toString(), tags.toString());
		assertEquals(v.getBugs(), "none");
	}

	@Test(expectedExceptions = MongoSerializationException.class)
	public void testPutTyped() throws Exception {
		Variant v = new Variant();
		v.put("name", 0);
	}

	@Test
	public void testGet() throws Exception {
		BasicDBList tags = new BasicDBList();
		tags.add("one");
		tags.add("two");
		tags.add("thr");

		Variant v = new Variant();
		v.setName("variant");
		v.put("comment", new BasicDBObject("author", "bob"));
		v.put("date", "20.12.2012");
		v.setBugs("none");
		v.setTags(tags);

		assertEquals(v.get("name"), "variant");
		assertEquals(v.get("comment").toString(), "{ 'author' : 'bob'}".replace('\'', '"'));
		assertEquals(v.get("date"), "20.12.2012");
		assertEquals(v.get("bugs"), "none");
		assertEquals(v.get("tags").toString(), tags.toString());
		assertNull(v.get("none"));
	}

	@Test
	public void testContainsField() throws Exception {
		Variant v = new Variant();
		assertFalse(v.containsField("name"));
		assertFalse(v.containsField("date"));
		v.put("name", "variant");
		v.put("date", "20.12.2012");
		assertTrue(v.containsField("name"));
		assertTrue(v.containsField("date"));
	}

	@Test
	public void testKeySet() throws Exception {
		Variant v = new Variant();
		assertEquals(v.keySet().toArray(), new String[] {});
		v.setBugs("none");
		assertEquals(v.keySet().toArray(), new String[]{"bugs"});
		v.put("date", "20.12.2012");
		assertEquals(v.keySet().toArray(), new String[]{"bugs", "date"});
		v.setName("variant");
		assertEquals(v.keySet().toArray(), new String[] { "name", "bugs", "date" });
	}

	@Test
	public void testToMap() throws Exception {
		Variant v = new Variant();
		assertEquals(v.toMap().toString(), "{ }");
		v.setBugs("none");
		assertEquals(v.toMap().toString(), "{ 'bugs' : 'none'}".replace('\'', '"'));
		v.put("date", "20.12.2012");
		assertEquals(v.toMap().toString(), "{ 'bugs' : 'none' , 'date' : '20.12.2012'}".replace('\'', '"'));
		v.setName("variant");
		assertEquals(v.toMap().toString(), "{ 'name' : 'variant' , 'bugs' : 'none' , 'date' : '20.12.2012'}".replace('\'', '"'));
	}

	@Test
	public void testRemoveField() throws Exception {
		Variant v = new Variant();
		v.setName("variant");
		v.put("date", "20.12.2012");
		v.setBugs("none");
		assertEquals(v.toMap().toString(), "{ 'name' : 'variant' , 'bugs' : 'none' , 'date' : '20.12.2012'}".replace('\'', '"'));

		v.removeField("name");
		assertEquals(v.toMap().toString(), "{ 'bugs' : 'none' , 'date' : '20.12.2012'}".replace('\'', '"'));

		v.removeField("date");
		assertEquals(v.toMap().toString(), "{ 'bugs' : 'none'}".replace('\'', '"'));

		v.removeField("bugs");
		assertEquals(v.toMap().toString(), "{ }");
	}

	@Test
	public void testDelegation() throws Exception {
		DBObject delegate = mock(DBObject.class);
		Variant c = new Variant();
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
		Variant v = new Variant();
		v.setNulls(Variant.NULL);
		assertNull(v.get("nulls"));
		assertEquals(v.toMap().toString(), "{ \"nulls\" :  null }");
		assertTrue(v.containsField("nulls"));
		assertEquals(v.keySet().toString(), "[nulls]");
	}
}
