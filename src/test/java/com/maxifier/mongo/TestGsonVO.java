/*
 * Copyright (c) 2008-2014 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.google.gson.*;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

import static org.testng.Assert.*;

/**
 * @author Konstantin Lyamshin (2014-12-05 12:13)
 */
@SuppressWarnings("AssertEqualsBetweenInconvertibleTypesTestNG")
public class TestGsonVO {
    public ObjectId _id;
    public int i;
    public double dbl;
    public String s;
    public String[] sArr;
    public String nullString;
    public GsonNullable<String> nullableString;
    public List<GsonNullable<String>> nullableList;
    public boolean boolTrue;
    public boolean boolFalse;
    public boolean[] boolArr;
    public Long l;
    public Date date;
    public byte[] binary;
    public ObjectId oid;
    public String uuid; // TODO: enable UUID
    public TestInnerVO inner;
    public TestInnerVO[] innerArr;
    public List<TestInnerVO> innerList;
    public TestGsonAdapted adapted;
    public TestGsonAdapted[] adaptedArr;
    public List<TestGsonAdapted> adaptedList;
    public TestGsonSerialized serialized;
    public TestGsonSerialized[] serializedArr;
    @SerializedName("serializedList\ud83d\ude21")
    public List<TestGsonSerialized> serializedList;

    public TestGsonVO initVO1() {
        i = 10;
        dbl = 0.1;
        s = "vo1";
        sArr = new String[]{"line 1", "line\0 2\uD83D\uDE21"};
        nullString = null;
        nullableString = GsonNullable.absent();
        nullableList = new ArrayList<GsonNullable<String>>();
        boolTrue = true;
        boolFalse = false;
        boolArr = new boolean[]{true, true, false, true};
        l = Long.MAX_VALUE;
        Calendar calendar = Calendar.getInstance();
        calendar.set(2014, Calendar.DECEMBER, 5, 19, 48, 32);
        date = calendar.getTime();
        binary = "Sample".getBytes();
        _id = new ObjectId("ABABABABABABABABABABABAB");
        oid = new ObjectId("BABABABABABABABABABABABA");
        uuid = UUID.fromString("aadf3303-4a9e-11e4-a4b0-fe6be46727c1").toString();
        inner = new TestInnerVO().init("Inner1", false, new TestGsonAdapted().init(Integer.class, "adapt1"));
        innerArr = new TestInnerVO[]{
            new TestInnerVO().init("Inner2", false, new TestGsonAdapted().init(Integer.class, "adapt2")),
            new TestInnerVO().init("Inner3", false, new TestGsonAdapted().init(Boolean.class, "adapt3")),
            new TestInnerVO().init("Inner4", true, new TestGsonAdapted().init(Long.class, "adapt4"))
        };
        innerList = Arrays.asList(
            new TestInnerVO().init("Inner5", false, new TestGsonAdapted().init(Boolean.class, "adapt5")),
            new TestInnerVO().init("Inner6", true, new TestGsonAdapted().init(Integer.class, "adapt6")),
            new TestInnerVO().init("Inner7", false, new TestGsonAdapted().init(Long.class, "adapt7"))
        );
        adapted = new TestGsonAdapted().init(Double.class, "Adapt8");
        adaptedArr = new TestGsonAdapted[]{
            new TestGsonAdapted().init(Double.class, "Adapt9"),
            new TestGsonAdapted().init(Character.class, "Adapt10"),
            new TestGsonAdapted().init(Byte.class, "Adapt11"),
        };
        adaptedList = Arrays.asList(
            new TestGsonAdapted().init(Short.class, "Adapt11"),
            new TestGsonAdapted().init(Long.class, null)
        );
        serialized = new TestGsonSerialized().init("ser1");
        serializedArr = new TestGsonSerialized[]{
            new TestGsonSerialized().init("ser1"),
            new TestGsonSerialized().init("ser2"),
            new TestGsonSerialized().init("ser3")
        };
        serializedList = Arrays.asList(
            new TestGsonSerialized().init("ser4"),
            new TestGsonSerialized().init("ser5")
        );

        return this;
    }

    public TestGsonVO initVO2() {
        i = 22;
        dbl = 7.7;
        s = "vo2";
        sArr = new String[]{"line 3", "line 4"};
        nullString = null;
        nullableString = GsonNullable.of("Nullable");
        nullableList = new ArrayList<GsonNullable<String>>();
        nullableList.add(GsonNullable.<String>absent());
        nullableList.add(GsonNullable.of("Present"));
        boolTrue = false;
        boolFalse = true;
        boolArr = new boolean[]{false, false};
        l = Long.MIN_VALUE + Integer.MAX_VALUE;
        Calendar calendar = Calendar.getInstance();
        calendar.set(2014, Calendar.DECEMBER, 5, 20, 0, 17);
        date = calendar.getTime();
        binary = "Mample".getBytes();
        _id = new ObjectId("BABECAFEBABECAFEBABECAFE");
        oid = new ObjectId("CAFEBABECAFEBABECAFEBABE");
        uuid = UUID.fromString("cafebabe-0000-0000-0000-000000000000").toString();
        inner = new TestInnerVO().init("In1", false, new TestGsonAdapted().init(Integer.class, "adpt1"));
        innerArr = new TestInnerVO[]{
            new TestInnerVO().init("In2", true, new TestGsonAdapted().init(Integer.class, "adpt2")),
            null, // Array with null
            new TestInnerVO().init("In4", true, new TestGsonAdapted().init(Long.class, "adpt4"))
        };
        innerList = Arrays.asList(
            new TestInnerVO().init("In5", false, new TestGsonAdapted().init(Boolean.class, "adpt5")),
            new TestInnerVO().init("In6", false, new TestGsonAdapted().init(Integer.class, "adpt6")),
            new TestInnerVO().init("In7", false, new TestGsonAdapted().init(Long.class, "adpt7"))
        );
        adapted = new TestGsonAdapted().init(Double.class, "Adpt8");
        adaptedArr = new TestGsonAdapted[]{
            new TestGsonAdapted().init(Double.class, "Adpt9"),
            new TestGsonAdapted().init(Character.class, "Adpt10"),
            new TestGsonAdapted().init(Byte.class, "Adpt11"),
        };
        adaptedList = Arrays.asList(
            new TestGsonAdapted().init(Short.class, "Adpt11"),
            new TestGsonAdapted().init(Long.class, "Adpt12")
        );
        serialized = new TestGsonSerialized().init("srlzd1");
        serializedArr = new TestGsonSerialized[]{
            new TestGsonSerialized().init("srlzd1"),
            new TestGsonSerialized().init("srlzd2"),
            new TestGsonSerialized().init("srlzd3")
        };
        serializedList = Arrays.asList(
            new TestGsonSerialized().init("srlzd4"),
            new TestGsonSerialized().init("srlzd5")
        );

        return this;
    }

    public TestGsonVO initBasic() {
        i = 7;
        dbl = 7.7;
        s = "basic";
        nullString = null;
        boolTrue = true;
        boolFalse = false;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestGsonVO that = (TestGsonVO) o;
        return boolFalse == that.boolFalse
            && boolTrue == that.boolTrue
            && i == that.i
            && l.equals(that.l)
            && Double.compare(that.dbl, dbl) == 0
            && equal(adapted, that.adapted)
            && Arrays.equals(adaptedArr, that.adaptedArr)
            && equal(adaptedList, that.adaptedList)
            && Arrays.equals(binary, that.binary)
            && Arrays.equals(boolArr, that.boolArr)
            && equal(date, that.date)
            && equal(inner, that.inner)
            && Arrays.equals(innerArr, that.innerArr)
            && equal(innerList, that.innerList)
            && equal(nullString, that.nullString)
            && equal(oid, that.oid)
            && equal(s, that.s)
            && Arrays.equals(sArr, that.sArr)
            && equal(uuid, that.uuid);
    }

    private static boolean equal(Object a, Object b) {
        return a == b || (a != null && a.equals(b));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[]{i, dbl, s, nullString, boolTrue, boolFalse, l, date, oid, uuid, inner, innerList, adapted, adaptedList})
            + Arrays.hashCode(sArr)
            + Arrays.hashCode(boolArr)
            + Arrays.hashCode(binary)
            + Arrays.hashCode(innerArr)
            + Arrays.hashCode(adaptedArr);
    }

    @Override
    public String toString() {
        return s;
    }

    public DBObject toBson() {
        BasicDBObject bson = new BasicDBObject();
        bson.put("i", i);
        bson.put("dbl", dbl);
        bson.put("s", s);
        bson.put("sArr", sArr != null? asList(sArr): null);
        bson.put("nullString", nullString);
        bson.put("boolTrue", boolTrue);
        bson.put("boolFalse", boolFalse);
        bson.put("boolArr", boolArr != null? asList(boolArr): null);
        bson.put("l", l);
        bson.put("date", date);
        bson.put("binary", binary);
        bson.put("oid", oid);
        bson.put("uuid", uuid);
        bson.put("inner", inner != null? inner.toBson(): null);
        bson.put("innerArr", innerArr != null? toInnerList(asList(innerArr)): null);
        bson.put("innerList", innerList != null? toInnerList(innerList): null);
        bson.put("adapted", adapted != null? adapted.toBson(): null);
        bson.put("adaptedArr", adaptedArr != null? toAdaptedList(asList(adaptedArr)): null);
        bson.put("adaptedList", adaptedList != null? toAdaptedList(adaptedList): null);
        bson.put("serialized", serialized != null? serialized.toBson(): null);
        bson.put("serializedArr", serializedArr != null? toSerializedList(asList(serializedArr)): null);
        bson.put("serializedList\uD83D\uDE21", serializedList != null? toSerializedList(serializedList): null);
        for (Iterator<Object> it = bson.values().iterator(); it.hasNext(); ) {
            if (it.next() == null) {
                it.remove();
            }
        }
        if (nullableString != null) {
            bson.put("nullableString", nullableString.orNull());
        }
        return bson;
    }

    public void assertBson(DBObject bson) {
        assertEquals(bson.get("i"), i);
        assertEquals(bson.get("dbl"), dbl);
        assertEquals(bson.get("s"), s);
        assertEquals(bson.get("sArr"), sArr != null? asList(sArr): null);
        assertEquals(bson.containsField("nullString"), nullString != null);
        assertEquals(bson.containsField("nullableString"), nullableString != null);
        assertEquals(bson.get("nullString"), nullString);
        assertEquals(bson.containsField("nullableString"), nullableString != null);
        assertEquals(bson.get("nullableString"), nullableString != null? nullableString.orNull(): null);
        assertEquals(bson.get("boolTrue"), boolTrue);
        assertEquals(bson.get("boolFalse"), boolFalse);
        assertEquals(bson.get("boolArr"), boolArr != null? asList(boolArr): null);
        assertEquals(bson.get("l"), l);
        assertEquals(bson.get("date"), date);
        assertEquals(bson.get("binary"), binary);
        assertEquals(bson.get("oid"), oid);
        assertEquals(bson.get("uuid"), uuid);

        assertInner((DBObject) bson.get("inner"), inner);
        assertInner((BasicDBList) bson.get("innerArr"), innerArr != null? asList(innerArr): null);
        assertInner((BasicDBList) bson.get("innerList"), innerList);

        assertAdapted((DBObject) bson.get("adapted"), adapted);
        assertAdapted((BasicDBList) bson.get("adaptedArr"), adaptedArr != null? asList(adaptedArr): null);
        assertAdapted((BasicDBList) bson.get("adaptedList"), adaptedList);

        assertSerialized((DBObject) bson.get("serialized"), serialized);
        assertSerialized((BasicDBList) bson.get("serializedArr"), serializedArr != null? asList(serializedArr): null);
        assertSerialized((BasicDBList) bson.get("serializedList\uD83D\uDE21"), serializedList);
    }

    public void assertGson(TestGsonVO o) {
        assertEquals(o.i, i);
        assertEquals(o.dbl, dbl);
        assertEquals(o.s, s);
        assertEquals(o.sArr, sArr);
        assertEquals(o.nullString, nullString);
        assertEquals(o.nullableString, nullableString);
        assertEquals(o.boolTrue, boolTrue);
        assertEquals(o.boolFalse, boolFalse);
        assertEquals(o.boolArr, boolArr);
        assertEquals(o.l, l);
        assertEquals(o.date, date);
        assertEquals(o.binary, binary);
        assertEquals(o.oid, oid);
        assertEquals(o.uuid, uuid);
        assertEquals(o.inner, inner);
        assertEquals(o.innerArr, innerArr);
        assertEquals(o.innerList, innerList);
        assertEquals(o.adapted, adapted);
        assertEquals(o.adaptedArr, adaptedArr);
        assertEquals(o.adaptedList, adaptedList);
        assertEquals(o.serialized, serialized);
        assertEquals(o.serializedArr, serializedArr);
        assertEquals(o.serializedList, serializedList);
    }

    public static void assertInner(DBObject bson, TestGsonVO.TestInnerVO vo) {
        if (bson == null && vo == null) {
            return;
        }
        assertNotNull(bson);
        assertNotNull(vo);
        assertEquals(bson.get("s"), vo.s);
        assertEquals(bson.get("bool"), vo.bool);
        assertAdapted((DBObject) bson.get("adapted"), vo.adapted);
    }

    public static void assertInner(BasicDBList list, List<TestGsonVO.TestInnerVO> vos) {
        if (list == null && vos == null) {
            return;
        }
        assertNotNull(list);
        assertNotNull(vos);
        Iterator<Object> it1 = list.iterator();
        Iterator<TestGsonVO.TestInnerVO> it2 = vos.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            assertInner((DBObject) it1.next(), it2.next());
        }
        assertEquals(it1.hasNext(), it2.hasNext());
    }

    public static void assertAdapted(DBObject bson, TestGsonVO.TestGsonAdapted vo) {
        if (bson == null && vo == null) {
            return;
        }
        assertNotNull(bson);
        assertNotNull(vo);
        assertEquals(bson.get("cls"), vo.clazz.getName());
        assertEquals(bson.containsField("id"), true);
        assertEquals(bson.get("id"), vo.id);
    }

    public static void assertAdapted(BasicDBList list, List<TestGsonVO.TestGsonAdapted> vos) {
        if (list == null && vos == null) {
            return;
        }
        assertNotNull(list);
        assertNotNull(vos);
        Iterator<Object> it1 = list.iterator();
        Iterator<TestGsonVO.TestGsonAdapted> it2 = vos.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            assertAdapted((DBObject) it1.next(), it2.next());
        }
        assertEquals(it1.hasNext(), it2.hasNext());
    }

    public static void assertSerialized(DBObject bson, TestGsonVO.TestGsonSerialized vo) {
        if (bson == null && vo == null) {
            return;
        }
        assertNotNull(bson);
        assertNotNull(vo);
        assertEquals(bson.get("id"), vo.id);
    }

    public static void assertSerialized(BasicDBList list, List<TestGsonVO.TestGsonSerialized> vos) {
        if (list == null && vos == null) {
            return;
        }
        assertNotNull(list);
        assertNotNull(vos);
        Iterator<Object> it1 = list.iterator();
        Iterator<TestGsonVO.TestGsonSerialized> it2 = vos.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            assertSerialized((DBObject) it1.next(), it2.next());
        }
        assertEquals(it1.hasNext(), it2.hasNext());
    }

    public static <T> List<T> asList(T... list) {
        ArrayList<T> result = new ArrayList<T>(list.length);
        Collections.addAll(result, list);
        return result;
    }

    public static List<Boolean> asList(boolean[] list) {
        ArrayList<Boolean> result = new ArrayList<Boolean>(list.length);
        for (boolean b : list) {
            result.add(b);
        }
        return result;
    }

    private static List<DBObject> toInnerList(List<TestGsonVO.TestInnerVO> list) {
        ArrayList<DBObject> result = new ArrayList<DBObject>(list.size());
        for (TestInnerVO vo : list) {
            result.add(vo != null? vo.toBson(): null);
        }
        return result;
    }

    private static List<DBObject> toAdaptedList(List<TestGsonVO.TestGsonAdapted> list) {
        ArrayList<DBObject> result = new ArrayList<DBObject>(list.size());
        for (TestGsonAdapted vo : list) {
            result.add(vo.toBson());
        }
        return result;
    }

    private static List<DBObject> toSerializedList(List<TestGsonVO.TestGsonSerialized> list) {
        ArrayList<DBObject> result = new ArrayList<DBObject>(list.size());
        for (TestGsonSerialized vo : list) {
            result.add(vo.toBson());
        }
        return result;
    }

    public static class TestInnerVO {
        public String s;
        public boolean bool;
        public TestGsonAdapted adapted;

        public TestInnerVO init(String s, boolean bool, TestGsonAdapted adapted) {
            this.s = s;
            this.bool = bool;
            this.adapted = adapted;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestInnerVO that = (TestInnerVO) o;
            return bool == that.bool && adapted.equals(that.adapted) && s.equals(that.s);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(new Object[]{s, bool, adapted});
        }

        @Override
        public String toString() {
            return String.format("TestInnerVO{s='%s', bool=%s, adapted=%s}", s, bool, adapted);
        }

        public DBObject toBson() {
            BasicDBObject bson = new BasicDBObject();
            bson.put("s", s);
            bson.put("bool", bool);
            bson.put("adapted", adapted.toBson());
            return bson;
        }
    }

    public static class TestGsonAdapted {
        public Class clazz;
        public String id;

        public TestGsonAdapted init(Class clazz, String id) {
            this.clazz = clazz;
            this.id = id;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestGsonAdapted that = (TestGsonAdapted) o;
            return clazz.equals(that.clazz) && equal(id, that.id);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(new Object[]{clazz.getName(), id});
        }

        @Override
        public String toString() {
            return String.format("TestGsonAdapted{clazz=%s, id='%s'}", clazz != null? clazz.getName(): null, id);
        }

        public DBObject toBson() {
            BasicDBObject bson = new BasicDBObject();
            bson.put("cls", clazz.getName());
            bson.put("id", id);
            return bson;
        }
    }

    public static class TestGsonAdapter extends TypeAdapter<TestGsonAdapted> {
        private final TypeAdapter<GsonNullable<String>> nullableString;

        public TestGsonAdapter(TypeAdapter<GsonNullable<String>> nullableString) {
            this.nullableString = nullableString;
        }

        @Override
        public void write(JsonWriter out, TestGsonAdapted value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.beginObject();
                out.name("cls").value(value.clazz.getName());
                nullableString.write(out.name("id"), GsonNullable.fromNullable(value.id));
                out.endObject();
            }
        }

        @Override
        public TestGsonAdapted read(JsonReader in) throws IOException {
            TestGsonAdapted value = new TestGsonAdapted();
            in.beginObject();
            String n1 = in.nextName();
            if (!"cls".equals(n1)) {
                throw new JsonSyntaxException("Expected cls, but got " + n1);
            }
            String clazz = in.nextString();
            try {
                value.clazz = Class.forName(clazz);
            } catch (ClassNotFoundException e) {
                throw new JsonSyntaxException("Invalid clazz " + clazz);
            }
            String n2 = in.nextName();
            if (!"id".equals(n2)) {
                throw new JsonSyntaxException("Expected id, but got " + n2);
            }
            value.id = nullableString.read(in).orNull();
            in.endObject();
            return value;
        }
    }

    public static class TestGsonAdapterFactory implements TypeAdapterFactory {
        @Override
        @SuppressWarnings("unchecked") // checked by reflection
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            if (!TestGsonAdapted.class.isAssignableFrom(type.getRawType())) {
                return null;
            }
            TypeToken<GsonNullable<String>> nullableString = GsonNullable.getNullableType(TypeToken.get(String.class));
            return (TypeAdapter<T>) new TestGsonAdapter(gson.getAdapter(nullableString));
        }
    }

    public static class TestGsonSerialized {
        public String id;

        public TestGsonSerialized init(String id) {
            this.id = id;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestGsonSerialized that = (TestGsonSerialized) o;
            return id.equals(that.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public String toString() {
            return String.format("TestGsonSerialized{id='%s'}", id);
        }

        public DBObject toBson() {
            BasicDBObject bson = new BasicDBObject();
            bson.put("id", id);
            return bson;
        }
    }

    public static class TestGsonSerializer implements JsonSerializer<TestGsonSerialized>, JsonDeserializer<TestGsonSerialized> {
        @Override
        public JsonElement serialize(TestGsonSerialized src, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject o = new JsonObject();
            o.addProperty("id", src.id);
            return o;
        }

        @Override
        public TestGsonSerialized deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            TestGsonSerialized value = new TestGsonSerialized();
            value.id = json.getAsJsonObject().getAsJsonPrimitive("id").getAsString();
            return value;
        }
    }
}
