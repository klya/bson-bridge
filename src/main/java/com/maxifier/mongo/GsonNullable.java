/*
 * Copyright (c) 2008-2015 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.google.gson.internal.$Gson$Types;
import com.google.gson.reflect.TypeToken;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.ParameterizedType;

/**
 * Special value wrapper that propagates Null values to Mongo.
 *
 * @author Konstantin Lyamshin (2015-01-22 11:20)
 */
public abstract class GsonNullable<T> {
    private static final Absent ABSENT = new Absent();

    @SuppressWarnings("unchecked")
    public static <T> GsonNullable<T> absent() {
        return ABSENT;
    }

    public static <T> GsonNullable<T> of(@Nonnull T value) {
        if (value == null) {
            throw new NullPointerException("Value required");
        }
        return new Present<T>(value);
    }

    public static <T> GsonNullable<T> fromNullable(@Nullable T value) {
        return value == null? GsonNullable.<T>absent(): new Present<T>(value);
    }

    /**
     * Special helper routine to provide nullable TypeToken for specified Type.
     *
     * @param type type token of T
     * @param <T> desired nullable type
     * @return nullable type token
     */
    @SuppressWarnings("unchecked") // correct because of GsonNullable structure
    public static <T> TypeToken<GsonNullable<T>> getNullableType(TypeToken<T> type) {
        ParameterizedType nullableType = $Gson$Types.newParameterizedTypeWithOwner(
            GsonNullable.class.getDeclaringClass(), GsonNullable.class, type.getType()
        );
        return (TypeToken<GsonNullable<T>>) TypeToken.get(nullableType);
    }

    /**
     * Special helper routine to provide nullable TypeToken for specified Type.
     *
     * @param type class of T
     * @param <T> desired nullable type
     * @return nullable type token
     */
    public static <T> TypeToken<GsonNullable<T>> getNullableType(Class<T> type) {
        return getNullableType(TypeToken.get(type));
    }

    private GsonNullable() {}

    public abstract boolean isPresent();

    @Nonnull
    public abstract T get();

    @Nullable
    public abstract T orNull();

    @Override
    public abstract boolean equals(@Nullable Object object);

    @Override
    public abstract int hashCode();

    @Override
    public abstract String toString();

    private static final class Absent extends GsonNullable {
        @Override
        public boolean isPresent() {
            return false;
        }

        @Nonnull
        @Override
        public Object get() {
            throw new IllegalStateException("Value absent within GsonNullable, call isEmpty() first");
        }

        @Nullable
        @Override
        public Object orNull() {
            return null;
        }

        @Override
        public boolean equals(@Nullable Object object) {
            return object == this;
        }

        @Override
        public int hashCode() {
            return 0X6CAEFA2F;
        }

        @Override
        public String toString() {
            return "Null";
        }
    }

    private static final class Present<T> extends GsonNullable<T> {
        private final T value;

        private Present(T value) {
            this.value = value;
        }

        @Override
        public boolean isPresent() {
            return true;
        }

        @Nonnull
        @Override
        public T get() {
            return value;
        }

        @Nullable
        @Override
        public T orNull() {
            return value;
        }

        @Override
        public boolean equals(@Nullable Object object) {
            return (object instanceof Present) && (value.equals(((Present) object).value));
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }
}
