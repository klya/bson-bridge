/*
 * Copyright (c) 2008-2013 Maxifier Ltd. All Rights Reserved.
 */
package com.maxifier.mongo;

import com.google.common.base.Strings;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.net.SocketFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.GregorianCalendar;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * MongoProvider
 * <p/>
 * This class provides MongoDB connection to other classes.
 *
 * @author Konstantin Lyamshin (konstantin.lyamshin@maxifier.com) (2012-08-24 16:43)
 */
@Singleton
public class MongoProvider implements Provider<Mongo> {
    private static final Logger logger = LoggerFactory.getLogger(MongoProvider.class);
    private static final int POOL_SIZE = 10;
    private static final int WAIT_MULTIPLIER = 20;

    public static final String BADCHARS = "[/\\. \"*<>:|?]";

    private final Mongo mongo;

    @Inject
    public MongoProvider(@Named("mongo.connection.url") String mongoURI) throws UnknownHostException {
        // Turn off proxy for mongoDB connections
        SocketFactory socketFactoryNoProxy = new SocketFactory() {
            public Socket createSocket() throws IOException {
                return new Socket(Proxy.NO_PROXY);
            }

            public Socket createSocket(String host, int port) throws IOException {
                throw new UnsupportedOperationException("Overrided SocketFactory implementation don't implement connected sockets");
            }

            public Socket createSocket(String host, int port, InetAddress clientAddress, int clientPort) throws IOException {
                throw new UnsupportedOperationException("Overrided SocketFactory implementation don't implement connected sockets");
            }

            public Socket createSocket(InetAddress address, int port) throws IOException {
                throw new UnsupportedOperationException("Overrided SocketFactory implementation don't implement connected sockets");
            }

            public Socket createSocket(InetAddress address, int port, InetAddress clientAddress, int clientPort) throws IOException {
                throw new UnsupportedOperationException("Overrided SocketFactory implementation don't implement connected sockets");
            }
        };

        // Initialize default options
        MongoClientOptions.Builder options = new MongoClientOptions.Builder()
            .connectionsPerHost(POOL_SIZE)
            .threadsAllowedToBlockForConnectionMultiplier(WAIT_MULTIPLIER)
            .socketFactory(socketFactoryNoProxy);

        MongoClientURI uri = new MongoClientURI(mongoURI, options);

        logger.debug("Connecting to mongoDB at {}", uri);
        this.mongo = new MongoClient(uri);
    }

    @Override
    public Mongo get() {
        return mongo;
    }

    private static final long GREGORIAN_CUTOVER = MILLISECONDS.toSeconds(new GregorianCalendar().getGregorianChange().getTime());

    /**
     * Converts {@link org.bson.types.ObjectId} value to nearly correct {@link java.util.UUID} value.
     *
     * @param id object id.
     * @return object UDDI.
     */
    public static UUID toUUID(ObjectId id) {
        byte[] b = id.toByteArray();
        long time = (id.getTimestamp() - GREGORIAN_CUTOVER) * 10000000L;
        time |= (b[4] >> 6) & 0x3; // store two most significant bits from machine field in time

        long mostSigBits = 0x1000L // version
            | time >> 48 & 0x0FFFL // time_hi
            | time >> 16 & 0xFFFF0000L // time_mid
            | time << 32; // time_low
        long leastSigBits = 2L << 62 // variant
            | (b[4] & 0x3FL) << 56 | (b[5] & 0xFFL) << 48
            | (b[6] & 0xFFL) << 40 | (b[7] & 0xFFL) << 32
            | (b[8] & 0xFFL) << 24 | (b[9] & 0xFFL) << 16
            | (b[10] & 0xFFL) << 8 | b[11] & 0xFFL;

        return new UUID(mostSigBits, leastSigBits);
    }

    /**
     * Converts value previously returned by {@link #toUUID(org.bson.types.ObjectId)} to correct {@link org.bson.types.ObjectId} value.
     *
     * @param uuid value returned by link #toUUID(org.bson.types.ObjectId)}.
     * @return object id.
     * @throws IllegalArgumentException uuid is not returned by {@link #toUUID(org.bson.types.ObjectId)}
     */
    public static ObjectId toObjectId(UUID uuid) throws IllegalArgumentException {
        long mostSigBits = uuid.getMostSignificantBits();
        long time = (mostSigBits & 0x0FFFL) << 48
            | (mostSigBits & 0xFFFF0000L) << 16
            | (mostSigBits >>> 32);

        byte[] b = new byte[12];
        long leastSigBits = uuid.getLeastSignificantBits();
        for (int i = 11; i >= 4; i--, leastSigBits >>= Byte.SIZE) {
            b[i] = (byte) leastSigBits;
        }

        // restore two most significant machine bits from time
        b[4] = (byte) (b[4] & 0x3F | ((int) time << 6));

        // check lower bits to make sure of uuid origin (correct time are multiplied by ten million)
        if (time % 10000000L >> 2 != 0) {
            throw new IllegalArgumentException("Invalid uuid");
        }

        time = time / 10000000L + GREGORIAN_CUTOVER;
        for (int i = 3; i >= 0; i--, time >>= Byte.SIZE) {
            b[i] = (byte) time;
        }

        return new ObjectId(b);
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Program usage: main (OID|UUID) [(OID|UUID)...]");
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            System.out.print("What to convert: ");
            String s = reader.readLine();
            while (!Strings.isNullOrEmpty(s)) {
                convert(s);
                System.out.print("What to convert: ");
                s = reader.readLine();
            }
            return;
        }

        for (String arg : args) {
            convert(arg);
        }
    }

    private static void convert(String value) {
        try {
            System.out.printf("UUID(\"%s\") is ObjectId(\"%s\")%n", value, toObjectId(UUID.fromString(value)).toString());
            return;
        } catch (IllegalArgumentException ignored) {
        }
        try {
            System.out.printf("ObjectId(\"%s\") is UUID(\"%s\")%n", value, toUUID(new ObjectId(value)).toString());
            return;
        } catch (IllegalArgumentException ignored) {
        }

        System.out.printf("I don't know what \"%s\" is%n", value);
    }
}
