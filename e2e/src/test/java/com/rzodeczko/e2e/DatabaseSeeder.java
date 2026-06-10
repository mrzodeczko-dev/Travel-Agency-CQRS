package com.rzodeczko.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static com.mongodb.client.model.Filters.eq;

/**
 * Seeds the command-side PostgreSQL and query-side MongoDB databases with test hotels.
 */
public class DatabaseSeeder {

    private static final int HOTEL_COUNT = 10;
    private static final int DEFAULT_CAPACITY = 100;

    private DatabaseSeeder() {
    }

    static void seedHotels() {
        seedPostgresHotels();
        seedMongoHotels();
    }

    private static void seedPostgresHotels() {
        try (Connection conn = DriverManager.getConnection(
                E2EConfig.POSTGRES_URL, E2EConfig.POSTGRES_USER, E2EConfig.POSTGRES_PASSWORD);
             Statement stmt = conn.createStatement()) {

            for (int i = 1; i <= HOTEL_COUNT; i++) {
                stmt.executeUpdate(
                        "INSERT INTO hotels (id, capacity) VALUES (%d, %d) ON CONFLICT (id) DO NOTHING"
                                .formatted(i, DEFAULT_CAPACITY)
                );
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to seed hotels in PostgreSQL: " + e.getMessage(), e);
        }
    }

    private static void seedMongoHotels() {
        try (MongoClient client = MongoClients.create(E2EConfig.MONGO_URI)) {
            MongoDatabase db = client.getDatabase("travels_read_db");
            MongoCollection<Document> hotels = db.getCollection("hotels");

            for (int i = 1; i <= HOTEL_COUNT; i++) {
                Document hotel = new Document("_id", (long) i)
                        .append("capacity", (long) DEFAULT_CAPACITY);
                hotels.replaceOne(eq("_id", (long) i), hotel, new ReplaceOptions().upsert(true));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to seed hotels in MongoDB: " + e.getMessage(), e);
        }
    }
}
