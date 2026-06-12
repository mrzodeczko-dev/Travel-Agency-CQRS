package com.rzodeczko.e2e;

import io.restassured.http.ContentType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * Seeds hotels via REST API (POST /api/hotels) and waits
 * until they appear on the Query Side (GET /api/hotels/{id}),
 * covering the full CQRS flow (PostgreSQL + Outbox → Kafka → MongoDB).
 * Hotel IDs are collected from API responses (not assumed sequential).
 */
public final class HotelSeeder {

    static final int HOTEL_COUNT = 10;
    static final int DEFAULT_CAPACITY = 100;

    private static final Duration TIMEOUT = Duration.ofSeconds(E2EConfig.PROPAGATION_TIMEOUT_SECONDS);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);
    private static final Duration POLL_DELAY = Duration.ofSeconds(1);

    private static final List<Integer> hotelIds = new ArrayList<>();

    private HotelSeeder() {
    }

    static int getHotelId(int index) {
        return List.copyOf(hotelIds).get(index);
    }

    static void seedHotels() {
        if (!hotelIds.isEmpty()) {
            return;
        }

        for (int i = 0; i < HOTEL_COUNT; i++) {
            int hotelId = given()
                    .baseUri(E2EConfig.COMMAND_SIDE_URL)
                    .contentType(ContentType.JSON)
                    .body(Map.of("capacity", DEFAULT_CAPACITY))
                    .when()
                    .post("/api/hotels")
                    .then()
                    .statusCode(201)
                    .body("capacity", equalTo(DEFAULT_CAPACITY))
                    .extract()
                    .jsonPath().getInt("hotelId");

            hotelIds.add(hotelId);
        }

        // Wait for the last hotel to propagate through Kafka to the Query Side
        int lastId = hotelIds.getLast();
        await().pollDelay(POLL_DELAY).atMost(TIMEOUT).pollInterval(POLL_INTERVAL).untilAsserted(() ->
                given()
                        .baseUri(E2EConfig.QUERY_SIDE_URL)
                        .get("/api/hotels/" + lastId)
                        .then()
                        .statusCode(200)
                        .body("hotelId", equalTo(lastId))
                        .body("capacity", equalTo(DEFAULT_CAPACITY))
        );
    }
}
