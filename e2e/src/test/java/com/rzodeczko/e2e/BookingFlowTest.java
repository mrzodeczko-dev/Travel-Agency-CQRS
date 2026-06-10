package com.rzodeczko.e2e;

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;

/**
 * E2E smoke tests — full CQRS booking flow.
 * <p>
 * Tests run against the live stack ({@code docker compose up}).
 * The flow exercises the entire event pipeline:
 * Command Side (REST) → PostgreSQL → Outbox → Kafka → Streams → MongoDB → Query Side (REST)
 * <p>
 * Availability assertions use Awaitility polling to account for
 * asynchronous Kafka event propagation.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BookingFlowTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(E2EConfig.PROPAGATION_TIMEOUT_SECONDS);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);
    private static final Duration POLL_DELAY = Duration.ofSeconds(3);

    @BeforeAll
    static void seedTestData() {
        DatabaseSeeder.seedHotels();
    }


    @Test
    @Order(1)
    @DisplayName("POST /api/bookings returns 201 with bookingId")
    void createBookingReturns201() {
        given()
                .baseUri(E2EConfig.COMMAND_SIDE_URL)
                .contentType(ContentType.JSON)
                .body(booking(1, 100, "2027-01-10", "2027-01-12"))
                .when()
                .post("/api/bookings")
                .then()
                .statusCode(201)
                .body("bookingId", notNullValue())
                .body("bookingId", instanceOf(Number.class));
    }

    @Test
    @Order(2)
    @DisplayName("Full booking lifecycle: create → propagation → cancel → propagation")
    void fullBookingLifecycle() {
        int hotelId = 2;

        // 1. Create booking
        int bookingId = createBooking(hotelId, 101, "2027-02-01", "2027-02-03");

        // 2. Wait for availability to appear (create event propagates)
        await().pollDelay(POLL_DELAY).atMost(TIMEOUT).pollInterval(POLL_INTERVAL).untilAsserted(() ->
                given()
                        .baseUri(E2EConfig.QUERY_SIDE_URL)
                        .queryParam("from", "2027-02-01")
                        .queryParam("to", "2027-02-03")
                        .get("/api/availability/" + hotelId)
                        .then()
                        .statusCode(200)
                        .body("content", not(empty()))
                        .body("content[0].hotelId", equalTo(hotelId))
                        .body("content[0].occupied", greaterThanOrEqualTo(1))
                        .body("content[0].status", anyOf(
                                equalTo("AVAILABLE"),
                                equalTo("LAST_ROOMS"),
                                equalTo("SOLD_OUT")
                        ))
        );

        // 3. Record occupancy before cancel
        int occupiedBefore = given()
                .baseUri(E2EConfig.QUERY_SIDE_URL)
                .queryParam("from", "2027-02-01")
                .queryParam("to", "2027-02-03")
                .get("/api/availability/" + hotelId)
                .jsonPath().getInt("content[0].occupied");

        // 4. Cancel booking
        given()
                .baseUri(E2EConfig.COMMAND_SIDE_URL)
                .delete("/api/bookings/" + bookingId)
                .then()
                .statusCode(204);

        // 5. Wait for occupancy to decrease (cancel event propagates)
        await().pollDelay(POLL_DELAY).atMost(TIMEOUT).pollInterval(POLL_INTERVAL).untilAsserted(() ->
                given()
                        .baseUri(E2EConfig.QUERY_SIDE_URL)
                        .queryParam("from", "2027-02-01")
                        .queryParam("to", "2027-02-03")
                        .get("/api/availability/" + hotelId)
                        .then()
                        .statusCode(200)
                        .body("content[0].occupied", lessThan(occupiedBefore))
        );
    }

    @Test
    @Order(3)
    @DisplayName("DELETE /api/bookings/{id} returns 204")
    void cancelBookingReturns204() {
        int bookingId = createBooking(3, 102, "2027-03-01", "2027-03-02");

        given()
                .baseUri(E2EConfig.COMMAND_SIDE_URL)
                .when()
                .delete("/api/bookings/" + bookingId)
                .then()
                .statusCode(204);
    }

    @Test
    @Order(4)
    @DisplayName("Double cancel returns 409 Conflict")
    void doubleCancelReturns409() {
        int bookingId = createBooking(4, 103, "2027-04-01", "2027-04-02");

        // First cancel
        given()
                .baseUri(E2EConfig.COMMAND_SIDE_URL)
                .delete("/api/bookings/" + bookingId)
                .then()
                .statusCode(204);

        // Second cancel — conflict
        given()
                .baseUri(E2EConfig.COMMAND_SIDE_URL)
                .when()
                .delete("/api/bookings/" + bookingId)
                .then()
                .statusCode(409);
    }

    @Test
    @Order(5)
    @DisplayName("POST /api/bookings with missing fields returns 400")
    void missingFieldsReturns400() {
        given()
                .baseUri(E2EConfig.COMMAND_SIDE_URL)
                .contentType(ContentType.JSON)
                .body("{}")
                .when()
                .post("/api/bookings")
                .then()
                .statusCode(400);
    }

    @Test
    @Order(6)
    @DisplayName("DELETE /api/bookings/{nonexistent} returns 404")
    void cancelNonexistentReturns404() {
        given()
                .baseUri(E2EConfig.COMMAND_SIDE_URL)
                .when()
                .delete("/api/bookings/999999999")
                .then()
                .statusCode(404);
    }

    @Test
    @Order(7)
    @DisplayName("GET /api/availability with invalid date range returns 400")
    void invalidDateRangeReturns400() {
        given()
                .baseUri(E2EConfig.QUERY_SIDE_URL)
                .queryParam("from", "2027-06-10")
                .queryParam("to", "2027-06-01")
                .when()
                .get("/api/availability/1")
                .then()
                .statusCode(400);
    }

    @Test
    @Order(8)
    @DisplayName("GET /api/availability returns correct paged response shape")
    void availabilityResponseShape() {
        given()
                .baseUri(E2EConfig.QUERY_SIDE_URL)
                .queryParam("from", "2099-01-01")
                .queryParam("to", "2099-01-02")
                .when()
                .get("/api/availability/999999")
                .then()
                .statusCode(200)
                .body("content", instanceOf(List.class))
                .body("page", notNullValue())
                .body("size", notNullValue())
                .body("totalElements", notNullValue())
                .body("totalPages", notNullValue());
    }

    private static int createBooking(int hotelId, int userId, String start, String end) {
        return given()
                .baseUri(E2EConfig.COMMAND_SIDE_URL)
                .contentType(ContentType.JSON)
                .body(booking(hotelId, userId, start, end))
                .when()
                .post("/api/bookings")
                .then()
                .statusCode(201)
                .extract()
                .jsonPath().getInt("bookingId");
    }

    private static Map<String, Object> booking(int hotelId, int userId, String start, String end) {
        return Map.of(
                "hotelId", hotelId,
                "userId", userId,
                "start", start,
                "end", end
        );
    }
}
