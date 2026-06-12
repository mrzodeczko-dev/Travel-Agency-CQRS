package com.rzodeczko.e2e;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

/**
 * Smoke tests -verify both services are up and responding.
 */
class HealthCheckTest {

    @Test
    @DisplayName("Command Side /actuator/health returns UP")
    void commandSideIsHealthy() {
        given()
                .baseUri(E2EConfig.COMMAND_SIDE_URL)
        .when()
                .get("/actuator/health")
        .then()
                .statusCode(200)
                .body("status", equalTo("UP"));
    }

    @Test
    @DisplayName("Query Side /actuator/health returns UP")
    void querySideIsHealthy() {
        given()
                .baseUri(E2EConfig.QUERY_SIDE_URL)
        .when()
                .get("/actuator/health")
        .then()
                .statusCode(200)
                .body("status", equalTo("UP"));
    }
}
