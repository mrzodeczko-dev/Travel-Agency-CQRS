package com.rzodeczko.e2e;

/**
 * Shared configuration for E2E smoke tests.
 * <p>
 * Override via system properties or environment variables:
 * <pre>
 *   mvn test -Dcommand.side.url=http://myhost:8080
 *   COMMAND_SIDE_URL=http://myhost:8080 mvn test
 * </pre>
 */
public final class E2EConfig {

    public static final String COMMAND_SIDE_URL = resolve("command.side.url", "COMMAND_SIDE_URL", "http://localhost:8080");
    public static final String QUERY_SIDE_URL = resolve("query.side.url", "QUERY_SIDE_URL", "http://localhost:8081");
    public static final int PROPAGATION_TIMEOUT_SECONDS = Integer.parseInt(
            resolve("e2e.propagation.timeout", "E2E_PROPAGATION_TIMEOUT", "120")
    );

    private E2EConfig() {
    }

    private static String resolve(String sysProp, String envVar, String defaultValue) {
        String value = System.getProperty(sysProp);
        if (value != null && !value.isBlank() && !value.equals("null")) {
            return value;
        }
        value = System.getenv(envVar);
        if (value != null && !value.isBlank()) {
            return value;
        }
        return defaultValue;
    }
}
