package zm.gov.moh.hie.pipeline.disa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesConfig {
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesConfig.class);
    private static final Properties properties = new Properties();
    private static final Map<String, String> cachedProperties = new HashMap<>();
    private static final String DEFAULT_PROPERTIES = "application.properties";
    private static final String PROD_PROPERTIES = "application-prod.properties";

    static {
        loadProperties();
    }

    private static void loadProperties() {
        try {
            // Load default properties first
            loadPropertiesFile(DEFAULT_PROPERTIES);

            // Check if production profile is active
            String activeProfile = System.getenv("SPRING_PROFILES_ACTIVE");
            if ("prod".equalsIgnoreCase(activeProfile)) {
                LOG.info("Loading production properties");
                loadPropertiesFile(PROD_PROPERTIES);
            }

            // Override with environment variables
            loadEnvironmentOverrides();

            // Process property placeholders
            resolvePlaceholders();

            LOG.info("Properties loaded successfully. Active profile: {}",
                    activeProfile != null ? activeProfile : "default");
        } catch (Exception e) {
            LOG.error("Failed to load properties", e);
            throw new RuntimeException("Failed to load properties", e);
        }
    }

    private static void loadPropertiesFile(String filename) throws IOException {
        try (InputStream input = PropertiesConfig.class.getClassLoader().getResourceAsStream(filename)) {
            if (input == null) {
                LOG.warn("Unable to find {}", filename);
                return;
            }
            Properties fileProperties = new Properties();
            fileProperties.load(input);
            // Merge properties, with new properties taking precedence
            properties.putAll(fileProperties);
            LOG.debug("Loaded properties from {}", filename);
        }
    }

    private static void loadEnvironmentOverrides() {
        System.getenv().forEach((key, value) -> {
            String normalizedKey = key.toLowerCase().replace('_', '.');
            if (properties.containsKey(normalizedKey)) {
                properties.setProperty(normalizedKey, value);
                LOG.debug("Overrode property {} with environment variable {}", normalizedKey, key);
            }
        });
    }

    private static void resolvePlaceholders() {
        Properties resolvedProperties = new Properties();
        properties.forEach((key, value) -> {
            String resolvedValue = resolvePlaceholder(value.toString(), 0);
            resolvedProperties.setProperty(key.toString(), resolvedValue);
        });
        properties.putAll(resolvedProperties);
    }

    private static String resolvePlaceholder(String value, int depth) {
        if (depth > 10) {
            LOG.warn("Max placeholder resolution depth reached for value: {}", value);
            return value;
        }

        if (!value.contains("${")) {
            return value;
        }

        String result = value;
        int startIndex;
        while ((startIndex = result.indexOf("${")) != -1) {
            int endIndex = result.indexOf("}", startIndex);
            if (endIndex == -1) {
                break;
            }

            String placeholder = result.substring(startIndex + 2, endIndex);
            String[] parts = placeholder.split(":", 2);
            String propertyKey = parts[0];
            String defaultValue = parts.length > 1 ? parts[1] : "";

            String propertyValue = properties.getProperty(propertyKey, defaultValue);
            if (propertyValue != null) {
                // Recursively resolve any nested placeholders
                propertyValue = resolvePlaceholder(propertyValue, depth + 1);
                result = result.substring(0, startIndex) +
                        propertyValue +
                        result.substring(endIndex + 1);
            } else {
                LOG.warn("No value found for placeholder: {}", propertyKey);
                break;
            }
        }
        return result;
    }

    public static String getProperty(String key) {
        return cachedProperties.computeIfAbsent(key, k -> properties.getProperty(k));
    }

    public static String getProperty(String key, String defaultValue) {
        String value = getProperty(key);
        return value != null ? value : defaultValue;
    }

    public static int getIntProperty(String key, int defaultValue) {
        String value = getProperty(key);
        try {
            return value != null ? Integer.parseInt(value) : defaultValue;
        } catch (NumberFormatException e) {
            LOG.warn("Invalid integer value for key {}: {}", key, value);
            return defaultValue;
        }
    }

    public static boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = getProperty(key);
        return value != null ? Boolean.parseBoolean(value) : defaultValue;
    }

    public static Properties getKafkaProperties() {
        Properties kafkaProps = new Properties();
        properties.stringPropertyNames().stream()
                .filter(key -> key.startsWith("kafka."))
                .forEach(key -> {
                    String kafkaKey = key.substring("kafka.".length());
                    String value = properties.getProperty(key);
                    if (value != null && !value.trim().isEmpty()) {
                        kafkaProps.setProperty(kafkaKey, value);
                    }
                });
        return kafkaProps;
    }

    public static Properties getKafkaSecurityProperties() {
        Properties securityProps = new Properties();
        properties.stringPropertyNames().stream()
                .filter(key -> key.startsWith("kafka.") &&
                        (key.contains(".security.") ||
                                key.contains(".sasl.") ||
                                key.contains(".ssl.")))
                .forEach(key -> {
                    String kafkaKey = key.substring("kafka.".length());
                    String value = properties.getProperty(key);
                    if (value != null && !value.trim().isEmpty()) {
                        securityProps.setProperty(kafkaKey, value);
                    }
                });
        return securityProps;
    }

    public static void clearCache() {
        cachedProperties.clear();
    }
}