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
            // Log the classpath for debugging
            LOG.info("Classpath: {}", System.getProperty("java.class.path"));

            // Load default properties first
            LOG.info("Attempting to load default properties from: {}", DEFAULT_PROPERTIES);
            loadPropertiesFile(DEFAULT_PROPERTIES);

            // Check if production profile is active
            String activeProfile = System.getenv("SPRING_PROFILES_ACTIVE");
            LOG.info("Active Profile: {}", activeProfile);

            if ("prod".equalsIgnoreCase(activeProfile)) {
                LOG.info("Loading production properties from: {}", PROD_PROPERTIES);
                loadPropertiesFile(PROD_PROPERTIES);

                // Log production-specific properties
                LOG.info("Production Kafka Bootstrap Servers: {}",
                        properties.getProperty("kafka.bootstrap.servers"));
                LOG.info("Production Security Protocol: {}",
                        properties.getProperty("kafka.security.protocol"));
                LOG.info("Production SASL Mechanism: {}",
                        properties.getProperty("kafka.sasl.mechanism"));
            }

            // Override with environment variables
            loadEnvironmentOverrides();

            // Process property placeholders
            resolvePlaceholders();

            // Log all loaded properties (excluding sensitive info)
            LOG.info("Loaded Properties:");
            properties.stringPropertyNames().stream()
                    .filter(key -> !key.contains("password") && !key.contains("secret"))
                    .forEach(key -> LOG.info("{}={}", key, properties.getProperty(key)));

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
                LOG.error("Unable to find {} in classpath", filename);
                throw new IOException("Property file '" + filename + "' not found in classpath");
            }

            Properties fileProperties = new Properties();
            fileProperties.load(input);

            // Log the number of properties loaded from this file
            LOG.info("Loaded {} properties from {}", fileProperties.size(), filename);

            // Merge properties, with new properties taking precedence
            properties.putAll(fileProperties);
        }
    }

    private static void loadEnvironmentOverrides() {
        LOG.info("Loading environment variable overrides");
        System.getenv().forEach((key, value) -> {
            String normalizedKey = key.toLowerCase().replace('_', '.');
            if (properties.containsKey(normalizedKey)) {
                properties.setProperty(normalizedKey, value);
                LOG.debug("Overrode property {} with environment variable {}", normalizedKey, key);
            }
        });
    }

    private static void resolvePlaceholders() {
        LOG.info("Resolving property placeholders");
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
        String value = cachedProperties.computeIfAbsent(key, k -> properties.getProperty(k));
        if (value == null) {
            LOG.warn("Property not found: {}", key);
        } else {
            // Log non-sensitive property values
            if (!key.contains("password") && !key.contains("secret")) {
                LOG.info("Retrieved property: {}={}", key, value);
            } else {
                LOG.info("Retrieved sensitive property: {}=*****", key);
            }
        }
        return value;
    }

    public static String getProperty(String key, String defaultValue) {
        String value = getProperty(key);
        return value != null ? value : defaultValue;
    }

    public static Properties getKafkaProperties() {
        Properties kafkaProps = new Properties();
        LOG.info("Collecting Kafka properties...");

        properties.stringPropertyNames().stream()
                .filter(key -> key.startsWith("kafka."))
                .forEach(key -> {
                    String kafkaKey = key.substring("kafka.".length());
                    String value = properties.getProperty(key);
                    if (value != null && !value.trim().isEmpty()) {
                        kafkaProps.setProperty(kafkaKey, value);
                        if (!key.contains("password") && !key.contains("secret")) {
                            LOG.debug("Adding Kafka property: {}={}", kafkaKey, value);
                        } else {
                            LOG.debug("Adding sensitive Kafka property: {}=*****", kafkaKey);
                        }
                    }
                });

        return kafkaProps;
    }

    public static Properties getKafkaSecurityProperties() {
        Properties securityProps = new Properties();
        LOG.info("Collecting Kafka security properties...");

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
                        if (!key.contains("password") && !key.contains("secret")) {
                            LOG.info("Adding security property: {}={}", kafkaKey, value);
                        } else {
                            LOG.info("Adding security property: {}=*****", kafkaKey);
                        }
                    }
                });

        return securityProps;
    }

    public static void clearCache() {
        cachedProperties.clear();
        LOG.info("Property cache cleared");
    }
}