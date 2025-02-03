package zm.gov.moh.hie.pipeline.disa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesConfig {
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesConfig.class);
    private static final Properties properties = new Properties();
    private static final String PROPERTIES_FILE = "application.properties";

    static {
        loadProperties();
    }

    private static void loadProperties() {
        try (InputStream input = PropertiesConfig.class.getClassLoader()
                .getResourceAsStream(PROPERTIES_FILE)) {
            if (input == null) {
                throw new IOException("Unable to find " + PROPERTIES_FILE);
            }
            properties.load(input);
            LOG.info("Properties loaded successfully");

            // Override with environment variables if they exist
            loadEnvironmentOverrides();
        } catch (Exception e) {
            LOG.error("Failed to load properties", e);
            throw new RuntimeException("Failed to load properties", e);
        }
    }

    private static void loadEnvironmentOverrides() {
        properties.stringPropertyNames().forEach(prop -> {
            String envVar = prop.toUpperCase().replace('.', '_');
            String envValue = System.getenv(envVar);
            if (envValue != null) {
                properties.setProperty(prop, envValue);
                LOG.debug("Overrode property {} with environment variable {}", prop, envVar);
            }
        });
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
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
                    kafkaProps.setProperty(kafkaKey, properties.getProperty(key));
                });
        return kafkaProps;
    }
}