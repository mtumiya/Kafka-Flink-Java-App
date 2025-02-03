package zm.gov.moh.hie.pipeline.disa;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public abstract class BasePostgresSink<T> extends RichSinkFunction<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(BasePostgresSink.class);
    private static final int MAX_RETRIES = 5;
    private static final long RETRY_DELAY_MS = 5000; // 5 seconds delay between retries

    private final String url;
    private final String username;
    private final String password;
    private transient Connection connection;
    private transient PreparedStatement preparedStatement;

    protected BasePostgresSink(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        establishConnection();
    }

    private void establishConnection() throws Exception {
        int attempts = 0;
        Exception lastException = null;

        while (attempts < MAX_RETRIES && connection == null) {
            try {
                attempts++;
                LOG.info("Attempt {} to establish database connection to: {}", attempts, url);

                Class.forName("org.postgresql.Driver");
                connection = DriverManager.getConnection(url, username, password);

                // Set the schema
                try (PreparedStatement stmt = connection.prepareStatement("SET search_path TO crt")) {
                    LOG.info("Setting search_path to 'crt'");
                    stmt.execute();
                }

                // Verify connection
                try (PreparedStatement stmt = connection.prepareStatement("SELECT current_schema(), current_database()")) {
                    var rs = stmt.executeQuery();
                    if (rs.next()) {
                        LOG.info("Successfully connected to database: {}, current schema: {}",
                                rs.getString(2), rs.getString(1));
                    }
                }

                // Prepare the statement
                if (preparedStatement == null) {
                    LOG.info("Preparing SQL statement: {}", getInsertSql());
                    preparedStatement = connection.prepareStatement(getInsertSql());
                }

                return; // Connection successful, exit the loop

            } catch (Exception e) {
                lastException = e;
                LOG.warn("Attempt {} failed to connect to database: {}", attempts, e.getMessage());

                if (attempts < MAX_RETRIES) {
                    LOG.info("Waiting {} ms before next retry...", RETRY_DELAY_MS);
                    Thread.sleep(RETRY_DELAY_MS);
                }
            }
        }

        if (connection == null) {
            LOG.error("Failed to establish database connection after {} attempts", MAX_RETRIES);
            throw new RuntimeException("Could not establish database connection after " + MAX_RETRIES + " attempts", lastException);
        }
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        boolean success = false;
        Exception lastException = null;
        int attempts = 0;

        while (!success && attempts < MAX_RETRIES) {
            try {
                attempts++;
                if (connection == null || connection.isClosed()) {
                    LOG.info("Reestablishing database connection");
                    establishConnection();
                }

                setStatementParameters(preparedStatement, value);
                preparedStatement.executeUpdate();
                success = true;

            } catch (SQLException e) {
                lastException = e;
                LOG.warn("Attempt {} failed to execute SQL: {}", attempts, e.getMessage());

                if (attempts < MAX_RETRIES) {
                    LOG.info("Waiting {} ms before next retry...", RETRY_DELAY_MS);
                    Thread.sleep(RETRY_DELAY_MS);

                    // Close and clear the old connection/statement
                    if (preparedStatement != null) {
                        try { preparedStatement.close(); } catch (Exception ex) { }
                        preparedStatement = null;
                    }
                    if (connection != null) {
                        try { connection.close(); } catch (Exception ex) { }
                        connection = null;
                    }
                }
            }
        }

        if (!success) {
            LOG.error("Failed to execute SQL after {} attempts", MAX_RETRIES);
            throw new RuntimeException("Failed to execute SQL after " + MAX_RETRIES + " attempts", lastException);
        }
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    protected abstract String getInsertSql();
    protected abstract void setStatementParameters(PreparedStatement ps, T value) throws SQLException;

    // Utility methods for common JDBC operations
    protected static void setTimestamp(PreparedStatement ps, int index, LocalDateTime dateTime) throws SQLException {
        if (dateTime != null) {
            ps.setTimestamp(index, Timestamp.valueOf(dateTime));
        } else {
            ps.setNull(index, java.sql.Types.TIMESTAMP);
        }
    }

    protected static void setString(PreparedStatement ps, int index, String value) throws SQLException {
        if (value != null) {
            ps.setString(index, value);
        } else {
            ps.setNull(index, java.sql.Types.VARCHAR);
        }
    }
}