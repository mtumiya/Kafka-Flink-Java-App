# HL7 Lab Message Processing Pipeline

A Flink-based application for processing HL7 laboratory messages through Kafka, transforming them to JSON, and storing them in PostgreSQL.

## Overview

This application processes three types of HL7 messages:
1. Lab Orders (OML^O21)
2. Lab Order Acknowledgments (ACK)
3. Lab Results (ORU^R01)

## Prerequisites

- Java 17
- Apache Maven 3.8+
- Docker and Docker Compose
- PostgreSQL 14
- Apache Kafka 7.5.1

## Project Structure

```
flink-project/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── zm/gov/moh/hie/pipeline/disa/
│   │   │       ├── LabMessageProcessingJob.java
│   │   │       ├── PropertiesConfig.java
│   │   │       └── ... (other Java files)
│   │   └── resources/
│   │       └── application.properties
├── docker-compose.yml
└── pom.xml
```

## Configuration

All configuration is managed through application.properties:

```properties
# Kafka Configuration
kafka.bootstrap.servers=localhost:9092
kafka.consumer.group=flink-lab-message-consumer

# Database Configuration
spring.datasource.url=jdbc:postgresql://localhost:9876/hie_manager
spring.datasource.username=postgres
spring.datasource.password=postgres
```

## Building and Running

1. **Build the Application**
```bash
mvn clean package
```

2. **Start Infrastructure**
```bash
docker-compose up -d
```

3. **Create Kafka Topics**
```bash
java -cp target/flink-kafka-stream-1.0-SNAPSHOT.jar zm.gov.moh.hie.pipeline.disa.KafkaTopicSetup
```

4. **Submit Flink Job**
```bash
docker-compose exec jobmanager ./bin/flink run \
  -c zm.gov.moh.hie.pipeline.disa.LabMessageProcessingJob \
  /opt/flink/usrlib/flink-kafka-stream-1.0-SNAPSHOT.jar
```

## Testing

The project includes test producers for simulating HL7 messages:

1. **Test Lab Orders**
```bash
java -cp target/flink-kafka-stream-1.0-SNAPSHOT.jar zm.gov.moh.hie.pipeline.disa.TestHL7Producer
```

2. **Test Acknowledgments**
```bash
java -cp target/flink-kafka-stream-1.0-SNAPSHOT.jar zm.gov.moh.hie.pipeline.disa.TestHL7AckProducer
```

3. **Test Lab Results**
```bash
java -cp target/flink-kafka-stream-1.0-SNAPSHOT.jar zm.gov.moh.hie.pipeline.disa.TestHL7ResultProducer
```

## Monitoring

- **Flink Dashboard**: http://localhost:8081
- **Kafka UI**: http://localhost:8080
- **PostgreSQL**: localhost:9876

## Troubleshooting

### Common Issues

1. **Kafka Connection Issues**
```bash
# Check Kafka accessibility
nc -zv localhost:9092

# View Kafka logs
docker-compose logs kafka
```

2. **Database Connection Issues**
```bash
# Check PostgreSQL connection
psql -h localhost -p 9876 -U postgres -d hie_manager

# View PostgreSQL logs
docker-compose logs postgres
```

3. **Application Issues**
```bash
# Check Flink job logs
docker-compose logs jobmanager
docker-compose logs taskmanager
```

### Environment Variables

You can override configurations using environment variables:
```bash
# Example: Override Kafka bootstrap servers
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Example: Override database credentials
export SPRING_DATASOURCE_USERNAME=myuser
export SPRING_DATASOURCE_PASSWORD=mypassword
```

## Clean Up

```bash
# Stop all containers
docker-compose down

# Remove volumes (caution: this deletes all data)
docker-compose down -v
```

## Development

### Adding New Features

1. Create feature branch from main
2. Make changes and test thoroughly
3. Update documentation if needed
4. Submit pull request

### Running Tests

```bash
# Run all tests
mvn test

# Run specific test
mvn test -Dtest=TestHL7Producer
```

## API Documentation

### Kafka Topics

- **lab-orders**: Receives HL7 OML^O21 messages
- **lab-orders-ack**: Receives HL7 ACK messages
- **lab-results**: Receives HL7 ORU^R01 messages
- **lab-orders-json**: JSON transformed lab orders
- **lab-orders-ack-json**: JSON transformed acknowledgments
- **lab-results-json**: JSON transformed lab results

### Database Schema

The application uses the following PostgreSQL tables:
- `crt.lab_orders`: Stores lab orders
- `crt.lab_orders_ack`: Stores order acknowledgments
- `crt.lab_results`: Stores lab results

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes
4. Submit pull request

## License

[Your License Information]

## Contact

[Your Contact Information]