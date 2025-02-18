# HL7 Lab Message Processing Pipeline

This application is a Flink-based processing pipeline for handling HL7 laboratory messages. It processes three types of messages:
- Lab Orders (OML^O21)
- Lab Order Acknowledgments (ACK)
- Lab Results (ORU^R01)

## Overview

The pipeline reads HL7 messages from Kafka topics, processes them, and:
1. Converts them to JSON format
2. Stores them in PostgreSQL
3. Forwards the JSON messages to output Kafka topics


### Project Structure
```
project-root/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── zm/gov/moh/hie/pipeline/disa/
│   │   │       ├── parsers/           # HL7 message parsers
│   │   │       ├── models/            # Data models
│   │   │       ├── jobs/              # Flink job definitions
│   │   │       └── config/            # Configuration classes
│   │   └── resources/
│   │       ├── application.properties      # Default config
│   │       └── application-prod.properties # Production config
└── pom.xml
```

### Architecture
```
[Input Kafka Topics] -> [Flink Processing Jobs] -> [PostgreSQL + Output Kafka Topics]

Processing Flow:
HL7 Message -> Parse -> Transform -> Store & Forward
```

## Setup

### Prerequisites
- Java 17
- Apache Maven
- PostgreSQL
- Apache Kafka
- Apache Flink

### Database Setup
Create the necessary tables in PostgreSQL:

```sql
-- Lab Orders Table
CREATE TABLE lab_orders (
    lab_order_id VARCHAR(255) PRIMARY KEY,
    sending_facility_id VARCHAR(255),
    receiving_lab_code VARCHAR(255),
    sending_date_time TIMESTAMP,
    lab_tests_ordered JSONB,
    message_id VARCHAR(255)
);

-- Lab Order Acknowledgments Table
CREATE TABLE lab_orders_ack (
    ack_message_id VARCHAR(255) PRIMARY KEY,
    sending_facility_id VARCHAR(255),
    targeted_disa_code VARCHAR(255),
    sending_date_time TIMESTAMP,
    message_id VARCHAR(255),
    acknowledgment_status VARCHAR(50),
    acknowledgment_description TEXT
);

-- Lab Results Table
CREATE TABLE lab_results (
    lab_order_id VARCHAR(255) PRIMARY KEY,
    sending_facility_id VARCHAR(255),
    receiving_lab_code VARCHAR(255),
    sending_date_time TIMESTAMP,
    lab_test_results JSONB,
    message_id VARCHAR(255),
    patient_nupn VARCHAR(255)
);
```
#### Kafka Topics
```properties
kafka.topics.lab-orders=lab-orders
kafka.topics.lab-orders-ack=lab-orders-ack
kafka.topics.lab-results=lab-results
kafka.topics.lab-orders-json=lab-orders-json-extract
kafka.topics.lab-orders-ack-json=lab-orders-ack-json-extract
kafka.topics.lab-results-json=lab-results-json-extract
```

### Configuration

#### Default Mode
1. Update `application.properties`:
```properties
spring.datasource.url=jdbc:postgresql://host.docker.internal:9876/hie_manager
spring.datasource.username=postgres
spring.datasource.password=postgres
spring.datasource.driver-class-name=org.postgresql.Driver
```

#### Production Mode
1. Update `application-prod.properties`:
```properties
kafka.bootstrap.servers=154.120.216.119:9093,154.120.216.120:9093
kafka.security.protocol=SASL_PLAINTEXT
kafka.sasl.mechanism=SCRAM-SHA-256
kafka.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="075F80FED7C6";
```


2. Set production profile:
```powershell
$env:SPRING_PROFILES_ACTIVE="prod"
```
   Check Active Profile: 
```powershell
echo $env:SPRING_PROFILES_ACTIVE
```
Revert to Default Profile
```powershell
Remove-Item Env:SPRING_PROFILES_ACTIVE
```

### Building the Application
```bash
mvn clean package
```

This creates three JAR files:
- `target/flink-kafka-stream-1.0-SNAPSHOT-lab-orders.jar`
- `target/flink-kafka-stream-1.0-SNAPSHOT-lab-orders-ack.jar`
- `target/flink-kafka-stream-1.0-SNAPSHOT-lab-results.jar`

### Running the Applications

Run each component in a separate terminal:

1. Lab Orders Processing:
```bash
java -jar target/flink-kafka-stream-1.0-SNAPSHOT-lab-orders.jar
```

2. Lab Order Acknowledgments:
```bash
java -jar target/flink-kafka-stream-1.0-SNAPSHOT-lab-orders-ack.jar
```

3. Lab Results:
```bash
java -jar target/flink-kafka-stream-1.0-SNAPSHOT-lab-results.jar
```
Remember to set the profile before running:
```bash
# For production
$env:SPRING_PROFILES_ACTIVE="prod"

# For default
Remove-Item Env:SPRING_PROFILES_ACTIVE
```
## Message Flow Examples

### Lab Order Processing
```
// Input HL7 Message
MSH|^~\&|CarePro|Facility^ID|DISA*LAB|ZCR|20250127115737||OML^O21^OML_O21|MSG0001|P^T|2.5
PID|1||NUPN123^^^MR||TEST^PATIENT||19601101|M
ORC|NW|ORDER123|||||||20250127155100
OBR|1|ORDER123||47245-6^Test Name^LOINC|Regular|20250127115737
```
```
// Output JSON
{
    "lab_order_id": "ORDER123",
    "sending_facility_id": "Facility",
    "receiving_lab_code": "ZCR",
    "message_id": "MSG0001",
    "lab_tests_ordered": [{
        "code": "47245-6",
        "name": "Test Name",
        "type": "Regular"
    }]
}
```

