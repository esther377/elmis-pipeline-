# Elmis Pipeline
## Overview
Elmis pipeline is a data streaming application that utilizes Apache Flink and Kafka to streamline a smooth ETL process.

## Project Structure

```
zm.gov.moh.hie.elmis/
├── src/main/java
│   ├── BusinessLogic  # Implements extended operations for other main methods
│   ├── zm.gov.moh.hie.elmis # Implements main methods for each job
│   ├── Configuration  # Handles essential configuration e.g. database, server, etc.
│   ├── HelperClass   # Handles the implementation of DTOs and other reusable methods.
├── pom.xml   # Initiates the project dependencies.
└── README.md  # Sample project documentation.
```

## Technologies Used
- **Apache Flink**: For real-time stream processing
- **Apache Kafka**: As a message broker
- **PostgreSQL**: As the target database
- **Jackson (FasterXML)**: For JSON parsing
- **Maven**: For dependency management and project build

## Setup Instructions

### Prerequisites
Ensure you have the following installed:
- Java 11 or higher
- Maven 3.6+
- PostgreSQL database
- Kafka cluster with configured topics and consumer group IDs.

## Configuration

### Kafka Connection Settings (in `Configuration/StreamingConfiguration`)
Modify the following properties in `getKafkaProperties()` to match your Kafka setup:
```java
props.setProperty("bootstrap.servers", "");
props.setProperty("group.id", "");
props.setProperty("sasl.jaas.config", "");
```

### Database Connection Settings (in `Configuration/DbConfiguration`)
Modify the JDBC connection settings:
```java
new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
    .withUrl("jdbc:postgresql://localhost:5432/HIE_db")
    .withDriverName("org.postgresql.Driver")
    .withUsername("postgres")
    .withPassword("root")
    .build();
```

## Building and Running

### Build the Project
```sh
mvn clean package
```

### Run the Application
```sh
java -jar target/CarePro-pipelines-1.0-SNAPSHOT.jar
```

## Code Explanation

### `BusinessLogic`
- Implements the operations of the dispensation-Ack process (logic to load data to the new created dispensation table after extraction from dispensations-Ack topic).
- Handles similar operations for prescription acknowledgment and dispensation-new.

### `Configuration/DbConfiguration`
- Defines database setup including database name, username, password, etc.

### `Configuration/StreamingConfiguration`
- Defines Kafka consumer properties and creates a Flink Kafka consumer.

### `Configuration/KafkaProducerService`
- Defines Kafka producer properties and writes data to generated dynamic topics.

### `pom.xml`
- Contains project dependencies such as Flink, Kafka, PostgreSQL JDBC, and Jackson.

## `zm.gov.moh.hie.elmis`
- Defines the main entry-point methods for each job. The implemented methods include dispensation, prescription, dispensation acknowledgment, prescription acknowledgment, etc.

## Database Schema
Ensure your PostgreSQL database has the following tables. Below are some of the database schemas utilized in the business logic of the implemented processes:

```sql
CREATE TABLE prescription_new (
    timestamp TIMESTAMP,
    sending_application VARCHAR(255),
    receiving_application VARCHAR(255),
    message_id VARCHAR(255),
    hmis_code VARCHAR(255),
    regimen_code VARCHAR(255),
    regimen_duration INT,
    prescriptions_count INT,
    prescription_uuid UUID
);

CREATE TABLE public.dispensation_ack (
  id integer NOT NULL DEFAULT nextval('dispensation_ack_id_seq'::regclass),
  "timestamp" timestamp with time zone NOT NULL,
  sending_application character varying(255) NOT NULL,
  receiving_application character varying(255) NOT NULL,
  message_id character varying(255) NOT NULL,
  acknowledgement_code character varying(255),
  referenced_message_id character varying(255),
  CONSTRAINT dispensation_ack_pkey PRIMARY KEY (id),
  CONSTRAINT dispensation_ack_message_id_key UNIQUE (message_id)
);

CREATE TABLE public.dispensation_new (
  id integer NOT NULL DEFAULT nextval('dispensation_new_id_seq'::regclass),
  "timestamp" timestamp with time zone NOT NULL,
  sending_application character varying(255) NOT NULL,
  receiving_application character varying(255) NOT NULL,
  message_id character varying(255) NOT NULL,
  hmis_code character varying(50),
  regimen_code character varying(50),
  regimen_duration integer,
  dispensation_count integer,
  prescription_uuid uuid NOT NULL,
  CONSTRAINT dispensation_new_pkey PRIMARY KEY (id),
  CONSTRAINT dispensation_new_message_id_key UNIQUE (message_id)
);

CREATE TABLE public.prescription_ack (
  id integer NOT NULL DEFAULT nextval('prescription_ack_id_seq'::regclass),
  "timestamp" timestamp with time zone NOT NULL,
  sending_application character varying(255) NOT NULL,
  receiving_application character varying(255) NOT NULL,
  message_id character varying(255) NOT NULL,
  acknowledgement_code character varying(255),
  referenced_message_id character varying(255),
  CONSTRAINT prescription_ack_pkey PRIMARY KEY (id),
  CONSTRAINT prescription_ack_message_id_key UNIQUE (message_id)
);
```

## Utils
- Implements static DTOs which are inherited in some business logic.

## Troubleshooting

### Common Issues
- **Kafka Connection Issues**: Ensure Kafka brokers and authentication settings are correct.
- **Database Connection Errors**: Verify that PostgreSQL is running and credentials are valid.
- **Dependency Conflicts**: Run `mvn dependency:tree` to check for conflicting dependencies.

## Future Enhancements
- Add monitoring and logging mechanisms.
- Implement error handling and retry mechanisms for failed Kafka messages.
- Optimize Flink job performance for better efficiency.

