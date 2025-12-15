# Bric

A simple REPL (Read-Eval-Print Loop) command-line interface for Besu.

## Requirements

- Java 17 or higher
- Gradle 7.x or higher (or use the Gradle wrapper)

## Building the Project

```bash
# Build the project
./gradlew build

# Create a fat JAR with all dependencies
./gradlew fatJar

# Check for dependency updates
./gradlew dependencyUpdates
```

## Running the REPL

### Using Gradle

```bash
./gradlew run
```

### Using the JAR

```bash
java -jar build/libs/bric-1.0.0-SNAPSHOT-all.jar
```

### With verbose mode

```bash
./gradlew run --args="--verbose"
# or
java -jar build/libs/bric-1.0.0-SNAPSHOT-all.jar --verbose
```

## Available Commands

Once the REPL is running, you can use the following commands:

### General Commands
- `help` - Display available commands
- `version` - Display version information
- `status` - Display REPL status
- `exit` or `quit` - Exit the REPL

### Database Commands
- `db-open <path>` - Open a Besu database in read-only mode
- `db-close` - Close the currently open database
- `db-info` - Display database statistics and column family information

## Project Structure

```
bric/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── net/consensys/bric/
│   │   │       ├── BricApplication.java
│   │   │       └── BricCommandProcessor.java
│   │   └── resources/
│   │       └── log4j2.xml
│   └── test/
│       └── java/
│           └── net/consensys/bric/
│               └── BricCommandProcessorTest.java
├── build.gradle
├── settings.gradle
└── README.md
```

## Dependencies

- **JLine 3** - Enhanced terminal and console functionality
- **Picocli** - Command-line parsing
- **RocksDB** - Database access for Besu databases
- **Besu Datatypes** - Core Besu data structures
- **Tuweni** - Bytes operations and RLP encoding/decoding
- **Log4j2** - Logging framework
- **JUnit 5** - Testing framework

### Dependency Management

Dependencies are managed using:
- **Spring Dependency Management Plugin** - For centralized version management
- **Ben Manes Versions Plugin** - For checking dependency updates

Versions are defined in `gradle/versions.gradle`. To check for outdated dependencies, run:

```bash
./gradlew dependencyUpdates
```

## Development

### Running Tests

```bash
./gradlew test
```

### Cleaning Build Artifacts

```bash
./gradlew clean
```
