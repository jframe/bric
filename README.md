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

- `help` - Display available commands
- `version` - Display version information
- `echo <message>` - Echo back the message
- `status` - Display REPL status
- `exit` or `quit` - Exit the REPL

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
- **Log4j2** - Logging framework
- **JUnit 5** - Testing framework

## Development

### Running Tests

```bash
./gradlew test
```

### Cleaning Build Artifacts

```bash
./gradlew clean
```
