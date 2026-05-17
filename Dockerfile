# Build stage
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:21-jre-jammy

# Update package lists and install netcat
RUN apt-get update && apt-get install -y netcat-traditional && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the wait-for-db.sh script and make it executable
COPY wait-for-db.sh wait-for-db.sh
RUN chmod +x wait-for-db.sh

# Copy the executable JAR file from the build stage
COPY --from=build /app/target/kafka-consumer-demo-1.0.1.jar app.jar

# Expose the port the Spring Boot application runs on (default is 8080)
EXPOSE 8080

# Use the wait-for-db.sh script as the entrypoint
ENTRYPOINT ["./wait-for-db.sh", "oracle", "java", "-jar", "app.jar"]
