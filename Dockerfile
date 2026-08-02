# Build stage
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /app
# Dependencies resolve in their own layer, keyed on pom.xml alone, so editing sources does not
# re-download the whole dependency tree on every build.
COPY pom.xml .
RUN mvn -B --no-transfer-progress dependency:go-offline
COPY src ./src
RUN mvn -B --no-transfer-progress clean package -DskipTests

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
# Wildcard rather than a pinned version: the hardcoded 1.0.1 filename broke the image on every
# version bump. Only the repackaged jar matches; the Spring Boot original keeps a .jar.original name.
COPY --from=build /app/target/*.jar app.jar

# Expose the port the Spring Boot application runs on (default is 8080)
EXPOSE 8080

# Use the wait-for-db.sh script as the entrypoint
ENTRYPOINT ["./wait-for-db.sh", "oracle", "java", "-jar", "app.jar"]
