# Use a lightweight base image with Java installed
FROM eclipse-temurin:21-jre-jammy

# Update package lists and install netcat
RUN apt-get update && apt-get install -y netcat-traditional && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the wait-for-db.sh script and make it executable
COPY wait-for-db.sh wait-for-db.sh
RUN chmod +x wait-for-db.sh

# Copy the executable JAR file from the target directory into the container
# The `kafka-consumer-demo-1.0.0-SNAPSHOT.jar` is the name of the JAR produced by Maven
COPY target/kafka-consumer-demo-1.0.0-SNAPSHOT.jar app.jar

# Expose the port the Spring Boot application runs on (default is 8080)
EXPOSE 8080

# Use the wait-for-db.sh script as the entrypoint
# It will wait for the database to be ready and then run the Spring Boot app
ENTRYPOINT ["./wait-for-db.sh", "oracle", "java", "-jar", "app.jar"]
