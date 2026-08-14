# Build stage
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:25-jre-jammy

# netcat sert au script d'attente de la base ; curl au HEALTHCHECK.
RUN apt-get update \
    && apt-get install -y --no-install-recommends netcat-traditional curl \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the wait-for-db.sh script and make it executable
COPY wait-for-db.sh wait-for-db.sh
RUN chmod +x wait-for-db.sh

# Copy the executable JAR file from the build stage.
# Le motif évite de répéter la version du pom, qui devrait sinon être
# mise à jour ici à chaque montée de version.
COPY --from=build /app/target/kafka-consumer-demo-*.jar app.jar

# L'application n'a besoin d'aucun privilège : elle tourne sous un compte
# dédié sans shell, propriétaire des seuls fichiers dont elle a besoin.
# logs/ reçoit l'appender fichier de logback, trace/ la persistance fichier
# optionnelle : tous deux sont relatifs au répertoire de travail.
RUN groupadd --system --gid 1001 app \
    && useradd --system --uid 1001 --gid app --no-create-home --shell /usr/sbin/nologin app \
    && mkdir -p /app/logs /app/trace \
    && chown -R app:app /app
USER app

# Expose the port the Spring Boot application runs on (default is 8080)
EXPOSE 8080

# Actuator est exposé sur le chemin par défaut ; /admin est le context-path
# de Spring Boot Admin, pas celui d'Actuator.
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
    CMD curl --fail --silent http://localhost:8080/actuator/health || exit 1

# Use the wait-for-db.sh script as the entrypoint
ENTRYPOINT ["./wait-for-db.sh", "oracle", "java", "-jar", "app.jar"]
