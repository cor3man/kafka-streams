FROM openjdk:17-jdk-slim
WORKDIR /app
COPY target/kafka-streams-0.0.1-SNAPSHOT.jar kafka-streams.jar
EXPOSE 8081
CMD ["java", "-jar", "kafka-streams.jar"]