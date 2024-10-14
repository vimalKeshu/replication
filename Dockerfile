FROM amazoncorretto:8-alpine3.19-jdk AS build
ENV JAVA_OPTS="-Xmx512m -XX:+HeapDumpOnOutOfMemoryError -Dfile.encoding=UTF-8"
COPY . /app
WORKDIR /app
RUN chmod +x gradlew && ./gradlew shadowJar

FROM amazoncorretto:8-alpine3.19-jre

RUN mkdir /app
COPY --from=build /app/build/libs/replication-*.jar /app/app.jar
WORKDIR /app
