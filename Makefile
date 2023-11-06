compile:
	mvn clean install -DskipTests=true

run_program: compile
	java -jar target/kafka-services.Consumer-jar-with-dependencies.jar

clean:
	mvn clean