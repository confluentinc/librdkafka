
KAFKA_DIR?=$(HOME)/src/kafka

Murmur2Cli.class: Murmur2Cli.java
	javac -classpath $(KAFKA_DIR)/clients/build/libs/kafka-clients-*.jar $^

clean:
	rm -f *.class
