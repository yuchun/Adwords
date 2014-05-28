all:
	javac -Xlint:deprecation -cp ojdbc5.jar:. adwords.java

run:
	java -cp ojdbc5.jar:. adwords

clean:
	rm *.class
