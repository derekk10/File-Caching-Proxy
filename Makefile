all: Proxy.class Server.class RMIInterface.class FileInfo.class Fwrapper.class 

%.class: %.java
	javac $<

clean:
	rm -f *.class
