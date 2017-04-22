hadoop com.sun.tools.javac.Main newKMeans.java
jar cf output.jar newKMeans*.class
hadoop fs -rm -r output
hadoop jar output.jar newKMeans hd-iris.txt output 3 4 0.01
