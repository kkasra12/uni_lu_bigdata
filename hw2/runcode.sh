class_name="HadoopWordPairs_p1b4"
javac -classpath $(hadoop classpath) *.java
read -n 1 -s -r -p "Press any key to continue, or press Ctrl+C to exit..."
jar cf hw2.jar *.class
rm *.class
rm -rf output_$class_name
hadoop jar hw2.jar $class_name ~/data/wikipedia/enwiki-articles/AA output_$class_name
head output_$class_name/part-r-00000
