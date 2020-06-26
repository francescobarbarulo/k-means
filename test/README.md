# _k_-means testing

1. Build the package with maven

```bash
mvn clean package
```

2. Prepare the files for the Hadoop or Spark execution

3. Place the program files, the Test jar and the inputData folder on the same directory

4. Run the jar file **with dependencies** passing "hadoop" or "spark" as argument:

```bash
java -classpath Test-1.0-SNAPSHOT-with_dependencies.jar it.unipi.kmeans.test.Test hadoop/spark
```

