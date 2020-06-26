# _k_-means testing

1. Build the package with maven

```bash
mvn clean package
```

2. Prepare the files for the Hadoop or Spark execution

3. Prepare an `inputData` folder containing txt files (points), with name format `data_numberOfPoints_numberOfDimensions`. The number and type of points must be coherent with the values embedded inside the `Test.java` file

4. Place the program files, the built jar and the inputData folder in the same directory

5. Run the jar file **with dependencies** passing "hadoop" or "spark" as argument:

```bash
java -classpath Test-1.0-SNAPSHOT-with_dependencies.jar it.unipi.kmeans.test.Test hadoop/spark
```
