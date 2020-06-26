# _k_-means hadoop implementation

1. Build the package with maven

```bash
mvn clean package
```

2. Set the parameters in the configuration file `config.ini`

3. Copy the dataset to the Hadoop Distributed File System (HDFS) in the directory specified in the configuration file

4. Run the jar file **with dependencies** by:

```bash
hadoop jar target/kMeans-1.0-SNAPSHOT-with_dependencies.jar it.unipi.hadoop.kMeans
```

