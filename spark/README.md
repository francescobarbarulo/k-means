# _k_-means spark implementation

1. Set the parameters in the configuration file `config.ini`

2. Copy the dataset to the Hadoop Distributed File System (HDFS) in the directory specified in the configuration file

3. Zip the util directory

4. Run the file:

```bash
spark-submit --master yarn --deploy-mode client --py-files util.zip main.py
```

