package it.unipi.hadoop;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        long startTime = System.currentTimeMillis();
        kMeans.main(args);
        long endTime = System.currentTimeMillis();
        System.out.printf("\nExecution time: %d ms\n", (endTime - startTime));
    }
}
