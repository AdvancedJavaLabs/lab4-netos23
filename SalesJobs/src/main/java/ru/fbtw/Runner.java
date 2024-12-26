package ru.fbtw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import ru.fbtw.job.CategoryInfoJob;

public class Runner {
    public static void main(String[] args) throws Exception {
        String inputDir = args[0];
        String outputDir = args[1];

        Configuration configuration = new Configuration();
        if (args.length > 3) {
            configuration.set("mapreduce.input.fileinputformat.split.maxsize", args[3]);
        }

        long startTime = System.currentTimeMillis();

        int exitCode = ToolRunner.run(
                configuration,
                new CategoryInfoJob(),
                new String[]{inputDir, outputDir, args.length > 2 ? args[2] : ""}
        );

        long endTime = System.currentTimeMillis();
        System.out.printf("Time used: %d", (endTime - startTime));

        System.exit(exitCode);

    }
}