package ru.fbtw.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import ru.fbtw.dto.SalesData;
import ru.fbtw.mapper.SalesMapper;
import ru.fbtw.reducer.SalesReducer;


public class CategoryInfoJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String inputDir = args[0];
        String outputDir = args[1];
        int reducerCount = Integer.parseInt(args[2]);

        Configuration configuration = getConf();

        Job job = Job.getInstance(configuration, "analysis");
        job.setJarByClass(getClass());
        job.setNumReduceTasks(reducerCount);

        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SalesData.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
}