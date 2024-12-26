package ru.fbtw.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import ru.fbtw.comparator.DecComparator;
import ru.fbtw.dto.CategoryData;
import ru.fbtw.mapper.SalesSortingMapper;
import ru.fbtw.reducer.ResultReducer;

public class DataSortJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String inputDir = args[0];
        String outputDir = args[1];
        int reducerCount = 1;

        Configuration configuration = getConf();

        Job job = Job.getInstance(configuration, "sort");
        job.setJarByClass(getClass());
        job.setNumReduceTasks(reducerCount);

        job.setMapperClass(SalesSortingMapper.class);
        job.setReducerClass(ResultReducer.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(CategoryData.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(DecComparator.class);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
}