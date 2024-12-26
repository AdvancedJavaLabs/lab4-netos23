package ru.fbtw.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import ru.fbtw.comparator.DecComparator;
import ru.fbtw.dto.CategoryData;
import ru.fbtw.dto.SalesData;
import ru.fbtw.mapper.FilterMapper;
import ru.fbtw.mapper.SalesMapper;
import ru.fbtw.mapper.TextMapper;
import ru.fbtw.mapper.SalesSortingMapper;
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

        Configuration mapConf1 = new Configuration(false);

        ChainMapper.addMapper(
                job,
                FilterMapper.class,
                Text.class,
                Text.class,
                LongWritable.class,
                Text.class,
                mapConf1
        );

        Configuration mapConf2 = new Configuration(false);

        ChainMapper.addMapper(
                job,
                SalesMapper.class,
                LongWritable.class,
                Text.class,
                Text.class,
                SalesData.class,
                mapConf2
        );


        Configuration reduceConf = new Configuration(false);

        ChainReducer.setReducer(
                job,
                SalesReducer.class,
                Text.class,
                SalesData.class,
                Text.class,
                CategoryData.class,
                reduceConf
        );

//        Configuration mapConf3 = new Configuration(false);
//
//        ChainReducer.addMapper(
//                job,
//                SalesSortingMapper.class,
//                Text.class, CategoryData.class,
//                DoubleWritable.class, CategoryData.class,
//                mapConf3
//        );
//
//        JobConf mapConf4 = new JobConf(false);
//
////        mapConf4.setOutputKeyComparatorClass(DecComparator.class);
//
//        ChainReducer.addMapper(
//                job,
//                TextMapper.class,
//                DoubleWritable.class, CategoryData.class,
//                DoubleWritable.class, Text.class,
//                mapConf4
//        );



        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
}