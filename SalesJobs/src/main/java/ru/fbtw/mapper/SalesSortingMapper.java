package ru.fbtw.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import ru.fbtw.dto.CategoryData;

import java.io.IOException;

public class SalesSortingMapper extends Mapper<Writable, Text, DoubleWritable, CategoryData> {
    @Override
    protected void map(Writable key, Text value, Mapper<Writable, Text, DoubleWritable, CategoryData>.Context context) throws IOException, InterruptedException {

        String[] s = value.toString().split("\t");

        CategoryData data = new CategoryData(
                s[0],
                Double.parseDouble(s[1]),
                Integer.parseInt(s[2])
        );

        context.write(new DoubleWritable(data.getTotalRevenue()), data);
    }
}