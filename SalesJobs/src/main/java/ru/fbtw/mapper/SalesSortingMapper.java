package ru.fbtw.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.fbtw.dto.CategoryData;

import java.io.IOException;

public class SalesSortingMapper extends Mapper<Text, CategoryData, DoubleWritable, CategoryData> {
    @Override
    protected void map(Text key, CategoryData value, Mapper<Text, CategoryData, DoubleWritable, CategoryData>.Context context) throws IOException, InterruptedException {
        context.write(new DoubleWritable(value.getTotalRevenue()), value);
    }
}