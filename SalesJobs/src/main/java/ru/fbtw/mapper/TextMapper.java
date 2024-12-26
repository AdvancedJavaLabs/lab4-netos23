package ru.fbtw.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.fbtw.dto.CategoryData;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class TextMapper extends Mapper<DoubleWritable, CategoryData, DoubleWritable, Text> {
    @Override
    protected void map(DoubleWritable key, CategoryData value, Mapper<DoubleWritable, CategoryData, DoubleWritable, Text>.Context context) throws IOException, InterruptedException {

        String formatted = String.format("%s %.4f %d", value.getCategory(), value.getTotalRevenue(), value.getTotalQuantity());
        context.write(key, new Text(formatted));
    }
}