package ru.fbtw.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.fbtw.dto.CategoryData;

import java.io.IOException;

public class ResultReducer extends Reducer<DoubleWritable, CategoryData, Text, Text> {
    @Override
    protected void reduce(DoubleWritable key, Iterable<CategoryData> values, Reducer<DoubleWritable, CategoryData, Text, Text>.Context context) throws IOException, InterruptedException {
        for(CategoryData c : values) {
            String formatted = String.format("%.4f %d", c.getTotalRevenue(), c.getTotalQuantity());
            context.write(new Text(c.getCategory()), new Text(formatted));
        }
    }
}