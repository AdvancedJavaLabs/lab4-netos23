package ru.fbtw.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.fbtw.dto.CategoryData;
import ru.fbtw.dto.SalesData;

import java.io.IOException;

public class SalesReducer extends Reducer<Text, SalesData, Text, CategoryData> {
    @Override
    protected void reduce(Text key, Iterable<SalesData> values, Reducer<Text, SalesData, Text, CategoryData>.Context context) throws IOException, InterruptedException {
        double totalRevenue = 0.0;
        int totalQuantity = 0;

        for (SalesData val : values) {
            totalRevenue += val.getRevenue();
            totalQuantity += val.getQuantity();
        }

        context.write(key, new CategoryData(key.toString(), totalRevenue, totalQuantity));

    }
}