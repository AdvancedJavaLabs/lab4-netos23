package ru.fbtw.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.fbtw.dto.SalesData;

import java.io.IOException;

public class SalesMapper extends Mapper<LongWritable, Text, Text, SalesData> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, SalesData>.Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        int transactionId = Integer.parseInt(fields[0]);
        int productId = Integer.parseInt(fields[1]);
        String category = fields[2];
        double price = Double.parseDouble(fields[3]);
        int quantity = Integer.parseInt(fields[4]);

        context.write(
                new Text(category),
                new SalesData(
                        transactionId,
                        productId,
                        category,
                        price,
                        quantity
                )
        );
    }
}