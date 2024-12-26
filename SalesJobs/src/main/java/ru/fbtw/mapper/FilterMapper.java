package ru.fbtw.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import ru.fbtw.dto.SalesData;

import java.io.IOException;

public class FilterMapper extends Mapper<Writable, Text, LongWritable, Text> {

    @Override
    protected void map(Writable key, Text value, Mapper<Writable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        if (fields.length != 5) {
            return;
        }

        if (fields[0].equals("transaction_id")) {
            return;
        }

        context.write(new LongWritable(Long.parseLong(fields[0])), value);
    }
}
