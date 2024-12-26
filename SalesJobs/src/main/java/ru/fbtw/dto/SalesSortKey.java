package ru.fbtw.dto;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
public class SalesSortKey implements Writable {

    String category;
    double totalPrice;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(category);
        dataOutput.writeDouble(totalPrice);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        category = dataInput.readUTF();
        totalPrice = dataInput.readDouble();
    }

}