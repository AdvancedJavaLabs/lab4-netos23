package ru.fbtw.dto;


import lombok.*;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


@Getter
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
public class SalesData implements Writable {
    int transactionId;
    int productId;
    String category;
    double price;
    int quantity;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(transactionId);
        dataOutput.writeInt(productId);
        dataOutput.writeUTF(category);
        dataOutput.writeDouble(price);
        dataOutput.writeInt(quantity);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        transactionId = dataInput.readInt();
        productId = dataInput.readInt();
        category = dataInput.readUTF();
        price = dataInput.readDouble();
        quantity = dataInput.readInt();
    }

    public double getRevenue() {
        return quantity * price;
    }
}
