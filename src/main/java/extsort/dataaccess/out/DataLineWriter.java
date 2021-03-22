package extsort.dataaccess.out;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.List;

public class DataLineWriter implements IDataWriter {

    private Writer writer;

    public DataLineWriter(Writer writer) throws IOException {
        this.writer = writer;
    }

    @Override
    public void writeRecords(List<String> data) throws IOException {
        for(String record: data) {
            writeRecord(record);
        }
    }

    @Override
    public void writeRecord(String record) throws IOException {
        writer.write(record);
        writer.write(System.lineSeparator());
    }

    @Override
    public void close() throws  IOException {
        writer.close();
    }
}
