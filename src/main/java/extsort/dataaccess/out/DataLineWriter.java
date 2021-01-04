package extsort.dataaccess.out;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class DataLineWriter implements IDataWriter {

    private PrintWriter _pw;

    public DataLineWriter(String fileName) throws IOException {
        _pw = new PrintWriter(fileName);
    }

    @Override
    public void writeRecords(List<String> data) throws  IOException {
        for(String record: data) {
            writeRecord(record);
        }
    }

    @Override
    public void writeRecord(String record) throws IOException {
        _pw.println(record);
    }

    @Override
    public void close() throws  IOException {
        _pw.close();
    }
}
