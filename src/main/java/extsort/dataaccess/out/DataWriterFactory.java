package extsort.dataaccess.out;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

public class DataWriterFactory implements IDataWriterFactory {
    public IDataWriter CreateForFile(String fileName) throws IOException {
        Writer writer = new PrintWriter(fileName);
        return new DataLineWriter(writer);
    }
}
