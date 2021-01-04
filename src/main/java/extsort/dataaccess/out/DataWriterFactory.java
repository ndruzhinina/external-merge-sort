package extsort.dataaccess.out;

import java.io.IOException;

public class DataWriterFactory implements IDataWriterFactory {
    public IDataWriter CreateForFile(String fileName) throws IOException {
        return new DataLineWriter(fileName);
    }
}
