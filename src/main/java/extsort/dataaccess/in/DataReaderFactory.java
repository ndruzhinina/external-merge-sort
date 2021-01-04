package extsort.dataaccess.in;

import java.io.IOException;

public class DataReaderFactory implements IDataReaderFactory {
    public IDataReader CreateForFile(String fileName) throws IOException {
        return new DataLineReader(fileName);
    }
}
