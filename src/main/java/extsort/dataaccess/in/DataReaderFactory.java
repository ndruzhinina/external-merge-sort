package extsort.dataaccess.in;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class DataReaderFactory implements IDataReaderFactory {
    public IDataReader CreateForFile(String fileName) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(fileName);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
        return new DataLineReader(inputStreamReader);
    }
}
