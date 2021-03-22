package extsort.dataaccess.in;

import java.io.*;
import java.util.*;

public class DataLineReader implements IDataReader {

    private long lastMemoryBytes;
    private boolean endOfFile;

    private BufferedReader bufferedReader;

    public DataLineReader(Reader reader) throws IOException {
        bufferedReader = new BufferedReader(reader);
        endOfFile = false;
    }

    @Override
    public List<String> readRecords(long maxMemoryBytes) throws IOException {
        List<String> data = new ArrayList<String>();

        long memoryBytes = 0;
        if(bufferedReader.ready()) {
            while (true) {
                if(maxMemoryBytes == 0 || memoryBytes <= maxMemoryBytes) {
                    String line = bufferedReader.readLine();
                    if(line != null) {
                        if(!line.equals("")) {
                            data.add(line);
                            memoryBytes += line.length() * 2;
                        }
                    } else {
                        endOfFile = true;
                        break;
                    }
                } else break;
            }
        }

        lastMemoryBytes = memoryBytes;
        return data;
    }

    @Override
    public String readRecord() throws IOException {
        long memoryBytes = 0;
        String record = null;
        while(true) {
            String line = bufferedReader.readLine();
            if (line != null) {
                if (!line.equals("")) {
                    record = line;
                    lastMemoryBytes = record.length() * 2;
                    break;
                }
            } else {
                endOfFile = true;
                break;
            }
        }

        return record;
    }

    @Override
    public void close()  throws IOException{
        bufferedReader.close();
    }

    @Override
    public long getLastMemoryBytes() {
        return lastMemoryBytes;
    }

    @Override
    public boolean isEOF() {
        return endOfFile;
    }
}
