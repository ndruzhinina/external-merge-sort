package extsort.dataaccess.in;

import java.io.*;
import java.util.*;

public class DataLineReader implements IDataReader {

    private long _lastMemoryBytes;
    private boolean _eof;

    private FileInputStream _fis;
    private InputStreamReader _isr;
    private BufferedReader _br;

    public DataLineReader(String fileName) throws IOException {
        _fis = new FileInputStream(fileName);
        _isr = new InputStreamReader(_fis);
        _br = new BufferedReader(_isr);
        _eof = false;
    }

    @Override
    public List<String> readRecords(long maxMemoryBytes) throws IOException {
        List<String> data = new ArrayList<String>();

        long memoryBytes = 0;
        if(_br.ready()) {
            while (true) {
                if(maxMemoryBytes == 0 || memoryBytes <= maxMemoryBytes) {
                    String line = _br.readLine();
                    if(line != null) {
                        if(!line.isBlank()) {
                            data.add(line);
                            memoryBytes += line.length() * 2;
                        }
                    } else {
                        _eof = true;
                        break;
                    }
                } else break;
            }
        }

        _lastMemoryBytes = memoryBytes;
        return data;
    }

    @Override
    public String readRecord() throws IOException {
        long memoryBytes = 0;
        String record = null;
        while(true) {
            String line = _br.readLine();
            if (line != null) {
                if (!line.isBlank()) {
                    record = line;
                    _lastMemoryBytes = record.length() * 2;
                    break;
                }
            } else {
                _eof = true;
                break;
            }
        }

        return record;
    }

    @Override
    public void close()  throws IOException{
        _br.close();
    }

    @Override
    public long getLastMemoryBytes() {
        return _lastMemoryBytes;
    }

    @Override
    public boolean isEOF() {
        return _eof;
    }
}
