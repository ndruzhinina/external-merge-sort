package extsort.dataaccess;

import java.io.File;

public class FileManager implements IFileManager {
    @Override
    public void delete(String fileName) {
        File file = new File(fileName);
        file.delete();
    }
}
