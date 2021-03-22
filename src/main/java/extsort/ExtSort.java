package extsort;

import java.io.*;

import extsort.business.ExternalMergeSorter;
import extsort.dataaccess.FileManager;
import extsort.dataaccess.in.DataReaderFactory;
import extsort.dataaccess.out.DataWriterFactory;

public class ExtSort {
    public static void main(String[] args) {

        if (args.length != 3) {
            System.out.println("Error: 3 arguments expected.");
            System.err.println("Usage: ExtMergeSort <inputFile> <outputFile> <chunkSize>");
            System.err.println("   <inputFile>: input file name.");
            System.err.println("   <outputFile>: name of the file to write sorted data to.");
            System.err.println("   <chunkSize>: size of the chunk (e.g. buffer for in-memory sorting) in bytes.");
            System.exit(1);
        }

        String inFileName = args[0];
        String outFileName = args[1];
        int chunkSize = 0;
        try {
            chunkSize = Integer.parseInt(args[2]);
        } catch (NumberFormatException ex) {
            System.err.println("Failed to parse the chunk size.");
            System.exit(1);
        }

        if (chunkSize < 10000) {
            System.err.println("Invalid chunk size. The size is expected to be greater than 9999");
            System.exit(1);
        }

        ExternalMergeSorter sorter = new ExternalMergeSorter(
                inFileName,
                outFileName,
                chunkSize,
                new DataReaderFactory(),
                new DataWriterFactory(),
                new FileManager());

        try {
            sorter.run();
        } catch (IOException ex) {
            System.err.println("I/O exception: " + ex.getMessage());
            System.exit(1);
        }

        System.exit(0);
    }
}

