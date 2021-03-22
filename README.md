# external-merge-sort
External-merge-sort application is for the external sort of strings in a *.txt file. 
External-merge-sort project contains two part.
First part let you generate a random text file with strings which contains names of countries in random order.
To create the file it is necessary to provide in the console 4 input values: minimal length of a record (line) in bytes, maximal length of a record (line) in bytes, size of the file to create, in bytes and Name of the file to create.
The example of input string: "java TestFileGenerator 1000 2000 2000000000 inputText.txt"
This would generate a file of ~20GB size, where each line varies between 1000 and 2000 bytes 

You can sort the created file or any other one using ExtSort. To run the application it is necessary to provide with three input arguments: the input file name, the name of the file to write sorted data to and size of the  buffer for in-memory sorting in bytes.
The example of input string: "java -jar ExtSort-SNAPSHOT.jar inputText.txt outputText.txt 1000000000"
This will run the algorithm, allocating ~1GB of memory for each chunk.
For the file generated using the 1st command, the number of chunks should be 40, because of internal Unicode string type representation in JRE. 
