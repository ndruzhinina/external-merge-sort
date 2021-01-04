
import extsort.dataaccess.out.DataLineWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

public class TestFileGenerator {

    private static final List<String> _dict = List.of("Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Antigua and Barbuda", "Argentina", "Armenia", "Australia", "Austria", "Azerbaijan",
            "Bahamas, the", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin", "Bhutan", "Bolivia", "Bosnia and Herzegovina", "Botswana", "Brazil", "Brunei", "Bulgaria", "Burkina Faso", "Burundi",
            "Cabo Verde", "Cambodia", "Cameroon", "Canada", "Central African Republic", "Chad", "China", "Colombia", "Comoros", "Congo, Democratic Republic of the", "Congo, Republic of the", "Costa Rica", "Côte d’Ivoire", "Croatia", "Cuba", "Cyprus", "Czech Republic",
            "Denmark", "Djibouti", "Dominica", "Dominican Republic",
            "East Timor (Timor-Leste)", "Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia", "Eswatini", "Ethiopia",
            "Fiji", "Finland", "France",
            "Gabon", "Gambia, the", "Georgia", "Germany", "Ghana", "Greece", "Grenada", "Guatemala", "Guinea", "Guinea-Bissau", "Guyana",
            "Haiti", "Honduras", "Hungary",
            "Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland", "Israel", "Italy",
            "Jamaica", "Japan", "Jordan",
            "Kazakhstan", "Kenya", "Kiribati", "Korea, North", "Korea, South", "Kosovo", "Kuwait", "Kyrgyzstan",
            "Laos", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya", "Liechtenstein", "Lithuania", "Luxembourg",
            "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall Islands", "Mauritania", "Mauritius", "Mexico", "Micronesia, Federated States of", "Moldova", "Monaco", "Mongolia", "Montenegro", "Morocco", "Mozambique", "Myanmar (Burma)",
            "Namibia", "Nauru", "Nepal", "Netherlands", "New Zealand", "Nicaragua", "Niger", "Nigeria", "North Macedonia", "Norway",
            "Oman",
            "Pakistan", "Palau", "Panama", "Papua New Guinea", "Paraguay", "Peru", "Philippines", "Poland", "Portugal",
            "Qatar",
            "Romania", "Russia", "Rwanda",
            "Saint Kitts and Nevis", "Saint Lucia",
            "Saint Vincent and the Grenadines", "Samoa", "San Marino", "Sao Tome and Principe", "Saudi Arabia", "Senegal", "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Slovakia", "Slovenia", "Solomon Islands", "Somalia", "South Africa", "Spain", "Sri Lanka", "Sudan", "Sudan, South", "Suriname", "Sweden", "Switzerland", "Syria",
            "Taiwan", "Tajikistan", "Tanzania", "Thailand", "Togo", "Tonga", "Trinidad and Tobago", "Tunisia", "Turkey", "Turkmenistan", "Tuvalu",
            "Uganda", "Ukraine", "United Arab Emirates", "United Kingdom", "United States", "Uruguay", "Uzbekistan",
            "Vanuatu", "Vatican City", "Venezuela", "Vietnam",
            "Yemen",
            "Zambia", "Zimbabwe");

    public static void main(String[] args) throws IOException {
        if(args.length == 0 ||
                (args.length == 1 &&
                        (args[0].toLowerCase().equals("help")
                                || args[0].toLowerCase().equals("--help")
                                || args[0].toLowerCase().equals("-h")))) {
            System.out.println("Generator of a random text file");
            printUsage(System.out);
            System.exit(0);
        }

        if(args.length != 4) {
            System.err.println("Error: 4 arguments expected.");
            printUsage(System.err);
            System.exit(1);
        }

        int minRecordLength = Integer.parseInt(args[0]);
        int maxRecordLength = Integer.parseInt(args[1]);
        long maxLength = Long.parseLong(args[2]);
        String outFileName = args[3];

        if(minRecordLength < 0 || maxRecordLength < 0 || minRecordLength >= maxRecordLength || maxLength < maxRecordLength) {
            System.err.println("Error: Invalid numerical parameters.");
            printUsage(System.err);
            System.exit(1);
        }

        System.out.println("Generating file...");
        CreateLinewiseFile(outFileName, minRecordLength, maxRecordLength, maxLength);
    }

    private static void printUsage(PrintStream stream) {
        stream.println("USAGE: TestFileGenerator <minRecordLength> <maxRecordLength> <fileSize> <fileName>");
        stream.println("   <minRecordLength> minimal length of a record (line) in bytes;");
        stream.println("   <maxRecordLength> maximal length of a record (line) in bytes;");
        stream.println("   <fileSize> size of the file to create, in bytes;");
        stream.println("   <fileName> Name of the file to create");
        stream.println("EXAMPLE: TestFileGenerator 100 200 2000000000 test.txt");
        stream.println("   This would create a file named test.txt of approximate size 2Gb, where length of each record (line) varies from 100 to 200 bytes.");
    }

    private static void CreateLinewiseFile(String filename, int minRecordLength, int maxRecordLength, long maxLength) throws IOException {
        DataLineWriter writer = new DataLineWriter(filename);

        int separatorLength = System.lineSeparator().getBytes().length;
        long bytes = 0;

        boolean firstRecord = true;
        long numRecords = 0;
        while(bytes < maxLength) {
            String record = getRecord(minRecordLength, maxRecordLength);
            writer.writeRecord(record);
            bytes += (record.getBytes().length + separatorLength);
            numRecords++;
        }

        System.out.println("File created.");
        System.out.println("Records: " + numRecords);
        System.out.println("Bytes: " + bytes);
        writer.close();
    }

    private static String getRecord(int minLength, int maxLength) {
        HashSet<String> elems = new HashSet<>();
        int dictSize = _dict.size();
        Random rand = new Random();
        int recordBytes = 0;
        int approxRecordLength = rand.nextInt(maxLength - minLength) + minLength;
        StringBuilder sb  = new StringBuilder();
        String elemSeparator = "; ";
        int separatorBytes = elemSeparator.getBytes().length;

        while(recordBytes < approxRecordLength) {
            int index = rand.nextInt(dictSize);
            String elem = _dict.get(index);
            if(elems.add(elem)) {
                if(recordBytes > 0)
                    recordBytes += separatorBytes;
                recordBytes += elem.getBytes().length;
            }
        }

        boolean firstElem = true;
        for(String elem: elems) {
            if(firstElem)
                firstElem = false;
            else
                sb.append(elemSeparator);
            sb.append(elem);
        }

        return sb.toString();
    }
}
