package edu.usfca.cs.chat.Utils;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FileChunker {



    public static void mergeFiles(String inputFilePath, String outputFilePath) throws IOException {
        FileInputStream instream = null;
        FileOutputStream outstream = null;

        List<String> files = Arrays.stream(Objects.requireNonNull(new File(inputFilePath).listFiles())).map(ele -> {
            try {
                return ele.getCanonicalPath();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }).sorted(Comparator.naturalOrder()).collect(Collectors.toList());
        try{
            int i;
            File inFile;
            File outFile = new File(outputFilePath);
            outstream = new FileOutputStream(outFile);

            for(i = 0; i < files.size(); i++) {
                inFile = new File(files.get(i));

                instream = new FileInputStream(inFile);

                byte[] buffer = new byte[1024];

                int length;
                /* copying the contents from input stream to
                 * output stream using read and write methods
                 */
                while ((length = instream.read(buffer)) > 0){
                    outstream.write(buffer, 0, length);
                }

                // Closing the input file streams
                instream.close();
            }
            outstream.close();

            System.out.println("File copied successfully!!");
        }catch(IOException ioe){
            ioe.printStackTrace();
        }
    }

}
