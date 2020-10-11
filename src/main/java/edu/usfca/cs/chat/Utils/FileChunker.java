package edu.usfca.cs.chat.Utils;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FileChunker {

    private void chunkFile(String filePath) throws IOException {
        File f = new File(filePath);
        int partCounter = 0;

        int sizeOfFile = 10 * 1024; // 128/chunk
//        int sizeOfFile = 128 * 1024 * 1024; // 128/chunk
        byte[] buffer = new byte[sizeOfFile];

        String fileName = f.getName();

        //try-with-resources to ensure closing stream
        try (FileInputStream fis = new FileInputStream(f);
             BufferedInputStream bis = new BufferedInputStream(fis)) {

            int bytesAmount = 0;
            while ((bytesAmount = bis.read(buffer)) > 0) {
                //write each chunk of data into separate file with different number in name
                String filePartName = String.format("%s-%03d", fileName, partCounter++);
                File newFile = new File(f.getParent(), filePartName);
                try (FileOutputStream out = new FileOutputStream(newFile)) {
                    //todo use server channel flush and proto message
                    out.write(buffer, 0, bytesAmount);
                }
                catch(Error e) {
                    System.out.println("Error while writing file in chunkFile " + e);
                }
            }
        }
        catch(Error e) {
            System.out.println("Error while splitting file in chunkFile " + e);
        }
        System.out.println("Total chunks: " + partCounter);
    }

    private void mergeFiles(String[] files, String outputFilePath) throws IOException {
        FileInputStream instream = null;
        FileOutputStream outstream = null;

        files = new String[]{"/Users/nishantmehta/Desktop/Videos/000-My Movie 2.mp4", "/Users/nishantmehta/Desktop/Videos/001-My Movie 2.mp4", "/Users/nishantmehta/Desktop/Videos/002-My Movie 2.mp4", "/Users/nishantmehta/Desktop/Videos/003-My Movie 2.mp4", "/Users/nishantmehta/Desktop/Videos/004-My Movie 2.mp4", "/Users/nishantmehta/Desktop/Videos/005-My Movie 2.mp4", "/Users/nishantmehta/Desktop/Videos/006-My Movie 2.mp4", "/Users/nishantmehta/Desktop/Videos/007-My Movie 2.mp4"};
        outputFilePath = "/Users/nishantmehta/Desktop/Videos/MyMovie2Merged.mov";

        try{
            int i;
            File inFile;
            File outFile = new File(outputFilePath);
            outstream = new FileOutputStream(outFile);

            for(i = 0; i < files.length; i++) {
                inFile = new File(files[i]);

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
//        files = new String[]{"/Users/nishantmehta/Desktop/Videos/000-My Movie 2.mp4", "/Users/nishantmehta/Desktop/Videos/001-My Movie 2.mp4", "/Users/nishantmehta/Desktop/Videos/002-My Movie 2.mp4", "/Users/nishantmehta/Desktop/Videos/003-My Movie 2.mp4", "/Users/nishantmehta/Desktop/Videos/004-My Movie 2.mp4", "/Users/nishantmehta/Desktop/Videos/005-My Movie 2.mp4", "/Users/nishantmehta/Desktop/Videos/006-My Movie 2.mp4", "/Users/nishantmehta/Desktop/Videos/007-My Movie 2.mp4"};
//        outputFilePath = "/Users/nishantmehta/Desktop/Videos/MyMovie2Merged.mov";

        try{
            int i;
            File inFile;
            File outFile = new File(outputFilePath+"wLoader.png");
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


    public static void main(String[] args) {
        FileChunker fileChunker = new FileChunker();

        try {
            fileChunker.chunkFile("nato_trial_file.txt");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
