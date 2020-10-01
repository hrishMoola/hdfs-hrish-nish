package edu.usfca.cs.chat.Utils;

import com.google.protobuf.ByteString;
import edu.usfca.cs.chat.DfsMessages;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;


public class FileUtils {

    public static void writeToFile(DfsMessages.FileChunk fileChunk, String storagePath) throws Exception{
        String fileName = fileChunk.getFilepath();
        String directoryName = fileName.split("-")[0];
        System.out.println("directoryName = " + directoryName);


        Path path = Paths.get(storagePath + "/original/" + directoryName);
        Files.createDirectories(path);

        path = Paths.get(storagePath + "/original/"  + directoryName + "/" + fileName);
        Files.write(path, fileChunk.getChunks().toByteArray());

        path = Paths.get(storagePath + "/original/"  + directoryName + "/" + fileName + "_checksum");
        Files.write(path, getCRC32Checksum(fileChunk.getChunks().toByteArray()));
    }

    public static void storeFile(String fileName, byte[] fileContents) throws IOException {
        Path path = Paths.get(fileName);
        Files.write(path, fileContents);
    }
    public static byte[] getCRC32Checksum(byte[] bytes) {
        Checksum crc32 = new CRC32();
        crc32.update(bytes, 0, bytes.length);
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(crc32.getValue()).array();
    }

    private static void purgeDirectory(File dir) {
        for (File file: dir.listFiles()) {
            if (file.isDirectory())
                purgeDirectory(file);
            file.delete();
        }
    }

    public static void clearDirectoryContents(String storagePath) {
        try {
            File dir = new File(storagePath);
            if(!dir.isDirectory()) throw new Error("Storage path given is not a directory");
            purgeDirectory(dir);
        }
        catch (Exception e) {
            System.out.println(e + " Error clearing storage node contents before startup");
        }
    }

    public static DfsMessages.FileChunk getChunks(File filepath) throws IOException {

        byte[] fileChunkString = Files.readAllBytes(Paths.get(filepath.getCanonicalPath()));
        byte[]  checkSum = Files.readAllBytes(Paths.get(filepath.getCanonicalPath() + "_checksum"));
        if(Arrays.equals(getCRC32Checksum(ByteString.copyFrom(fileChunkString).toByteArray()), checkSum)){
            System.out.println("issokay");
            return DfsMessages.FileChunk.newBuilder().setFilepath(filepath.getName()).setChunks(ByteString.copyFrom(fileChunkString)).build();
        } else { //request replica
            System.out.println("Time to request replicas");
        }
        return null;
    }


    public static DfsMessages.FileRequest getFileRequest(String localFile, String dfsPath, double chunkSize){
        File f = new File(localFile);
        return DfsMessages.FileRequest.newBuilder()
                .setFilepath(dfsPath).setType(DfsMessages.FileRequest.Type.STORE)
                .setDirectory(localFile)
                .setNumChunks(new Double(Math.ceil(f.length() / chunkSize)).intValue()).setSize(f.length()).build();
    }
    public static void main(String[] args) throws Exception{

        String test = "This is just some random text to write";
        DfsMessages.FileChunk fileChunkMessage = DfsMessages.FileChunk.newBuilder().setFilepath("nato_trial_file.txt-000").setChunks(ByteString.copyFrom(test.getBytes())).build();
        DfsMessages.FileChunk fileChunkMessage2 = DfsMessages.FileChunk.newBuilder().setFilepath("nato_trial_file.txt-001").setChunks(ByteString.copyFrom(test.getBytes())).build();
        DfsMessages.FileChunk fileChunkMessage3 = DfsMessages.FileChunk.newBuilder().setFilepath("nato_trial_file.txt-002").setChunks(ByteString.copyFrom(test.getBytes())).build();

//        writeToFile(fileChunkMessage);
//        writeToFile(fileChunkMessage2);
//        writeToFile(fileChunkMessage3);
//        Path path = Paths.get("_checksum");
//        Files.write(path, getCRC32Checksum(test.getBytes()));
//        System.out.println("getCRC32Checksum(test.getBytes()) = " + Arrays.toString(getCRC32Checksum(test.getBytes())));
//
//        path = Paths.get("_checksum");
//        String read = Arrays.toString(Files.readAllBytes(path));
//        System.out.println("read = " + read);
//        System.out.println(getFileRequest("nato_trial_file.txt", "something/nato_file", 0.0));


        String storageDirectory = "storage1/original/";
        String file = "nato_trial_file.txt";
        File dir = new File(storageDirectory + file);
        List<DfsMessages.FileChunk> chunks = new ArrayList<>();
        for (File eachFile: dir.listFiles()) {
            if (!eachFile.getName().contains("checksum") &&  !eachFile.getName().contains("metadata"))
                chunks.add(getChunks(eachFile));
        }
        System.out.println(chunks);
    }

    public static String getDirPath(String filepath) {
        System.out.println("filepath: " + filepath);
        int fileNameStart = filepath.lastIndexOf('/');
        System.out.println("fileNameStart: " + fileNameStart);

        if(fileNameStart == -1) return "";
        return filepath.substring(0, fileNameStart);
    }

    public static boolean doesFileExist(String filepath) {
        System.out.println(new File(filepath).isFile());
        return new File(filepath).isFile();
    }

}
