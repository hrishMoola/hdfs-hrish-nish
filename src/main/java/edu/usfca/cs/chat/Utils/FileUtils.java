package edu.usfca.cs.chat.Utils;

import com.google.protobuf.ByteString;
import edu.usfca.cs.chat.DfsMessages;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class FileUtils {

    public static void writeToFile(DfsMessages.FileChunk fileChunk, String storagePath) throws Exception{
        String fileName = fileChunk.getFilepath();
        String directoryName = fileName.split("-")[0];
        System.out.println("directoryName = " + directoryName);


        Path path = Paths.get(storagePath + "/" + directoryName);
        Files.createDirectories(path);

        path = Paths.get(storagePath + "/"  + directoryName + "/" + fileName);
        Files.write(path, fileChunk.getChunks().toByteArray());
    }

    public static void prepareLocalForStorage(){

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


    public static DfsMessages.FileRequest getFileRequest(String localFile, String dfsPath){
        File f = new File(localFile);
        return DfsMessages.FileRequest.newBuilder()
                .setFilepath(localFile).setType(DfsMessages.FileRequest.Type.STORE)
                .setNumChunks(new Double(Math.ceil(f.length() / 51200.0)).intValue()).setSize(f.length()).build();
    }
    public static void main(String[] args) throws Exception{

        String test = "This is just some random text to write";
        DfsMessages.FileChunk fileChunkMessage = DfsMessages.FileChunk.newBuilder().setFilepath("nato_trial_file.txt-000").setChunks(ByteString.copyFrom(test.getBytes())).build();
        DfsMessages.FileChunk fileChunkMessage2 = DfsMessages.FileChunk.newBuilder().setFilepath("nato_trial_file.txt-001").setChunks(ByteString.copyFrom(test.getBytes())).build();
        DfsMessages.FileChunk fileChunkMessage3 = DfsMessages.FileChunk.newBuilder().setFilepath("nato_trial_file.txt-002").setChunks(ByteString.copyFrom(test.getBytes())).build();

//        writeToFile(fileChunkMessage);
//        writeToFile(fileChunkMessage2);
//        writeToFile(fileChunkMessage3);

        System.out.println(getFileRequest("nato_trial_file.txt", "something/nato_file"));
    }

}
