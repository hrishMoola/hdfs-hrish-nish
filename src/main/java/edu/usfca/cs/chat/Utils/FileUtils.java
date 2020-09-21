package edu.usfca.cs.chat.Utils;

import com.google.protobuf.ByteString;
import edu.usfca.cs.chat.DfsMessages;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class FileUtils {

    public static void writeToFile(DfsMessages.FileChunk fileChunk) throws Exception{
        String fileName = fileChunk.getFilepath();
        String directoryName = fileName.split("-")[0];
        System.out.println("directoryName = " + directoryName);

        Path path = Paths.get("storage/" + directoryName);
        Files.createDirectories(path);

        path = Paths.get("storage/" + directoryName + "/" + fileName);
        Files.write(path, fileChunk.getChunks().toByteArray());
    }


    public static void main(String[] args) throws Exception{

        String test = "This is just some random text to write";
        DfsMessages.FileChunk fileChunkMessage = DfsMessages.FileChunk.newBuilder().setFilepath("nato_trial_file.txt-000").setChunks(ByteString.copyFrom(test.getBytes())).build();
        DfsMessages.FileChunk fileChunkMessage2 = DfsMessages.FileChunk.newBuilder().setFilepath("nato_trial_file.txt-001").setChunks(ByteString.copyFrom(test.getBytes())).build();
        DfsMessages.FileChunk fileChunkMessage3 = DfsMessages.FileChunk.newBuilder().setFilepath("nato_trial_file.txt-002").setChunks(ByteString.copyFrom(test.getBytes())).build();

        writeToFile(fileChunkMessage);
        writeToFile(fileChunkMessage2);
        writeToFile(fileChunkMessage3);
    }

}
