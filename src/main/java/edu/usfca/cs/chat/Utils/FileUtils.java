package edu.usfca.cs.chat.Utils;

import com.google.protobuf.ByteString;
import edu.usfca.cs.chat.DfsMessages;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;


public class FileUtils {

    public static DfsMessages.OnNodeDown createOnNodeDownMsg(String ip, List<DfsMessages.DataNodeMetadata> replicaNodes) {
        return DfsMessages.OnNodeDown.newBuilder()
                .setIp(ip)
                .addAllAffectedNodes(replicaNodes)
                .build();
    }

    public static void writeToFile(DfsMessages.FileChunk fileChunk, String storagePath, String type) throws Exception{
        String fileName = fileChunk.getFilepath();
        System.out.println("file name in writetoFile: " + fileName);

        String directoryName = fileName.split("-")[0];
        String chunkNum = fileName.split("-")[1];
        System.out.println("directoryName = " + directoryName);
        System.out.println(storagePath + type  + directoryName + "/chunk-" + chunkNum);

        Path path = Paths.get(storagePath + type + directoryName);
        Files.createDirectories(path);

        path = Paths.get(storagePath + type  + directoryName + "/chunk-" + chunkNum);
        Files.write(path, fileChunk.getChunks().toByteArray());

        path = Paths.get(storagePath + type  + directoryName + "/chunk-" + chunkNum+ "_checksum");
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

    public static File findDir(String name, File file) {
        File[] list = file.listFiles();
        File foundFile = null;
        if(list!=null)
            for (File innerFile : list)
            {
                if (name.equals(innerFile.getName())) {
                    foundFile = innerFile;
                    break;
                }
                if (innerFile.isDirectory())
                {
                    return findDir(name,innerFile);
                }
            }
        return foundFile;
    }

    public static void clearDirectoryContents(String storagePath) {
        try {
            File dir = new File(storagePath);
            purgeDirectory(dir);
        }
        catch (Exception e) {
            System.out.println(e + " Error clearing storage node contents before startup");
        }
    }

    public static synchronized DfsMessages.FileChunk getChunks(File filepath, DfsMessages.FileChunkHeader fileChunkHeader, boolean patchForNode) throws IOException {

        byte[] fileChunkString = Files.readAllBytes(Paths.get(filepath.getCanonicalPath()));
        byte[]  checkSum = Files.readAllBytes(Paths.get(filepath.getCanonicalPath() + "_checksum"));
        if(Arrays.equals(getCRC32Checksum(ByteString.copyFrom(fileChunkString).toByteArray()), checkSum)){
            System.out.println("issokay");
            if(!patchForNode)
                return DfsMessages.FileChunk.newBuilder().setFilepath(filepath.getName()).setChunks(ByteString.copyFrom(fileChunkString)).setFilechunkHeader(fileChunkHeader).build();
            return DfsMessages.FileChunk.newBuilder().setChunks(ByteString.copyFrom(fileChunkString)).setType(DfsMessages.FileChunk.Type.Patch).build();

        } else { //request replica
            System.out.println("File is corrupted");
            Files.delete(Paths.get(filepath.getCanonicalPath()));
            Files.delete(Paths.get(filepath.getCanonicalPath() + "_checksum"));
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

//        File chunk1OrgFile = new File("/Users/nishantmehta/Desktop/Videos/tiktokVer.mov");
//        File chunkFinal = new File("/Users/nishantmehta/Desktop/Videos/tiktokVer.mov");

        byte[] chunk1Og = getCRC32Checksum(Files.readAllBytes(Paths.get("/Users/nishantmehta/Desktop/CS677/P1-hrishikesh/_retrieved")));
        byte[] chunkFinal = getCRC32Checksum(Files.readAllBytes(Paths.get("/Users/nishantmehta/Desktop/CS677/P1-hrishikesh/_retrieved")));

        System.out.println("CHECKSUM");
        System.out.println(Arrays.equals(chunk1Og, chunkFinal));

        String storageDirectory = "storage1/original/";
        String file = "nato_trial_file.txt";
        File dir = new File(storageDirectory + file);
//        List<DfsMessages.FileChunk> chunks = new ArrayList<>();
//        for (File eachFile: dir.listFiles()) {
////            if (!eachFile.getName().contains("checksum") &&  !eachFile.getName().contains("metadata"))
////                chunks.add(getChunks(eachFile, fileChunkMetadataMap.get(filepath)));
//        }

        System.out.println(getFileName("dfs/nato1"));
//        System.out.println(chunks);
    }

    public static String getDirPath(String filepath) {
        System.out.println("filepath: " + filepath);
        int fileNameStart = filepath.lastIndexOf('/');
        System.out.println("fileNameStart: " + fileNameStart);

        if(fileNameStart == -1) return "";
        return filepath.substring(0, fileNameStart);
    }

    public static String getFileName(String filepath) {
        int fileNameEnd = filepath.lastIndexOf('/');
        if(fileNameEnd == -1) return "";
        return filepath.substring(fileNameEnd + 1);
    }

    public static boolean doesFileExist(String filepath) {
        System.out.println(new File(filepath).isFile());
        return new File(filepath).isFile();
    }
}
