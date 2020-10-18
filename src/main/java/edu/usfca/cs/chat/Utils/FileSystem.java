package edu.usfca.cs.chat.Utils;



import java.util.*;

public class FileSystem {

    private static class Node{
        private Node parentNode;
        private String name;
        private Set<Node> children = new HashSet<>();

        public Node(Node parentNode, String name) {
            this.parentNode = parentNode;
            if (this.parentNode != null) {
                this.parentNode.children.add(this);
            }
            this.name = name;
        }

        public Node getRoot() {
            return this.parentNode == null ? this : this.parentNode.getRoot();
        }

        public void merge( Node node) {
            if (!this.name.equals(node.name)) {
                return;
            } else if (this.children.isEmpty()) {
                this.children.addAll(node.children);
                return;
            }

            Node[] thisChildren = this.children.toArray(new Node[this.children.size()]);
            for (Node thisChild : thisChildren) {
                for (Node thatChild : node.children) {
                    if (thisChild.name.equals(thatChild.name)) {
                        thisChild.merge(thatChild);
                    } else if (!this.children.contains(thatChild)) {
                        this.children.add(thatChild);
                    }
                }
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.name, this.parentNode);
        }

        @Override
        public boolean equals( Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Node that = (Node) o;
            return Objects.equals(this.name, that.name)
                    && Objects.equals(this.parentNode, that.parentNode);
        }


        @Override
        public String toString() {
            return "{" +
                    "name='" + this.name + '\'' +
                    ", children=" + this.children +
                    '}';
        }

        public String traverse(Node root, String prefix, String fileSoFar, int currIndex){
            StringBuilder sb = new StringBuilder();
            String[] paths = prefix.split("/");
            if(prefix.equalsIgnoreCase(root.name)){
                return getContents(root.children);
            }
            if (currIndex == paths.length - 1 && paths.length > 1){
                if(fileSoFar.equalsIgnoreCase(prefix))
                    return getContents(root.children);
                else
                    return "File doesn't exist";
            }
            StringBuilder fileSoFarBuilder = new StringBuilder(fileSoFar);
            if(fileSoFar.equalsIgnoreCase("")) {
                if (!paths[0].equalsIgnoreCase(root.name))
                    return "File doesn't exist";
                fileSoFarBuilder.append(paths[0]);
            }
            for(Node node : root.children){
                if(paths[currIndex + 1].equalsIgnoreCase(node.name)){
                    fileSoFarBuilder.append("/").append(node.name);
                    if(fileSoFarBuilder.toString().equalsIgnoreCase(prefix)){
                        return getContents(node.children);
                    } else {
                        return traverse(node, prefix, fileSoFarBuilder.toString(), currIndex + 1);
                    }
                }
            }
            return "File doesn't exist";
        }

        private String getContents(Set<Node> children) {
            StringBuilder sb = new StringBuilder();
            for (Node child : children){
                sb.append(child.children.size() > 0 ? child.name + "/" : child.name).append("\n");
            }
            return sb.toString();
        }

    }

    Set<String> paths;
    Node root;

    public FileSystem(){
        paths = new HashSet<>();
        root = null;
    }

    public void addPaths(List<String> newPaths){
        paths.addAll(newPaths);
    }
    public void makeFileSystem(){
        for (String rawPath : paths) {
            String path = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
            String[] pathElements = path.split("/");
            Node currNode = null;
            for (String pathElement : pathElements) {
                currNode = new Node(currNode, pathElement);
            }
            if (root == null) {
                root = currNode.getRoot();
            } else {
                root.merge(currNode.getRoot());
            }
        }
    }

    public String traverseFile(String prefix){
        Node node = root;
        return root.traverse(root, prefix, "", 0);
    }
    public static void main(String[] args) {
        List<String> paths = new ArrayList<>();

        FileSystem fileSystem = new FileSystem();
        paths.add("dfs/nato1");
        paths.add("dfs/nato2");
        paths.add("dfs/nato3/file");
        paths.add("dfs/nato3/file2");
        fileSystem.addPaths(paths);
        paths.clear();
        paths.add("dfs/nato3/file3");
        paths.add("dfs/nato3/file4/file");
        fileSystem.addPaths(paths);
        paths.clear();
        paths.add("dfs/nato3/file5/file");
        paths.add("dfs/nato13/file2");
        fileSystem.addPaths(paths);

        fileSystem.makeFileSystem();
//        System.out.println(fileSystem.traverseFile("dfs/nato7"));
        System.out.println("");
        System.out.println(fileSystem.traverseFile("dfs"));
        System.out.println("");
        System.out.println(fileSystem.traverseFile("dfs/nato3"));


//        paths.add("dfs/nato1");
//        paths.add("dfs/nato2");
//        paths.add("dfs/nato3/file");
//        paths.add("dfs/nato3/file2");
//        paths.add("dfs/nato3/file3");
//        paths.add("dfs/nato3/file4/file");
//        paths.add("dfs/nato3/file5/file");
//        paths.add("dfs/nato13/file2");
//        Node treeRootNode = null;
//        for (String rawPath : paths) {
//            String path = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
//            String[] pathElements = path.split("/");
//            Node currNode = null;
//            for (String pathElement : pathElements) {
//                currNode = new Node(currNode, pathElement);
//            }
//
//            if (treeRootNode == null) {
//                treeRootNode = currNode.getRoot();
//            } else {
//                treeRootNode.merge(currNode.getRoot());
//            }
//        }
//
//        System.out.println(treeRootNode);
//        System.out.println(treeRootNode.traverse(treeRootNode, "dfs/nato7", "", 0));
//        System.out.println(treeRootNode.traverse(treeRootNode, "dfs", "", 0));
//        System.out.println(treeRootNode.traverse(treeRootNode, "dfs/nato3", "", 0));


    }

}
