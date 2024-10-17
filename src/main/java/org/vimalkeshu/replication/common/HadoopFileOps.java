package org.vimalkeshu.replication.common;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class HadoopFileOps {
    private static Logger logger = LoggerFactory.getLogger(HadoopFileOps.class);
    private HadoopFileOps(){}

    public static void copy(String sourceCatalogName,
                            String source,
                            String targetCatalogName,
                            String target,
                            int blockSize) throws Exception {
        FileSystem sourceFileSystem = HadoopFileSystemOps.getFileSystemInstance(sourceCatalogName, source);
        FileSystem targetFileSystem = HadoopFileSystemOps.getFileSystemInstance(targetCatalogName, target);

        Path sourcePath = new Path(source);
        Path targetPath = new Path(target);

        if (!sourceFileSystem.exists(sourcePath))
            throw new Exception("Not able to find the file: "+sourcePath.getName());

        if (targetFileSystem.exists(targetPath))
            targetFileSystem.delete(targetPath, true);

        try(FSDataInputStream fsDataInputStream = sourceFileSystem.open(sourcePath);
            FSDataOutputStream fsDataOutputStream = targetFileSystem.create(targetPath)){
            IOUtils.copyBytes(fsDataInputStream, fsDataOutputStream, blockSize, true);
        }
    }

    public static void copy(FileSystem sourceFileSystem,
                            Path sourcePath,
                            FileSystem targetFileSystem,
                            Path targetPath,
                            int blockSize) throws Exception{
        try(FSDataInputStream fsDataInputStream = sourceFileSystem.open(sourcePath);
            FSDataOutputStream fsDataOutputStream = targetFileSystem.create(targetPath)){
            IOUtils.copyBytes(fsDataInputStream, fsDataOutputStream, blockSize, true);
        }
    }

    public static void delete(String catalogName, String deletePath) throws Exception{
        FileSystem fileSystem = HadoopFileSystemOps.getFileSystemInstance(catalogName, deletePath);;
        Path path = new Path(deletePath);
        fileSystem.delete(path, true);
    }

    public static void create(String catalogName, String dirPath) throws Exception{
        FileSystem fileSystem = HadoopFileSystemOps.getFileSystemInstance(catalogName, dirPath);
        Path path = new Path(dirPath);
        if (!fileSystem.exists(path)) fileSystem.mkdirs(path);
    }

    public static boolean exist(String catalogName, String dirPath) throws Exception {
        FileSystem fileSystem = HadoopFileSystemOps.getFileSystemInstance(catalogName, dirPath);
        Path path = new Path(dirPath);
        return fileSystem.exists(path);
    }
}
