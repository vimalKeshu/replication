package org.vimalkeshu.replication.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;

@ThreadSafe
public class HadoopFileSystemOps {
    private static final Logger logger = LoggerFactory.getLogger(HadoopFileSystemOps.class);
    private HadoopFileSystemOps(){}

    public static FileSystem getFileSystemInstance(String catalogName, String path) throws Exception {
        return FileSystem.get(getConfig(catalogName, path));
    }
    /**
     * Todo: create the environment variables dynamically to avoid any modification of the code whenever new catalog will be added.
     */
    public static Configuration getConfig(String catalogName, final String path) throws Exception {
        if (path.startsWith(ReplicationConstant.HDFS_FILE_SCHEME)) {
            String upperCaseCatalogName = catalogName.toUpperCase().replace("-", "_");
            String envUserName = upperCaseCatalogName + ReplicationConstant.ENV_HADOOP_USER_PREFIX;
            String envHadoopHomeDirPath = upperCaseCatalogName + ReplicationConstant.ENV_HADOOP_HOME_PREFIX;
            String envKerberosPrincipal = upperCaseCatalogName + ReplicationConstant.ENV_HADOOP_KERBEROS_PRINCIPAL_PREFIX;
            String envKeytabPath = upperCaseCatalogName + ReplicationConstant.ENV_HADOOP_KERBEROS_KEYTAB_PATH_PREFIX;
            logger.info("hdfs file scheme detected for path: "+path);
            return getHdfsFileSystemInstance(
                    path,
                    envUserName,
                    envHadoopHomeDirPath,
                    envKerberosPrincipal,
                    envKeytabPath);
        } else if (path.startsWith(ReplicationConstant.S3A_FILE_SCHEME)) {
            logger.info("s3a file scheme detected for path: "+path);
            return getMinioFileSystemInstance(path, catalogName);
        } else if (path.startsWith(ReplicationConstant.GCS_FILE_SCHEME)) {
            logger.info("gs file scheme detected for path: "+path);
            return getGcsCatalogFileSystemInstance(path, catalogName);
        } else if (path.startsWith(ReplicationConstant.LOCAL_FILE_SCHEME)
                || path.startsWith(ReplicationConstant.FILE_SCHEME)) {
            logger.info("local/file scheme detected for path: "+path);
            return getLocalFileSystemInstance(path);
        } else {
            throw new RuntimeException("Unknown catalog name "+catalogName);
        }
    }
    public static Configuration getLocalFileSystemInstance(String path) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("fs.defaultFS", path);
        return conf;
    }
    public static Configuration getHdfsFileSystemInstance(String path,
                                                          String userName,
                                                          String hadoopHomeDirPath,
                                                          String kerberosPrincipal,
                                                          String keytabPath) throws Exception {
        logger.info("path: "+path);
        logger.info("userName: "+userName);
        logger.info("hadoopHomeDirPath: "+hadoopHomeDirPath);
        logger.info("kerberosPrincipal: "+kerberosPrincipal);
        logger.info("keytabPath: "+keytabPath);

        System.setProperty("HADOOP_USER_NAME", userName);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.addResource(new Path(new File(System.getenv(hadoopHomeDirPath) + "/core-site.xml").toURI()));
        conf.addResource(new Path(new File(System.getenv(hadoopHomeDirPath) + "/hdfs-site.xml").toURI()));
        conf.set("fs.defaultFS", path);
        conf.set("hadoop.security.authentication", "kerberos");

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(System.getenv(kerberosPrincipal),
                System.getenv(keytabPath));

        return conf;
    }
    public static Configuration getGcsCatalogFileSystemInstance(String path, String catalogName) throws Exception {
        String upperCaseCatalogName = catalogName.toUpperCase().replace("-", "_");
        String jsonKeyFileEnvVarName = upperCaseCatalogName + ReplicationConstant.ENV_GCS_SECRET_PREFIX;
        logger.info("json key file env variable name: "+jsonKeyFileEnvVarName);
        String jsonKeyFilePath = System.getenv(jsonKeyFileEnvVarName);

        Configuration conf = new Configuration();
        conf.set("fs.gs.impl", com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.class.getName());
        conf.set("fs.AbstractFileSystem.gs.impl", com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS.class.getName());
        conf.set("google.cloud.auth.service.account.enable", "true");
        conf.set("fs.defaultFS", path);

        // make keyfile json path optional, since in GKE we may use workload identity
        if(jsonKeyFilePath != null && ! jsonKeyFilePath.isEmpty()){
            logger.info("Keyfile JSON path is detected "+jsonKeyFilePath);
            conf.set("google.cloud.auth.service.account.json.keyfile", jsonKeyFilePath);
        }else{
            logger.info("Not able to find the json key at "+jsonKeyFilePath+". Expecting workload identity to be set");
        }
        return conf;
    }

    public static Configuration getMinioFileSystemInstance(String filePath, String catalogName) throws Exception {
        String upperCaseCatalogName = catalogName.toUpperCase().replace("-", "_");
        String homeDir = System.getenv(upperCaseCatalogName + ReplicationConstant.ENV_MINIO_HOME_PREFIX);

        File file = new File(homeDir + "/core-site.xml");
        if (!file.exists()) throw new RuntimeException("Not able to find the file, "+file.getPath());

        Configuration conf = new Configuration(false);
        conf.setQuietMode(false);
        conf.set("fs.s3a.impl", org.apache.hadoop.fs.s3a.S3AFileSystem.class.getName());
        conf.set("fs.defaultFS", filePath);
        conf.addResource(new Path(file.toURI()));

        return conf;
    }
}
