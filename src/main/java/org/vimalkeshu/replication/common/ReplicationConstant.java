package org.vimalkeshu.replication.common;

public class ReplicationConstant {
    // env prefix
    public static final String ENV_GCS_SECRET_PREFIX = "_SECRET";
    public static final String ENV_MINIO_HOME_PREFIX = "_HOME";
    public static final String ENV_HADOOP_HOME_PREFIX = "_HADOOP_HOME";
    public static final String ENV_HADOOP_USER_PREFIX = "_HADOOP_USER";
    public static final String ENV_HADOOP_KERBEROS_PRINCIPAL_PREFIX = "_HADOOP_KERBEROS_PRINCIPAL";
    public static final String ENV_HADOOP_KERBEROS_KEYTAB_PATH_PREFIX = "_HADOOP_KERBEROS_KEYTAB_PATH";
    public static final String HDFS_FILE_SCHEME = "hdfs://";
    public static final String GCS_FILE_SCHEME = "gs://";
    public static final String S3A_FILE_SCHEME = "s3a://";
    public  static final String LOCAL_FILE_SCHEME = "local://";
    public static final String FILE_SCHEME = "file://";
    public static final int HDFS_BLOCK_SIZE = 16384; // KB
    public static final int RETRY_DEFAULT = 3;
}
