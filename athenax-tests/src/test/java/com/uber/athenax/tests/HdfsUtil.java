package com.uber.athenax.tests;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtil extends HdfsWrapper {

    private static HdfsUtil hdfsUtil = new HdfsUtil();

    public static HdfsUtil getInstance() {
        return hdfsUtil;
    }

    public boolean clean(String filePath) throws Exception {
        FileSystem fs = FileSystem.get(loadConf());
        Path hdfsPath = new Path(filePath);
        if (fs.exists(hdfsPath)) {
            boolean isDeleted = fs.delete(hdfsPath, true);
            if (isDeleted) {
                return fs.mkdirs(hdfsPath);
            }
        }
        return false;
    }

    public void upload(String filePath, String targetPath) throws Exception {
        FileSystem fs = FileSystem.get(loadConf());
        fs.copyFromLocalFile(false, false, new Path(filePath), new Path(targetPath));
    }

    public void upload(String[] filePaths, String targetPath) throws Exception {
        Path[] srcPaths = new Path[filePaths.length];
        for (int i = 0; i < filePaths.length; i++) {
            srcPaths[i] = new Path(filePaths[i]);
        }
        FileSystem fs = FileSystem.get(loadConf());
        fs.copyFromLocalFile(false, false, srcPaths, new Path(targetPath));
    }

    public boolean exist(String targetPath) throws Exception {
        FileSystem fs = FileSystem.get(loadConf());
        return fs.exists(new Path(targetPath));
    }

    public static void main(String[] args) throws Exception {
        try {
            boolean exist = getInstance().exist("/tmp/athenax/yarn-site.xml");
            System.out.println(exist);
        } catch (Exception e) {
            e.printStackTrace();
        }
//        System.setProperty("HADOOP_CONF_DIR", "/Users/joey/tmp/hadoop");
//        Path path = new Path("/tmp/athenax/libs/activation-1.1.jar");
//        FileSystem fs = FileSystem.get(loadConf());
//        System.out.println(fs.getFileStatus(path).getPath().toUri());
//        System.out.println(fs.getUri());
//        System.out.println(path.);
//        try {
//            boolean ret =  getInstance().clean("/tmp/wyk_test");
//            System.out.println(ret);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
