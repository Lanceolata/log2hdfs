package com.ipinyou.appendcvt.util;

import java.io.File;

/**
 * Created by lance on 2017/7/5.
 */
public class FileUtils {

    public static boolean isFile(String path) {
        File file = new File(path);
        return file.isFile();
    }

    public static boolean deleteFileIfExists(String path) {
        boolean res = true;
        File file = new File(path);
        if (file.isFile() && file.exists()) {
            res = file.delete();
        }
        return res;
    }

    public static String getFileName(String path) {
        File file = new File(path);
        return file.getName();
    }

    public static String getDir(String path) {
        File file = new File(path);
        return file.getParent();
    }

    public static boolean createDir(String path) {
        File dir = new File(path);
        if(dir.exists()) {
            return true;
        }

        if(!dir.mkdirs()) {
            return false;
        }
        return true;
    }

    public static String renameFileWithTimestamp(String srcPath, String destPath) {
        File newfile = new File(destPath);
        while(newfile.exists()) {
            destPath += "." + System.currentTimeMillis();
            newfile = new File(destPath);
        }

        String res = null;
        File oldfile = new File(srcPath);
        if (oldfile.exists() && !newfile.exists()) {
            if(oldfile.renameTo(newfile)) {
                res = destPath;
            }
        }
        return res;
    }

    public static String getAbsPath(String path) {
        if(path == null || "".equals(path))
            return null;

        File f = new File(path);
        return f.getAbsolutePath();
    }
}
