package com.ipinyou.compress.compress;

import com.ipinyou.compress.orc.OrcWriter;
import com.ipinyou.compress.orc.local.flat.FlatLocalOrcWriterFactory;
import com.ipinyou.compress.orc.local.nested.NestedLocalOrcWriterFactory;
import com.ipinyou.compress.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by lance on 2017/7/5.
 */
public class FileCompress {
    private static final Logger logger = LoggerFactory.getLogger(FileCompress.class);

    private String inPath;
    private String outPath;
    private FileProperty fileProperty;

    public FileCompress(String inPath, String outPath, FileProperty fileProperty) {
        if (inPath == null || outPath == null || fileProperty == null) {
            throw new IllegalArgumentException("FileCompress invalid arguments");
        }

        this.inPath = FileUtils.getAbsPath(inPath);
        this.outPath = FileUtils.getAbsPath(outPath);
        this.fileProperty = fileProperty;
    }

    public boolean compress() {
        long startTime = System.currentTimeMillis();

        if (!FileUtils.isFile(inPath)) {
            logger.error("Invalid inpath[{}] not file", inPath);
            return false;
        }

        if (!FileUtils.deleteFileIfExists(outPath)) {
            logger.error("Delete exists file[{}] failed", outPath);
            return false;
        }

        String dirPath = FileUtils.getDir(outPath);
        String fileName = FileUtils.getFileName(outPath);
        String crcPath = dirPath + "/." + fileName + ".crc";

        try {
            OrcWriter writer = null;
            if (NestedLocalOrcWriterFactory.checkType(fileProperty.getType())) {
                NestedLocalOrcWriterFactory.NestedLocalOrcWriterType type =
                        NestedLocalOrcWriterFactory.getType(fileProperty.getType());
                if (type == null) {
                    logger.error("Invalid write type[{}]", fileProperty.getType());
                    return false;
                }
                writer = NestedLocalOrcWriterFactory.create(
                        type, outPath, fileProperty.getSchema(),
                        fileProperty.getOrcWriterOptions(), fileProperty.getRawSchema(),
                        fileProperty.getIndexs(), fileProperty.getDelimiters());
            } else {
                FlatLocalOrcWriterFactory.FlatLocalOrcWriterType type =
                        FlatLocalOrcWriterFactory.getType(fileProperty.getType());
                if (type == null) {
                    logger.error("Invalid write type[{}]", fileProperty.getType());
                    return false;
                }
                writer = FlatLocalOrcWriterFactory.create(
                        type, outPath, fileProperty.getSchema(),
                        fileProperty.getOrcWriterOptions(), fileProperty.getRawSchema(),
                        fileProperty.getIndexs(), fileProperty.getDelimiters());
            }

            BufferedReader br = new BufferedReader(new FileReader(inPath));
            String line = null;
            while ((line = br.readLine()) != null) {
                writer.addRow(line);
            }
            br.close();

            writer.close();
            if(!FileUtils.deleteFileIfExists(crcPath)) {
                logger.error("Delete exists file[{}] failed", outPath);
            }
            logger.info("Completed compress from[{}] to[{}] in {} millisecond", inPath,
                        outPath, System.currentTimeMillis() - startTime);

            if (fileProperty.getBackupDir() != null) {
                String backPath = fileProperty.getBackupDir() + "/" + FileUtils.getFileName(inPath);
                backPath = FileUtils.renameFileWithTimestamp(inPath, backPath);
                if (backPath == null) {
                    logger.error("renameFileWithTimestamp from[{}] to[{}] failed", inPath, fileProperty.getBackupDir());
                } else {
                    logger.info("renameFileWithTimestamp from[{}] to[{}] success", inPath, backPath);
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        } catch (IllegalArgumentException e) {
            logger.error(e.getMessage());
            return false;
        }

        return true;
    }

    public static void main(String[] args) {
        String inPath = "fsdfafafafa";
        String dirPath = FileUtils.getDir(inPath);
        String fileName = FileUtils.getFileName(inPath);
        String crcPath = dirPath + "/." + fileName + ".crc";
        System.out.println(dirPath);
        System.out.println(fileName);
        System.out.println(crcPath);
    }
}