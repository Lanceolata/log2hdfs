package com.ipinyou.appendcvt;

import com.ipinyou.appendcvt.cvt.CvtConvertor;
import com.ipinyou.appendcvt.util.FileUtils;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.ipinyou.appendcvt.util.FileUtils.createDir;

/**
 * Created by lance on 2017/7/6.
 */
public class Convert {
    private static final Logger logger = LoggerFactory.getLogger(Convert.class);

    public static void main(String[] args) {
        Options opt = new Options();

        CommandLineParser parser = new DefaultParser();
        CommandLine cl = null;

        try {
            cl = parser.parse(opt, args);
        } catch (ParseException e) {
            logger.error(e.getMessage());
            System.exit(2);
        }

        String[] filePaths = cl.getArgs();
        if (filePaths.length != 1) {
            logger.error("invalid command line arguments[{}]", Arrays.toString(args));
            System.exit(2);
        }

        String filePath = filePaths[0];
        CvtConvertor convert = new CvtConvertor(filePath, filePath + ".append");
        if (!convert.convert()) {
            logger.error("file convert failed");
            System.exit(2);
        }
        System.exit(0);
    }
}
