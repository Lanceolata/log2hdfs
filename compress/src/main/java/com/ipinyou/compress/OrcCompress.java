package com.ipinyou.compress;

import com.ipinyou.compress.compress.FileCompress;
import com.ipinyou.compress.compress.FileProperty;
import com.ipinyou.compress.orc.OrcWriterOptions;
import org.apache.commons.cli.*;
import org.ini4j.Config;
import org.ini4j.Ini;
import org.ini4j.Profile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;


/**
 * Created by lance on 2017/7/4.
 */
public class OrcCompress {
    private static final Logger logger = LoggerFactory.getLogger(OrcWriterOptions.class);

    public static void main(String[] args) {
        Options opt = new Options();
        opt.addOption("c", "config", true, "config file path");
        opt.addOption("t", "topic", true, "topic name");

        CommandLineParser parser = new DefaultParser();
        CommandLine cl = null;

        try {
            cl = parser.parse(opt, args);
        } catch (ParseException e) {
            logger.error(e.getMessage());
            System.exit(2);
        }

        if (!cl.hasOption("c") || !cl.hasOption("t")) {
            logger.error("Invalid arguments");
            System.exit(2);
        }

        String confPath = cl.getOptionValue("c");
        String topicName = cl.getOptionValue("t");

        Config cfg = new Config();
        Ini ini = new Ini();
        ini.setConfig(cfg);
        try {
            ini.load(new File(confPath));
        } catch (IOException e) {
            logger.error(e.getMessage());
            System.exit(2);
        }

        if (ini.containsKey("global")) {
            Profile.Section global = ini.get("global");
            OrcWriterOptions.setDefaultOptions(global);
        }

        Profile.Section topic = ini.get(topicName);
        if(topic == null) {
            logger.error("unknown topic[{}]", topicName);
            System.exit(2);
        }

        String[] filePaths = cl.getArgs();
        if (filePaths.length != 1) {
            logger.error("invalid command line arguments[{}]", Arrays.toString(args));
            System.exit(2);
        }
        String filePath = filePaths[0];
        FileProperty fileProperty = new FileProperty(topic);
        FileCompress sfc = new FileCompress(filePath, filePath + ".orc", fileProperty);
        if (!sfc.compress()) {
            logger.error("file convert failed");
            System.exit(2);
        }
        System.exit(0);
    }
}
