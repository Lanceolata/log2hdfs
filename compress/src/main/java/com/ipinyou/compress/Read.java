package com.ipinyou.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;

/**
 * Created by lance on 2017/7/7.
 */
public class Read {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        long sum = 0;
        for (int i = 0; i < args.length; ++i) {
            Path path = new Path(args[i]);
            try {
                Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
                sum += reader.getNumberOfRows();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println(sum);
    }
}
