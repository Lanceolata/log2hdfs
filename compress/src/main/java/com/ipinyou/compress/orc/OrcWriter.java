package com.ipinyou.compress.orc;

/**
 * Created by lanceolata on 17-1-3.
 */
public interface OrcWriter {
    boolean addRow(String line);
    boolean close();
}
