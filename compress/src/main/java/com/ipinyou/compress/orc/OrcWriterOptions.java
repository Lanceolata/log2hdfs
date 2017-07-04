package com.ipinyou.compress.orc;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.orc.CompressionKind;
import org.ini4j.Profile.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lanceolata on 10/24/16.
 */
public class OrcWriterOptions {
    private static final Logger logger = LoggerFactory.getLogger(OrcWriterOptions.class);

    private static CompressionKind COMPRESSION_KIND_DEFAULT = CompressionKind.NONE;
    private static long STRIP_SIZE_DEFAULT = 256L * 1024 * 1024;
    private static int BUFFER_SIZE_DEFAULT = 256 * 1024;
    private static int ROW_INDEX_STRIDE_DEFAULT = 10000;

    private CompressionKind compressionKind = COMPRESSION_KIND_DEFAULT;
    private long stripeSize = STRIP_SIZE_DEFAULT;
    private int bufferSize = BUFFER_SIZE_DEFAULT;
    private int rowIndexStride = ROW_INDEX_STRIDE_DEFAULT;

    public static void setDefaultOptions(Section section) {
        setDefaultOptions(section.get("compression"), section.get("stripsize"),
                          section.get("buffersize"), section.get("rowindex"));
    }

    public static void setDefaultOptions(String compresskind, String stripeSize, String bufferSize,
                                         String rowIndexStride) {
        if(compresskind != null && !"".equals(compresskind)) {
            if("zlib".equalsIgnoreCase(compresskind)) {
                COMPRESSION_KIND_DEFAULT = CompressionKind.ZLIB;
            } else if("lzo".equalsIgnoreCase(compresskind)) {
                COMPRESSION_KIND_DEFAULT = CompressionKind.LZO;
            } else if("lz4".equalsIgnoreCase(compresskind)) {
                COMPRESSION_KIND_DEFAULT = CompressionKind.LZ4;
            } else if("snappy".equalsIgnoreCase(compresskind)) {
                COMPRESSION_KIND_DEFAULT = CompressionKind.SNAPPY;
            } else if("uncompress".equalsIgnoreCase(compresskind)) {
                COMPRESSION_KIND_DEFAULT = CompressionKind.NONE;
            } else {
                COMPRESSION_KIND_DEFAULT = CompressionKind.NONE;
                logger.warn("unknown compress kind:{}", compresskind);
            }
        }

        if(stripeSize != null && !"".equals(stripeSize)) {
            STRIP_SIZE_DEFAULT = NumberUtils.toLong(stripeSize, STRIP_SIZE_DEFAULT);
        }

        if(bufferSize != null && !"".equals(bufferSize)) {
            BUFFER_SIZE_DEFAULT = NumberUtils.toInt(bufferSize, BUFFER_SIZE_DEFAULT);
        }

        if(rowIndexStride != null && !"".equals(rowIndexStride)) {
            ROW_INDEX_STRIDE_DEFAULT = NumberUtils.toInt(rowIndexStride, ROW_INDEX_STRIDE_DEFAULT);
        }
    }

    public OrcWriterOptions() {}

    public OrcWriterOptions(Section section) {
        setOptions(section.get("compression"), section.get("stripsize"),
                   section.get("buffersize"), section.get("rowindex"));
    }

    public OrcWriterOptions(String compressionKind, String stripeSize, String bufferSize,
                            String rowIndexStride) {
        setOptions(compressionKind, stripeSize, bufferSize, rowIndexStride);
    }

    public void setOptions(Section section) {
        setOptions(section.get("compression"), section.get("stripsize"),
                   section.get("buffersize"), section.get("rowindex"));
    }

    public void setOptions(String compresskind, String stripeSize, String bufferSize, String rowIndexStride) {
        if(compresskind != null && !"".equals(compresskind)) {
            if("zlib".equalsIgnoreCase(compresskind)) {
                this.compressionKind = CompressionKind.ZLIB;
            } else if("lzo".equalsIgnoreCase(compresskind)) {
                this.compressionKind = CompressionKind.LZO;
            } else if("lz4".equalsIgnoreCase(compresskind)) {
                this.compressionKind = CompressionKind.LZ4;
            } else if("snappy".equalsIgnoreCase(compresskind)) {
                this.compressionKind = CompressionKind.SNAPPY;
            } else if ("uncompress".equalsIgnoreCase(compresskind)) {
                this.compressionKind = CompressionKind.NONE;
            } else {
                this.compressionKind = CompressionKind.NONE;
                logger.warn("unknown compress kind:{}", compresskind);
            }
        }

        if(stripeSize != null && !"".equals(stripeSize)) {
            this.stripeSize = NumberUtils.toLong(stripeSize, this.stripeSize);
        }

        if(bufferSize != null && !"".equals(bufferSize)) {
            this.bufferSize = NumberUtils.toInt(bufferSize, this.bufferSize);
        }

        if(rowIndexStride != null && !"".equals(rowIndexStride)) {
            this.rowIndexStride = NumberUtils.toInt(rowIndexStride, this.rowIndexStride);
        }

        logger.info("Writer Options compression[{}] strip_size[{}] buffer_size[{}] row_index[{}]",
                    this.compressionKind, this.stripeSize, this.bufferSize, this.rowIndexStride);
    }

    public CompressionKind getCompressionKind() {
        return compressionKind;
    }

    public void setCompressionKind(CompressionKind compressionKind) {
        this.compressionKind = compressionKind;
    }

    public long getStripeSize() {
        return stripeSize;
    }

    public void setStripeSize(long stripeSize) {
        this.stripeSize = stripeSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getRowIndexStride() {
        return rowIndexStride;
    }

    public void setRowIndexStride(int rowIndexStride) {
        this.rowIndexStride = rowIndexStride;
    }
}
