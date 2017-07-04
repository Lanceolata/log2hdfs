package com.ipinyou.compress.orc.local;


import com.ipinyou.compress.orc.OrcWriter;
import com.ipinyou.compress.orc.OrcWriterOptions;
import com.ipinyou.compress.util.Delimiter;
import com.ipinyou.compress.util.Indexs;
import org.apache.commons.lang.IncompleteArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

public abstract class LocalOrcWriter implements OrcWriter {
    private static final Logger logger = LoggerFactory.getLogger(LocalOrcWriter.class);

    protected String filePath;
    protected Writer writer;
    protected TypeDescription schema;
    protected VectorizedRowBatch batch;

    public LocalOrcWriter(String filePath, TypeDescription schema,
                          OrcWriterOptions options) throws IOException {
        if (filePath == null || "".equals(filePath) || schema == null || options == null) {
            throw new IncompleteArgumentException("Invallid LocalOrcWriter argument");
        }

        Configuration conf = new Configuration();
        OrcFile.WriterOptions wo = OrcFile.writerOptions(conf);
        wo.setSchema(schema);
        wo.compress(options.getCompressionKind());
        wo.bufferSize(options.getBufferSize());
        wo.stripeSize(options.getStripeSize());
        wo.rowIndexStride(options.getRowIndexStride());
        wo.fileSystem(FileSystem.getLocal(conf));
        this.schema = schema;
        this.writer = OrcFile.createWriter(new Path(filePath), wo);
        this.batch = schema.createRowBatch();
        this.filePath = filePath;
    }

    public boolean addBatch() {
        if (batch.size == batch.getMaxSize()) {
            try {
                writer.addRowBatch(batch);
                batch.reset();
            } catch (IOException e) {
                logger.error("addRowBatch to file[{}] failed with error[{}]", filePath, e.getMessage());
                return false;
            }
        }
        return true;
    }

    public boolean close(){
        try {
            this.writer.addRowBatch(batch);
            this.batch.reset();
            this.writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public abstract boolean addRow(String line);
    protected abstract boolean checkNull(String col, String fieldType);
    protected abstract String[] splitByDelimiter(String col, String deli, int expectLength);

    protected boolean addRowWithNull(int row, ColumnVector cv) {
        if(cv.noNulls)
            cv.noNulls = false;

        cv.isNull[row] = true;
        return true;
    }

    // boolean, bigint, date, int, smallint, and tinyint
    // boolean is 0 or 1
    protected boolean addRowWithLong(int row, String col, LongColumnVector lcv) {
        try {
            lcv.vector[row] = Long.parseLong(col);
        } catch(Exception e) {
            logger.warn("parse Long fail[{}]", col);
            return false;
        }
        return true;
    }

    protected boolean addRowWithTimestamp(int row, String col, TimestampColumnVector tcv) {
        try {
            tcv.set(row, Timestamp.valueOf(col));
        } catch(Exception e) {
            logger.warn("parse Timestamp fail[{}]", col);
            return false;
        }
        return true;
    }

    protected boolean addRowWithDouble(int row, String col, DoubleColumnVector dcv) {
        try {
            dcv.vector[row] = Double.parseDouble(col);
        } catch(Exception e) {
            logger.warn("parse Double fail[{}]", col);
            return false;
        }
        return true;
    }

    protected boolean addRowWithDecimal(int row, String col, DecimalColumnVector dcv) {
        try {
//            dcv.set(row, new HiveDecimalWritable(col));
            dcv.set(row, HiveDecimal.create(col));
        } catch(Exception e) {
            logger.warn("parse HiveDecimalWritable fail[{}]", col);
            return false;
        }
        return true;
    }

    // binary, char, string, and varchar
    protected boolean addRowWithBytes(int row, String col, BytesColumnVector bcv) {
        try {
            bcv.setVal(row, col.getBytes());
        } catch(Exception e) {
            logger.warn("parse Bytes fail[{}]", col);
            return false;
        }
        return true;
    }

    protected boolean addRowWithStruct(int row, String[] cols, Delimiter delimiter,
                                       List<TypeDescription> children, Indexs indexs,
                                       ColumnVector[] cvs) {

        if (cols == null || delimiter == null || indexs == null
                || cols.length != indexs.getRawLength()) {
            logger.error("addRowWithStruct invalid arguments");
            return false;
        }

        if (children == null || cvs == null ||
                children.size() != cvs.length) {
            logger.error("addRowWithStruct invalid arguments");
            return false;
        }

        String fieldType;
        Boolean res = true;
        int schemaIndex = 0;
        for(TypeDescription child : children) {
            fieldType = child.getCategory().getName();
            int index = indexs.getIndex(schemaIndex);
            if ("boolean".equalsIgnoreCase(fieldType) || "bigint".equalsIgnoreCase(fieldType) ||
                    "date".equalsIgnoreCase(fieldType) || "int".equalsIgnoreCase(fieldType) ||
                    "smallint".equalsIgnoreCase(fieldType) || "tinyint".equalsIgnoreCase(fieldType)) {
                if (checkNull(cols[index], fieldType)) {
                    res = addRowWithNull(row, cvs[schemaIndex]);
                } else {
                    res = addRowWithLong(row, cols[index], (LongColumnVector) cvs[schemaIndex]);
                }
            } else if ("timestamp".equalsIgnoreCase(fieldType)) {
                if (checkNull(cols[index], fieldType)) {
                    res = addRowWithNull(row, cvs[schemaIndex]);
                } else {
                    res = addRowWithTimestamp(row, cols[index], (TimestampColumnVector) cvs[schemaIndex]);
                }
            } else if ("double".equalsIgnoreCase(fieldType) || "float".equalsIgnoreCase(fieldType)) {
                if (checkNull(cols[index], fieldType)) {
                    res = addRowWithNull(row, cvs[schemaIndex]);
                } else {
                    res = addRowWithDouble(row, cols[index], (DoubleColumnVector) cvs[schemaIndex]);
                }
            } else if ("decimal".equalsIgnoreCase(fieldType)) {
                if (checkNull(cols[index], fieldType)) {
                    res = addRowWithNull(row, cvs[schemaIndex]);
                } else {
                    res = addRowWithDecimal(row, cols[index], (DecimalColumnVector) cvs[schemaIndex]);
                }
            } else if ("binary".equalsIgnoreCase(fieldType) || "char".equalsIgnoreCase(fieldType) ||
                    "string".equalsIgnoreCase(fieldType) || "varchar".equalsIgnoreCase(fieldType)) {
                if (checkNull(cols[index], fieldType)) {
                    res = addRowWithNull(row, cvs[schemaIndex]);
                } else {
                    res = addRowWithBytes(row, cols[index], (BytesColumnVector) cvs[schemaIndex]);
                }
            } else if ("struct".equalsIgnoreCase(fieldType)) {
                Delimiter subDelimiter = delimiter.getChild(index);
                if (subDelimiter == null) {
                    logger.error("invalid delimiter");
                    return false;
                }

                Indexs subIndexs = indexs.getChild(schemaIndex);
                if (subIndexs == null) {
                    logger.error("invalid indexs index[{}]", schemaIndex);
                    return false;
                }

                String[] subCols = splitByDelimiter(cols[index], subDelimiter.getDelimiter(),
                        subIndexs.getRawLength());
                if(subCols == null) {
                    logger.warn("splitByDelimiter failed cols[{}] delimiter[{}] indexs[{}]",
                                cols[index], subDelimiter.toString(), subIndexs.toString());
                    return false;
                }

                res = addRowWithStruct(row, subCols, subDelimiter,
                                       child.getChildren(), subIndexs,
                                       ((StructColumnVector) cvs[schemaIndex]).fields);
            } else {
                throw new IllegalArgumentException("unknown field type[" + fieldType + "]");
            }
            if(!res) {
                return false;
            }
            ++schemaIndex;
        }
        return true;
    }

    protected boolean addRowWithUnion(int row, String col, TypeDescription td, UnionColumnVector ucv) {
        throw new IncompleteArgumentException("unsupport type");
    }

    protected boolean addRowWithList(int row, String col, TypeDescription td, ListColumnVector lcv) {
        throw new IncompleteArgumentException("unsupport type");
    }

    protected boolean addRowWithMap(int row, String col, TypeDescription td, MapColumnVector mcv) {
        throw new IncompleteArgumentException("unsupport type");
    }
}
