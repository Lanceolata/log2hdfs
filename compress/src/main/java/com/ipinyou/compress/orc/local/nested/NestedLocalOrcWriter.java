package com.ipinyou.compress.orc.local.nested;

import com.ipinyou.compress.orc.OrcWriterOptions;
import com.ipinyou.compress.orc.local.LocalOrcWriter;
import com.ipinyou.compress.util.Delimiter;
import com.ipinyou.compress.util.Indexs;
import org.apache.commons.lang.IncompleteArgumentException;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class NestedLocalOrcWriter extends LocalOrcWriter {
    private static final Logger logger = LoggerFactory.getLogger(NestedLocalOrcWriter.class);

    protected Delimiter delimiterRoot;
    protected Indexs indexsRoot;
    protected TypeDescription rawSchema;

    public NestedLocalOrcWriter(String filePath, TypeDescription schema,
                                OrcWriterOptions options, TypeDescription rawSchema,
                                Indexs indexsRoot, Delimiter delimiterRoot) throws IOException {
        super(filePath, schema, options);

        if(delimiterRoot == null || rawSchema == null || indexsRoot == null) {
            throw new IncompleteArgumentException("Invallid NestedLocalOrcWriter argument");
        }

        this.delimiterRoot = delimiterRoot;
        this.indexsRoot = indexsRoot;
        this.rawSchema = rawSchema;
    }

    public boolean addRow(String line){
        if(line == null || "".equals(line)) {
            logger.warn("empty line");
            return false;
        }

        String delimiter = delimiterRoot.getDelimiter();
        if(delimiter == null || "".equals(delimiter)) {
            logger.error("delimiter is empty");
            return false;
        }

        String[] cols = splitByDelimiter(line, delimiter, indexsRoot.getRawLength());
        if(cols == null) {
            logger.warn("splitByDelimiter failed");
            return false;
        }

        int row = batch.size++;
        if(!addRowWithStruct(row, cols, delimiterRoot,
                schema.getChildren(), indexsRoot, batch.cols)) {
            logger.warn("add row failed file[{}] line[{}]", filePath, line);
            batch.size--;
            return false;
        }

        return addBatch();
    }

    protected abstract boolean checkNull(String col, String fieldType);
    protected abstract String[] splitByDelimiter(String col, String separator, int expectLength);
}
