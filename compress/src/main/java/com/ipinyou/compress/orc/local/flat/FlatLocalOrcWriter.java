package com.ipinyou.compress.orc.local.flat;

import com.ipinyou.compress.orc.OrcWriterOptions;
import com.ipinyou.compress.orc.local.LocalOrcWriter;
import com.ipinyou.compress.util.Delimiter;
import com.ipinyou.compress.util.Indexs;
import org.apache.commons.lang.IncompleteArgumentException;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by lanceolata on 17-3-8.
 */
public abstract class FlatLocalOrcWriter extends LocalOrcWriter {
    private static final Logger logger = LoggerFactory.getLogger(FlatLocalOrcWriter.class);

    protected Indexs indexsRoot;
    protected Delimiter delimiterRoot;
    protected TypeDescription rawSchema;

    public FlatLocalOrcWriter(String filePath, TypeDescription schema,
                              OrcWriterOptions options, TypeDescription rawSchema,
                              Indexs indexsRoot, Delimiter delimiterRoot) throws IOException {
        super(filePath, schema, options);

        if (delimiterRoot == null || rawSchema == null || indexsRoot == null) {
            throw new IncompleteArgumentException("Invallid FlatLocalOrcWriter argument");
        }

        this.rawSchema = rawSchema;
        this.indexsRoot = indexsRoot;
        this.delimiterRoot = delimiterRoot;
    }

    public boolean addRow(String line) {
        if(line == null || "".equals(line)) {
            logger.warn("empty line");
            return false;
        }

        String cols[] = new String[indexsRoot.getRawLength()];
        if (nestedToFlat(line, delimiterRoot, 0, rawSchema.getChildren(), cols)
                != indexsRoot.getRawLength()) {
            logger.error("nestedToFlat failed file[{}] line[{}]", filePath, line);
            return false;
        }

        int row = batch.size++;
        if(!addRowWithStruct(row, cols, delimiterRoot, schema.getChildren(), indexsRoot, batch.cols)) {
            logger.warn("add row failed file[{}] line[{}]", filePath, line);
            batch.size--;
            return false;
        }

        return addBatch();
    }

    protected abstract boolean checkNull(String col, String fieldType);
    protected abstract String[] splitByDelimiter(String col, String deli, int expectLength);

    protected int nestedToFlat(String line, Delimiter delimiter, int num,
                               List<TypeDescription> children, String res[]) {
        String deli = delimiter.getDelimiter();
        if("".equals(deli) || deli == null) {
            logger.error("delimiter is empty");
            return -1;
        }

        String[] cols = splitByDelimiter(line, deli, children.size());
        if(cols == null) {
            logger.error("splitBySeparator failed");
            return -1;
        }

        String fieldType;
        int i = 0;
        for (TypeDescription child : children) {
            fieldType = child.getCategory().toString();
            if ("struct".equalsIgnoreCase(fieldType)) {
                num = nestedToFlat(cols[i], delimiter.getChild(i), num, child.getChildren(), res);
                if (num < 0) {
                    return num;
                }
            } else {
                res[num++] = cols[i];
            }
            ++i;
        }
        return num;
    }
}
