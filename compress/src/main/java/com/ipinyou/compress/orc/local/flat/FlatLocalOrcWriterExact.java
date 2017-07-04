package com.ipinyou.compress.orc.local.flat;

import com.ipinyou.compress.orc.OrcWriterOptions;
import com.ipinyou.compress.util.Delimiter;
import com.ipinyou.compress.util.Indexs;
import org.apache.commons.lang.StringUtils;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by lanceolata on 16-12-20.
 */
public class FlatLocalOrcWriterExact extends FlatLocalOrcWriter {
    private static final Logger logger = LoggerFactory.getLogger(FlatLocalOrcWriterExact.class);

    public FlatLocalOrcWriterExact(String filePath, TypeDescription schema,
                                   OrcWriterOptions options, TypeDescription rawSchema,
                                   Indexs indexsRoot, Delimiter delimiterRoot) throws IOException {
        super(filePath, schema, options, rawSchema, indexsRoot, delimiterRoot);
    }

    protected boolean checkNull(String col, String fieldType) {
        return false;
    }

    protected String[] splitByDelimiter(String col, String separator, int expectLength) {
        String[] cols;
        if ("".equals(col)) {
            cols = col.split(separator);
        } else {
            cols = StringUtils.splitByWholeSeparatorPreserveAllTokens(col, separator);
        }

        if (cols.length != expectLength) {
            logger.warn("column length[{}] not match expect length[{}]", cols.length, expectLength);
            return null;
        }
        return cols;
    }
}
