package com.ipinyou.compress.orc.local.nested;

import com.ipinyou.compress.orc.OrcWriterOptions;
import com.ipinyou.compress.util.Delimiter;
import com.ipinyou.compress.util.Indexs;
import org.apache.commons.lang.StringUtils;
import org.apache.orc.TypeDescription;

import java.io.IOException;

/**
 * Created by lanceolata on 17-3-14.
 */
public class NestedLocalOrcWriterCompat extends NestedLocalOrcWriter {

    public NestedLocalOrcWriterCompat(String filePath, TypeDescription schema,
                                      OrcWriterOptions options, TypeDescription rawSchema,
                                      Indexs indexsRoot, Delimiter delimiterRoot) throws IOException {
        super(filePath, schema, options, rawSchema, indexsRoot, delimiterRoot);
    }

    protected boolean checkNull(String col, String fieldType) {
        if (col == null)
            return true;
        return false;
    }

    protected String[] splitByDelimiter(String col, String separator, int expectLength) {
        String[] cols = StringUtils.splitByWholeSeparatorPreserveAllTokens(col, separator);
        if (cols.length < expectLength) {
            String[] res = new String[expectLength];
            int i = 0;
            for (; i < cols.length; i++) {
                res[i] = cols[i];
            }
            for (; i < expectLength; i++) {
                res[i] = null;
            }
            return res;
        }
        return cols;
    }
}
