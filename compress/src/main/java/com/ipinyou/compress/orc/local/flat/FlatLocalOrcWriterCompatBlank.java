package com.ipinyou.compress.orc.local.flat;

import com.ipinyou.compress.orc.OrcWriterOptions;
import com.ipinyou.compress.util.Delimiter;
import com.ipinyou.compress.util.Indexs;
import org.apache.orc.TypeDescription;

import java.io.IOException;

/**
 * Created by lanceolata on 17-1-4.
 */
public class FlatLocalOrcWriterCompatBlank extends FlatLocalOrcWriterCompat {

    public FlatLocalOrcWriterCompatBlank(String filePath, TypeDescription schema,
                                         OrcWriterOptions options, TypeDescription rawSchema,
                                         Indexs indexsRoot, Delimiter delimiterRoot) throws IOException {
        super(filePath, schema, options, rawSchema, indexsRoot, delimiterRoot);
    }

    protected boolean checkNull(String col, String fieldType) {
        if(col == null || "".equals(col)) {
            return true;
        }
        return false;
    }
}
