package com.ipinyou.compress.orc.local.nested;

import com.ipinyou.compress.orc.OrcWriter;
import com.ipinyou.compress.orc.OrcWriterOptions;
import com.ipinyou.compress.util.Delimiter;
import com.ipinyou.compress.util.Indexs;
import org.apache.orc.TypeDescription;

import java.io.IOException;

/**
 * Created by lanceolata on 16-12-21.
 */
public class NestedLocalOrcWriterFactory {

    public static OrcWriter create(String type, String filePath, TypeDescription schema,
                                   OrcWriterOptions options, TypeDescription rawSchema,
                                   Indexs indexsRoot, Delimiter delimiterRoot)
            throws IOException, IllegalArgumentException {
        if ("nested.exact".equalsIgnoreCase(type)) {
            return new NestedLocalOrcWriterExact(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else if ("nested.exactnull".equalsIgnoreCase(type)) {
            return new NestedLocalOrcWriterExactNull(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else if ("nested.exactnullisnull".equalsIgnoreCase(type)) {
            return new NestedLocalOrcWriterExactNullIsNull(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else if ("nested.exactisnull".equalsIgnoreCase(type)) {
            return new NestedLocalOrcWriterExactIsNull(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else if ("nested.compat".equalsIgnoreCase(type)) {
            return new NestedLocalOrcWriterCompat(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else if ("nested.compatnull".equalsIgnoreCase(type)) {
            return new NestedLocalOrcWriterCompatNull(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else if ("nested.compatnullisnull".equalsIgnoreCase(type)) {
            return new NestedLocalOrcWriterCompatNullIsNull(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } if ("nested.compatisnull".equalsIgnoreCase(type)) {
            return new NestedLocalOrcWriterCompatIsNull(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else {
            throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    protected static String[] types = {"nested.exact", "nested.exactnull", "nested.exactnullisnull", "nested.exactisnull",
                                       "nested.compat", "nested.compatnull", "nested.compatnullisnull", "nested.compatisnull"};

    public static boolean checkType(String type) {
        for(String t : types) {
            if(t.equalsIgnoreCase(type)) {
                return true;
            }
        }
        return false;
    }
}
