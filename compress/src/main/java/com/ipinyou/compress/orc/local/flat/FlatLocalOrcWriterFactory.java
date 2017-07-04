package com.ipinyou.compress.orc.local.flat;

import com.ipinyou.compress.orc.OrcWriter;
import com.ipinyou.compress.orc.OrcWriterOptions;
import com.ipinyou.compress.util.Delimiter;
import com.ipinyou.compress.util.Indexs;
import org.apache.orc.TypeDescription;

import java.io.IOException;

/**
 * Created by lanceolata on 17-3-8.
 */
public class FlatLocalOrcWriterFactory {
    public static OrcWriter create(String type, String filePath, TypeDescription schema,
                                   OrcWriterOptions options, TypeDescription rawSchema,
                                   Indexs indexsRoot, Delimiter delimiterRoot)
            throws IOException, IllegalArgumentException {
        if ("flat.exact".equalsIgnoreCase(type)) {
            return new FlatLocalOrcWriterExact(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else if ("flat.exactnull".equalsIgnoreCase(type)) {
            return new FlatLocalOrcWriterExactNull(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else if ("flat.exactnullisnull".equalsIgnoreCase(type)) {
            return new FlatLocalOrcWriterExactNullIsNull(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else if ("flat.exactisnull".equalsIgnoreCase(type)) {
            return new FlatLocalOrcWriterExactIsNull(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else if ("flat.compat".equalsIgnoreCase(type)) {
            return new FlatLocalOrcWriterCompat(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else if ("flat.compatnull".equalsIgnoreCase(type)) {
            return new FlatLocalOrcWriterCompatNull(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else if ("flat.compatnullisnull".equalsIgnoreCase(type)) {
            return new FlatLocalOrcWriterCompatNullIsNull(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else if ("flat.compatisnull".equalsIgnoreCase(type)) {
            return new FlatLocalOrcWriterCompatIsNull(filePath, schema, options, rawSchema,
                    indexsRoot, delimiterRoot);
        } else {
            throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    protected static String[] types = {"flat.exact", "flat.exactnull", "flat.exactnullisnull", "flat.exactisnull",
                                       "flat.compat", "flat.compatnull", "flat.compatnullisnull", "flat.compatisnull"};

    public static boolean checkType(String type) {
        for(String t : types) {
            if(t.equalsIgnoreCase(type)) {
                return true;
            }
        }
        return false;
    }
}
