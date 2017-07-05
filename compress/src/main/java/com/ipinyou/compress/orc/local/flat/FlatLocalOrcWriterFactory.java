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
    public static OrcWriter create(FlatLocalOrcWriterType type, String filePath, TypeDescription schema,
                                   OrcWriterOptions options, TypeDescription rawSchema,
                                   Indexs indexsRoot, Delimiter delimiterRoot)
            throws IOException, IllegalArgumentException {

        switch (type) {
            case flat_exact:
                return new FlatLocalOrcWriterExact(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            case flat_exact_blank:
                return new FlatLocalOrcWriterExactBlank(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            case flat_exact_null:
                return new FlatLocalOrcWriterExactNull(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            case flat_exact_blanknull:
                return new FlatLocalOrcWriterExactBlankNull(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            case flat_compat:
                return new FlatLocalOrcWriterCompat(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            case flat_compat_blank:
                return new FlatLocalOrcWriterCompatBlank(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            case flat_compat_null:
                return new FlatLocalOrcWriterCompatNull(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            case flat_compat_blanknull:
                return new FlatLocalOrcWriterCompatBlankNull(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    public enum FlatLocalOrcWriterType {
        flat_exact,             // 严格限制列长度，并且不会设置为null
        flat_exact_blank,       // 严格限制列长度，""设置为null
        flat_exact_null,        // 严格限制列长度，"null"设置为null
        flat_exact_blanknull,   // 严格限制列长度，"null"和""设置为null
        flat_compat,            // 兼容列长度，多出列舍弃，少列填为null，null设置为null
        flat_compat_blank,      // 兼容列长度，多出列舍弃，少列填为null，""和null设置为null
        flat_compat_null,       // 兼容列长度，多出列舍弃，少列填为null，"null"和null设置为null
        flat_compat_blanknull,  // 兼容列长度，多出列舍弃，少列填为null，"null" ""和null设置为null
    }

    public static FlatLocalOrcWriterType getType(String type) {
        for (FlatLocalOrcWriterType flatType : FlatLocalOrcWriterType.values()) {
            if (flatType.name().equals(type)) {
                return flatType;
            }
        }
        return null;
    }

    public static boolean checkType(String type) {
        for (FlatLocalOrcWriterType flatType : FlatLocalOrcWriterType.values()) {
            if (flatType.name().equals(type)) {
                return true;
            }
        }
        return false;
    }
}
