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

    public static OrcWriter create(NestedLocalOrcWriterType type, String filePath, TypeDescription schema,
                                   OrcWriterOptions options, TypeDescription rawSchema,
                                   Indexs indexsRoot, Delimiter delimiterRoot)
            throws IOException, IllegalArgumentException {

        switch (type) {
            case nested_exact:
                return new NestedLocalOrcWriterExact(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            case nested_exact_blank:
                return new NestedLocalOrcWriterExactBlank(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            case nested_exact_null:
                return new NestedLocalOrcWriterExactNull(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            case nested_exact_blanknull:
                return new NestedLocalOrcWriterExactBlankNull(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            case nested_compat:
                return new NestedLocalOrcWriterCompat(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            case nested_compat_blank:
                return new NestedLocalOrcWriterCompatBlank(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            case nested_compat_null:
                return new NestedLocalOrcWriterCompatNull(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            case nested_compat_blanknull:
                return new NestedLocalOrcWriterCompatBlankNull(filePath, schema, options, rawSchema,
                        indexsRoot, delimiterRoot);
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    public enum NestedLocalOrcWriterType {
        nested_exact,             // 严格限制列长度，并且不会设置为null
        nested_exact_blank,       // 严格限制列长度，""设置为null
        nested_exact_null,        // 严格限制列长度，"null"设置为null
        nested_exact_blanknull,   // 严格限制列长度，"null"和""设置为null
        nested_compat,            // 兼容列长度，多出列舍弃，少列填为null，null设置为null
        nested_compat_blank,      // 兼容列长度，多出列舍弃，少列填为null，""和null设置为null
        nested_compat_null,       // 兼容列长度，多出列舍弃，少列填为null，"null"和null设置为null
        nested_compat_blanknull,  // 兼容列长度，多出列舍弃，少列填为null，"null" ""和null设置为null
    }

    public static NestedLocalOrcWriterType getType(String type) {
        for (NestedLocalOrcWriterType nestedType : NestedLocalOrcWriterType.values()) {
            if (nestedType.name().equals(type)) {
                return nestedType;
            }
        }
        return null;
    }

    public static boolean checkType(String type) {
        for (NestedLocalOrcWriterType nestedType : NestedLocalOrcWriterType.values()) {
            if (nestedType.name().equals(type)) {
                return true;
            }
        }
        return false;
    }
}
