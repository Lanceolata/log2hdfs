package com.ipinyou.compress.util;

import org.apache.orc.TypeDescription;

import java.util.List;

/**
 * Created by lanceolata on 17-3-24.
 */
public class SchemaUtils {

    public static boolean checkFlatSchema(TypeDescription schema) {
        if (schema == null) {
            return false;
        }

        List<TypeDescription> children = schema.getChildren();
        for (TypeDescription child : children) {
            if ("struct".equalsIgnoreCase(child.getCategory().toString())) {
                return false;
            }
        }
        return true;
    }

    public static String buildFlatSchema(TypeDescription schema) {
        if (schema == null) {
            return null;
        }
        List<TypeDescription> children = schema.getChildren();
        List<String> names = schema.getFieldNames();

        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < children.size(); ++i) {
            String fieldName = names.get(i);
            String fieldType = children.get(i).getCategory().getName();
            if ("struct".equalsIgnoreCase(fieldType)) {
                String sub = buildFlatSchemaRecursive(fieldName, children.get(i));
                if (sub == null || "".equals(sub)) {
                    return null;
                }
                buffer.append(sub);
            } else {
                buffer.append(fieldName);
                buffer.append(":");
                buffer.append(fieldType);
            }
            if (i != children.size() - 1) {
                buffer.append(",");
            }
        }
        return "struct<" + buffer.toString() + ">";
    }

    private static String buildFlatSchemaRecursive(String prefix, TypeDescription schema) {
        List<TypeDescription> children = schema.getChildren();
        List<String> names = schema.getFieldNames();

        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < children.size(); ++i) {
            String fieldName = prefix + "_" + names.get(i);
            String fieldType = children.get(i).getCategory().getName();
            if ("struct".equalsIgnoreCase(fieldType)) {
                String sub = buildFlatSchemaRecursive(fieldName, children.get(i));
                if (sub == null || "".equals(sub)) {
                    return null;
                }
                buffer.append(sub);
            } else {
                buffer.append(fieldName);
                buffer.append(":");
                buffer.append(fieldType);
            }
            if (i != children.size() - 1) {
                buffer.append(",");
            }
        }
        return buffer.toString();
    }
}
