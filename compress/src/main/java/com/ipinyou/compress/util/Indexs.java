package com.ipinyou.compress.util;

import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by lanceolata on 17-3-24.
 */
public class Indexs {

    private static final Logger logger = LoggerFactory.getLogger(Indexs.class);

    private int rawLength;
    private int[] indexs;
    private Indexs[] children;

    public Indexs(int rawLength, int[] indexs, Indexs[] children) {
        if (rawLength <= 0 || indexs == null || indexs.length <= 0
                || children == null || children.length <= 0) {
            throw new IllegalArgumentException("Invalid rawSchema or schema");
        }

        this.rawLength = rawLength;
        this.indexs = indexs;
        this.children = children;
    }

    public int getIndex(int index) {
        return indexs[index];
    }

    public Indexs getChild(int index) {
        return children[index];
    }

    public int getRawLength() {
        return rawLength;
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(rawLength);
        buffer.append("<");
        for (int i = 0; i < indexs.length; ++i) {
            buffer.append(indexs[i]);
            if (children[i] != null) {
                buffer.append(":");
                buffer.append(children[i].toString());
            }
            if (i != indexs.length - 1) {
                buffer.append(",");
            }
        }
        buffer.append(">");
        return buffer.toString();
    }

    public static Indexs buildIndexs(TypeDescription rawSchema, TypeDescription schema) {
        List<TypeDescription> subs = schema.getChildren();
        List<String> names = schema.getFieldNames();
        int rawLength = rawSchema.getChildren().size();
        int[] indexs = new int[subs.size()];
        Indexs[] children = new Indexs[subs.size()];
        for (int i = 0; i < subs.size(); ++i) {
            String type = subs.get(i).getCategory().getName();
            int index = getIndexFromSchema(rawSchema, names.get(i), type);
            indexs[i] = index;
            if (index >= 0 && "struct".equalsIgnoreCase(type)) {
                children[i] = buildIndexs(rawSchema.getChildren().get(index), subs.get(i));
            } else {
                children[i] = null;
            }
        }
        return new Indexs(rawLength, indexs, children);
    }

    public static Indexs buildIndexsWithNULL(TypeDescription schema) {
        List<TypeDescription> subs = schema.getChildren();
        int[] indexs = new int[subs.size()];
        Indexs[] children = new Indexs[subs.size()];
        for (int i = 0; i < subs.size(); ++i) {
            String type = subs.get(i).getCategory().getName();
            indexs[i] = -1;
            if ("struct".equalsIgnoreCase(type)) {
                children[i] = buildIndexsWithNULL(subs.get(i));
            } else {
                children[i] = null;
            }
        }
        return new Indexs(0, indexs, children);
    }

    private static int getIndexFromSchema(TypeDescription rawSchema, String name, String type) {
        List<TypeDescription> children = rawSchema.getChildren();
        List<String> names = rawSchema.getFieldNames();

        for (int i = 0; i < children.size(); ++i) {
            if (type.equalsIgnoreCase(children.get(i).getCategory().getName()) &&
                    name.equalsIgnoreCase(names.get(i))) {
                return i;
            }
        }
        return -1;
    }
}
