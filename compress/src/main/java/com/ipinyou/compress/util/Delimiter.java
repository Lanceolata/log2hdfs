package com.ipinyou.compress.util;

import org.apache.orc.TypeDescription;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lance on 2017/7/4.
 */
public class Delimiter {
    private String delimiter;
    private int length = 0;
    private Delimiter[] children = null;

    public Delimiter(String separator, Delimiter[] children) {
        this.delimiter = separator;
        if(children != null && children.length > 0) {
            this.children = children;
            this.length = children.length;
        }
    }

    public Delimiter getChild(int index) {
        if(children == null || length == 0) {
            return null;
        }
        if(index >= length) {
            return children[length - 1];
        }
        return children[index];
    }

    public String getDelimiter() {
        return delimiter;
    }

    public Delimiter[] getChildren() {
        return children;
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(delimiter);
        if(children != null) {
            buffer.append("<");
            for(Delimiter node : children) {
                buffer.append(node.toString());
            }
            buffer.append(">,");
        }
        return buffer.toString();
    }

    private final static Map<String, String> separatorMap = new HashMap<String, String>() {
        {
            put("t", "\t");
            put("u0001", "\u0001");
            put("u0002", "\u0002");
            put("u0003", "\u0003");
        }
    };

    public static Delimiter fromString(String struct) {
        if(struct == null) {
            return null;
        } else {
            String s = struct.trim();
            Delimiter.StringPosition source = new Delimiter.StringPosition(s);
            Delimiter result = parseStruct(source);
            if(source.position != source.length) {
                throw new IllegalArgumentException("Extra characters at " + source);
            } else {
                return result;
            }
        }
    }

    private static Delimiter parseStruct(Delimiter.StringPosition source) {
        Delimiter res = parse(source);
        if(source.position != source.length) {
            throw new IllegalArgumentException("Extra characters at " + source);
        } else {
            return res;
        }
    }

    private static Delimiter parse(Delimiter.StringPosition source) {
        String separator = parseSeparator(source);
        Delimiter[] nodes = null;
        if(source.position < source.length && source.value.charAt(source.position) == '<') {
            nodes = parseChild(source);
        }
        return new Delimiter(separator, nodes);
    }

    private static Delimiter[] parseChild(Delimiter.StringPosition source) {
        requireChar(source, '<');

        List<Delimiter> list = new ArrayList<Delimiter>();
        do {
            int count = parseCount(source);
            Delimiter temp = parse(source);
            for(int i = 0; i < count; ++i) {
                list.add(temp);
            }
        } while(consumeChar(source, ','));

        requireChar(source, '>');
        Delimiter[] res = new Delimiter[list.size()];
        int index = 0;
        for(Delimiter node : list) {
            res[index++] = node;
        }
        return res;
    }

    private static int parseCount(Delimiter.StringPosition source) {
        int start;
        for(start = source.position; source.position < source.length; ++source.position) {
            char word = source.value.charAt(source.position);
            if(!Character.isDigit(word)) {
                break;
            }
        }
        if(source.position != start) {
            String temp = source.value.substring(start, source.position);
            requireChar(source, ':');
            return Integer.parseInt(temp);
        }
        return 1;
    }

    private static String parseSeparator(Delimiter.StringPosition source) {
        int start;
        for(start = source.position; source.position < source.length; ++source.position) {
            char word = source.value.charAt(source.position);
            if(!Character.isLetterOrDigit(word)) {
                break;
            }
        }
        if(source.position != start) {
            String temp = source.value.substring(start, source.position).toLowerCase();
            String res = separatorMap.get(temp);
            if(res != null) {
                return res;
            }
        }

        throw new IllegalArgumentException("Can\'t parse Separator at " + source.value);
    }

    static void requireChar(Delimiter.StringPosition source, char required) {
        if(source.position < source.length && source.value.charAt(source.position) == required) {
            ++source.position;
        } else {
            throw new IllegalArgumentException("Missing required char \'" + required + "\' at " + source);
        }
    }

    static boolean consumeChar(Delimiter.StringPosition source, char ch) {
        boolean result = source.position < source.length && source.value.charAt(source.position) == ch;
        if(result) {
            ++source.position;
        }
        return result;
    }

    static class StringPosition {
        final String value;
        int position;
        final int length;

        StringPosition(String value) {
            this.value = value;
            this.position = 0;
            this.length = value.length();
        }
    }

    public static boolean checkDelimiter(List<TypeDescription> children, Delimiter delimiters) {
        int childIndex = 0;
        boolean res;
        for(TypeDescription child : children) {
            String fieldtype = child.getCategory().toString();
            if("struct".equalsIgnoreCase(fieldtype)) {
                Delimiter dChild = delimiters.getChild(childIndex++);
                if(dChild == null) {
                    return false;
                }
                res = checkDelimiter(child.getChildren(), dChild);
                if (!res) {
                    return false;
                }
            }
        }
        return true;
    }
}
