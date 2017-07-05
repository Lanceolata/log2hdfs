package com.ipinyou.compress.compress;

import com.ipinyou.compress.orc.OrcWriterOptions;
import com.ipinyou.compress.orc.local.flat.FlatLocalOrcWriterFactory;
import com.ipinyou.compress.orc.local.nested.NestedLocalOrcWriterFactory;
import com.ipinyou.compress.util.Delimiter;
import com.ipinyou.compress.util.Indexs;
import com.ipinyou.compress.util.SchemaUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.orc.TypeDescription;
import org.ini4j.Profile.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ipinyou.compress.util.FileUtils.createDir;

/**
 * Created by lanceolata on 17-7-3.
 */
public class FileProperty {
    private static final Logger logger = LoggerFactory.getLogger(FileProperty.class);

    /**
     * File default roperties
     */


    private TypeDescription schema;
    private TypeDescription rawSchema;
    private TypeDescription flatSchema;
    private Delimiter delimiters;
    private String type;
    private Indexs indexs;
    private String backupDir = null;
    private OrcWriterOptions orcWriterOptions;

    public FileProperty(Section section) {
        if(section == null) {
            throw new IllegalArgumentException("section is null");
        }
        OrcWriterOptions options = new OrcWriterOptions(section);
        setProperties(section.get("raw.schema"), section.get("schema"),
                      section.get("delimiter"), section.get("type"),
                      section.get("backup"), options);
    }

    public void setProperties(String rawSchema, String schema, String delimiters,
                              String type, String backup, OrcWriterOptions options) {

        if(schema == null || "".equals(schema)) {
            throw new IllegalArgumentException("schema is empty");
        }
        this.schema = TypeDescription.fromString(schema);

        if (rawSchema == null || "".equals(rawSchema)) {
            this.rawSchema = TypeDescription.fromString(schema);
        } else {
            this.rawSchema = TypeDescription.fromString(rawSchema);
        }

        if (delimiters == null && "".equals(delimiters)) {
            throw new IllegalArgumentException("delimiters error");
        }
        this.delimiters = Delimiter.fromString(delimiters);

        if (!Delimiter.checkDelimiter(this.rawSchema.getChildren(), this.delimiters)) {
            logger.error("FileProperty unmatch delimiters[{}]", this.delimiters.toString());
            throw new IllegalArgumentException("schema separator not match");
        }

        if (type == null || "".equals(type)) {
            throw new IllegalArgumentException("type error");
        }
        this.type = type;
        logger.info("FileProperty type[{}]", this.type);

        if (NestedLocalOrcWriterFactory.checkType(type)) {
            this.indexs = Indexs.buildIndexs(this.rawSchema, this.schema);
            if (this.indexs == null) {
                throw new IllegalArgumentException("build Indexs failed");
            }
        } else if (FlatLocalOrcWriterFactory.checkType(type)) {
            String flatSchema = SchemaUtils.buildFlatSchema(this.rawSchema);
            if (flatSchema == null || "".equals(flatSchema)) {
                throw new IllegalArgumentException("buildFlatSchema failed");
            }

            this.flatSchema = TypeDescription.fromString(flatSchema);
            if (!SchemaUtils.checkFlatSchema(this.schema)) {
                throw new IllegalArgumentException("checkFlatSchema failed");
            }

            this.indexs = Indexs.buildIndexs(this.flatSchema, this.schema);
            if (this.indexs == null) {
                throw new IllegalArgumentException("build indexs failed");
            }
        } else {
            throw new IllegalArgumentException("type error");
        }
        logger.info("FileProperty indexs[{}]", this.indexs.toString());

        if (backup != null && !"".equals(backup)) {
            if (!createDir(backup)) {
                throw new IllegalArgumentException("invalid backup dir");
            } else {
                this.backupDir = backup;
            }
        }

        this.orcWriterOptions = options;
    }

    public TypeDescription getSchema() {
        return schema;
    }

    public void setSchema(TypeDescription schema) {
        this.schema = schema;
    }

    public TypeDescription getRawSchema() {
        return rawSchema;
    }

    public void setRawSchema(TypeDescription rawSchema) {
        this.rawSchema = rawSchema;
    }

    public TypeDescription getFlatSchema() {
        return flatSchema;
    }

    public void setFlatSchema(TypeDescription flatSchema) {
        this.flatSchema = flatSchema;
    }

    public Delimiter getDelimiters() {
        return delimiters;
    }

    public void setDelimiters(Delimiter delimiters) {
        this.delimiters = delimiters;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Indexs getIndexs() {
        return indexs;
    }

    public void setIndexs(Indexs indexs) {
        this.indexs = indexs;
    }

    public String getBackupDir() {
        return backupDir;
    }

    public void setBackupDir(String backupDir) {
        this.backupDir = backupDir;
    }

    public OrcWriterOptions getOrcWriterOptions() {
        return orcWriterOptions;
    }

    public void setOrcWriterOptions(OrcWriterOptions orcWriterOptions) {
        this.orcWriterOptions = orcWriterOptions;
    }
}
