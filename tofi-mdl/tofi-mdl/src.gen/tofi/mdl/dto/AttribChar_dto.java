/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Характеристики атрибута
* <p>
    * table: AttribChar
    */
    public class AttribChar_dto {

    private Long id;

    private Long attrib;

    @FieldProps(size = 30)
    private String maskReg;

    @FieldProps(size = 30)
    private String format;

    @FieldProps(size = 30)
    private String fileExt;

    @FieldProps(dict = "FD_EntityType")
    private Long entityType;

    @FieldProps(dict = "FD_PeriodType")
    private Long periodType;

    /**
    * id
    */
    public Long getId() {
    return this.id;
    }

    public void setId(Long v) {
    this.id = v;
    }

    /**
    * Атрибутивные свойства
    */
    public Long getAttrib() {
    return this.attrib;
    }

    public void setAttrib(Long v) {
    this.attrib = v;
    }

    /**
    * Маска
    */
    public String getMaskReg() {
    return this.maskReg;
    }

    public void setMaskReg(String v) {
    this.maskReg = v;
    }

    /**
    * Формат
    */
    public String getFormat() {
    return this.format;
    }

    public void setFormat(String v) {
    this.format = v;
    }

    /**
    * Расширение файла
    */
    public String getFileExt() {
    return this.fileExt;
    }

    public void setFileExt(String v) {
    this.fileExt = v;
    }

    /**
    * Сущность ТОФИ
    */
    public Long getEntityType() {
    return this.entityType;
    }

    public void setEntityType(Long v) {
    this.entityType = v;
    }

    /**
    * Тип периода
    */
    public Long getPeriodType() {
    return this.periodType;
    }

    public void setPeriodType(Long v) {
    this.periodType = v;
    }

    }
