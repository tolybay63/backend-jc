/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Переводы
* <p>
    * table: TableLang
    */
    public class TableLang_dto {

    private Long id;

    @FieldProps(size = 32)
    private String nameTable;

    private Long idTable;

    @FieldProps(size = 200)
    private String name;

    @FieldProps(size = 400)
    private String fullName;

    private String cmt;

    @FieldProps(size = 5)
    private String lang;

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
    * nameTable
    */
    public String getNameTable() {
    return this.nameTable;
    }

    public void setNameTable(String v) {
    this.nameTable = v;
    }

    /**
    * idTable
    */
    public Long getIdTable() {
    return this.idTable;
    }

    public void setIdTable(Long v) {
    this.idTable = v;
    }

    /**
    * Краткое наименование
    */
    public String getName() {
    return this.name;
    }

    public void setName(String v) {
    this.name = v;
    }

    /**
    * Полное наименование
    */
    public String getFullName() {
    return this.fullName;
    }

    public void setFullName(String v) {
    this.fullName = v;
    }

    /**
    * Комментарий
    */
    public String getCmt() {
    return this.cmt;
    }

    public void setCmt(String v) {
    this.cmt = v;
    }

    /**
    * lang
    */
    public String getLang() {
    return this.lang;
    }

    public void setLang(String v) {
    this.lang = v;
    }

    }
