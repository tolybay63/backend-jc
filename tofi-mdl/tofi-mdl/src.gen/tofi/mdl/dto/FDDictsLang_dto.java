/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Многоязычный словарь
* <p>
    * table: FD_DictsLang
    */
    public class FDDictsLang_dto {

    private Long id;

    @FieldProps(size = 120)
    private String text;

    @FieldProps(size = 30)
    private String nameDict;

    private Long idDict;

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
    * valueDict
    */
    public String getText() {
    return this.text;
    }

    public void setText(String v) {
    this.text = v;
    }

    /**
    * nameDict
    */
    public String getNameDict() {
    return this.nameDict;
    }

    public void setNameDict(String v) {
    this.nameDict = v;
    }

    /**
    * idDict
    */
    public Long getIdDict() {
    return this.idDict;
    }

    public void setIdDict(Long v) {
    this.idDict = v;
    }

    /**
    * Язык
    */
    public String getLang() {
    return this.lang;
    }

    public void setLang(String v) {
    this.lang = v;
    }

    }
