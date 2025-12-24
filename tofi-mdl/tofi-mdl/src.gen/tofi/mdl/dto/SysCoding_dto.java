/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Системы кодирования
* <p>
    * table: SysCoding
    */
    public class SysCoding_dto {

    private Long id;

    @FieldProps(size = 30)
    private String cod;

    @FieldProps(dict = "FD_AccessLevel")
    private Long accessLevel;

    @FieldProps(dict = "FD_SysCodingType")
    private Long sysCodingType;

    @FieldProps(size = 20)
    private String sgnSeparator;

    private Long sourceStock;

    @FieldProps(size = 200)
    private String name;

    @FieldProps(size = 400)
    private String fullName;

    private String cmt;

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
    * Код
    */
    public String getCod() {
    return this.cod;
    }

    public void setCod(String v) {
    this.cod = v;
    }

    /**
    * Уровень доступа
    */
    public Long getAccessLevel() {
    return this.accessLevel;
    }

    public void setAccessLevel(Long v) {
    this.accessLevel = v;
    }

    /**
    * Тип системы кодирования
    */
    public Long getSysCodingType() {
    return this.sysCodingType;
    }

    public void setSysCodingType(Long v) {
    this.sysCodingType = v;
    }

    /**
    * sgnSeparator
    */
    public String getSgnSeparator() {
    return this.sgnSeparator;
    }

    public void setSgnSeparator(String v) {
    this.sgnSeparator = v;
    }

    /**
    * Источники и стоки данных
    */
    public Long getSourceStock() {
    return this.sourceStock;
    }

    public void setSourceStock(Long v) {
    this.sourceStock = v;
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

    }
