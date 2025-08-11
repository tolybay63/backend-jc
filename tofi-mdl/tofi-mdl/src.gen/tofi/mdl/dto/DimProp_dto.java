/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Измерения свойств
* <p>
    * table: DimProp
    */
    public class DimProp_dto {

    private Long id;

    private Long dimPropGr;

    @FieldProps(dict = "FD_DimPropType")
    private Long dimPropType;

    private Long dimMultiProp;

    @FieldProps(size = 30)
    private String cod;

    @FieldProps(dict = "FD_AccessLevel")
    private Long accessLevel;

    @FieldProps(size = 200)
    private String name;

    @FieldProps(size = 400)
    private String fullName;

    private String cmt;

    private Long ord;

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
    * Группы свойств
    */
    public Long getDimPropGr() {
    return this.dimPropGr;
    }

    public void setDimPropGr(Long v) {
    this.dimPropGr = v;
    }

    /**
    * Типы измерения свойства
    */
    public Long getDimPropType() {
    return this.dimPropType;
    }

    public void setDimPropType(Long v) {
    this.dimPropType = v;
    }

    /**
    * Измерения многомерного свойства
    */
    public Long getDimMultiProp() {
    return this.dimMultiProp;
    }

    public void setDimMultiProp(Long v) {
    this.dimMultiProp = v;
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
    * Порядковый номер
    */
    public Long getOrd() {
    return this.ord;
    }

    public void setOrd(Long v) {
    this.ord = v;
    }

    }
