/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Многомерные свойства
* <p>
    * table: MultiProp
    */
    public class MultiProp_dto {

    private Long id;

    @FieldProps(size = 30)
    private String cod;

    @FieldProps(dict = "FD_AccessLevel")
    private Long accessLevel;

    private Long MultiPropGr;

    private Boolean isUniq;

    private Boolean isDependValueOnPeriod;

    private Long statusFactor;

    private Long providerTyp;

    @FieldProps(size = 200)
    private String name;

    @FieldProps(size = 400)
    private String fullName;

    private Long ord;

    private Integer fillMore;

    @FieldProps(size = 100)
    private String propTag;

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
    * Группа многомерных свойств
    */
    public Long getMultiPropGr() {
    return this.MultiPropGr;
    }

    public void setMultiPropGr(Long v) {
    this.MultiPropGr = v;
    }

    /**
    * isUniq
    */
    public Boolean getIsUniq() {
    return this.isUniq;
    }

    public void setIsUniq(Boolean v) {
    this.isUniq = v;
    }

    /**
    * isDependValueOnPeriod
    */
    public Boolean getIsDependValueOnPeriod() {
    return this.isDependValueOnPeriod;
    }

    public void setIsDependValueOnPeriod(Boolean v) {
    this.isDependValueOnPeriod = v;
    }

    /**
    * Факторы и значения факторов
    */
    public Long getStatusFactor() {
    return this.statusFactor;
    }

    public void setStatusFactor(Long v) {
    this.statusFactor = v;
    }

    /**
    * Типы объектов
    */
    public Long getProviderTyp() {
    return this.providerTyp;
    }

    public void setProviderTyp(Long v) {
    this.providerTyp = v;
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
    * Порядковый номер
    */
    public Long getOrd() {
    return this.ord;
    }

    public void setOrd(Long v) {
    this.ord = v;
    }

    /**
    * fillMore
    */
    public Integer getFillMore() {
    return this.fillMore;
    }

    public void setFillMore(Integer v) {
    this.fillMore = v;
    }

    /**
    * propTag
    */
    public String getPropTag() {
    return this.propTag;
    }

    public void setPropTag(String v) {
    this.propTag = v;
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
