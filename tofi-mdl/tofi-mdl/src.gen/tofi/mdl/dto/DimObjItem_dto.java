/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Компоненты измерения объектов и отношений
* <p>
    * table: DimObjItem
    */
    public class DimObjItem_dto {

    private Long id;

    private Long dimObj;

    @FieldProps(dict = "FD_LinkType")
    private Long linkType;

    @FieldProps(dict = "FD_DimObjItemType")
    private Long dimObjItemType;

    private Long typ;

    private Long cls;

    private Integer lev;

    private Long relTyp;

    private Long relCls;

    private Long relTypMember;

    private Long relClsMember;

    private Long parent;

    private Long prop;

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
    * Измерения объектов и отношений
    */
    public Long getDimObj() {
    return this.dimObj;
    }

    public void setDimObj(Long v) {
    this.dimObj = v;
    }

    /**
    * Тип связи между родительским и дочерним компонентами
    */
    public Long getLinkType() {
    return this.linkType;
    }

    public void setLinkType(Long v) {
    this.linkType = v;
    }

    /**
    * Типы компонента измерения объектов и отношений
    */
    public Long getDimObjItemType() {
    return this.dimObjItemType;
    }

    public void setDimObjItemType(Long v) {
    this.dimObjItemType = v;
    }

    /**
    * Типы объектов
    */
    public Long getTyp() {
    return this.typ;
    }

    public void setTyp(Long v) {
    this.typ = v;
    }

    /**
    * Классы
    */
    public Long getCls() {
    return this.cls;
    }

    public void setCls(Long v) {
    this.cls = v;
    }

    /**
    * Уровень
    */
    public Integer getLev() {
    return this.lev;
    }

    public void setLev(Integer v) {
    this.lev = v;
    }

    /**
    * Отношения между типами объектов
    */
    public Long getRelTyp() {
    return this.relTyp;
    }

    public void setRelTyp(Long v) {
    this.relTyp = v;
    }

    /**
    * Классы отношений
    */
    public Long getRelCls() {
    return this.relCls;
    }

    public void setRelCls(Long v) {
    this.relCls = v;
    }

    /**
    * Участники отношения между типами объектов
    */
    public Long getRelTypMember() {
    return this.relTypMember;
    }

    public void setRelTypMember(Long v) {
    this.relTypMember = v;
    }

    /**
    * Участники класса отношений
    */
    public Long getRelClsMember() {
    return this.relClsMember;
    }

    public void setRelClsMember(Long v) {
    this.relClsMember = v;
    }

    /**
    * Компоненты измерения объектов и отношений
    */
    public Long getParent() {
    return this.parent;
    }

    public void setParent(Long v) {
    this.parent = v;
    }

    /**
    * Свойства
    */
    public Long getProp() {
    return this.prop;
    }

    public void setProp(Long v) {
    this.prop = v;
    }

    }
