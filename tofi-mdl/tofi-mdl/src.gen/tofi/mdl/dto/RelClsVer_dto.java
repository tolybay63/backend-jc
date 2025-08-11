/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.datetime.*;
import jandcode.commons.reflect.*;

/**
 * Версионная часть класса отношений
* <p>
    * table: RelClsVer
    */
    public class RelClsVer_dto {

    private Long id;

    private Long ownerVer;

    @FieldProps(size = 200)
    private String name;

    @FieldProps(size = 400)
    private String fullName;

    private XDate dbeg;

    private XDate dend;

    private Integer lastVer;

    private String cmtVer;

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
    * Классы отношений
    */
    public Long getOwnerVer() {
    return this.ownerVer;
    }

    public void setOwnerVer(Long v) {
    this.ownerVer = v;
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
    * Начало интервала
    */
    public XDate getDbeg() {
    return this.dbeg;
    }

    public void setDbeg(XDate v) {
    this.dbeg = v;
    }

    /**
    * Конец интервала
    */
    public XDate getDend() {
    return this.dend;
    }

    public void setDend(XDate v) {
    this.dend = v;
    }

    /**
    * lastVer
    */
    public Integer getLastVer() {
    return this.lastVer;
    }

    public void setLastVer(Integer v) {
    this.lastVer = v;
    }

    /**
    * Комментарий
    */
    public String getCmtVer() {
    return this.cmtVer;
    }

    public void setCmtVer(String v) {
    this.cmtVer = v;
    }

    }
