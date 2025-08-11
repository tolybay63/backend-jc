/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.datetime.*;

/**
 * Интервалы жизни ролей
* <p>
    * table: TypRoleLifeInterval
    */
    public class TypRoleLifeInterval_dto {

    private Long id;

    private Long typRole;

    private XDate dbeg;

    private XDate dend;

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
    * Роли типы объектов
    */
    public Long getTypRole() {
    return this.typRole;
    }

    public void setTypRole(Long v) {
    this.typRole = v;
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
    * Комментарий
    */
    public String getCmt() {
    return this.cmt;
    }

    public void setCmt(String v) {
    this.cmt = v;
    }

    }
