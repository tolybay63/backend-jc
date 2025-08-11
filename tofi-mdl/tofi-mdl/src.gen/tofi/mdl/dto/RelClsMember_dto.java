/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Участники класса отношений
* <p>
    * table: RelClsMember
    */
    public class RelClsMember_dto {

    private Long id;

    private Long relCls;

    @FieldProps(size = 200)
    private String name;

    @FieldProps(size = 400)
    private String fullName;

    private Integer card;

    @FieldProps(dict = "FD_MemberType")
    private Long memberType;

    private Long cls;

    private Long relClsMemb;

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
    * Классы отношений
    */
    public Long getRelCls() {
    return this.relCls;
    }

    public void setRelCls(Long v) {
    this.relCls = v;
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
    * Число кардинальности
    */
    public Integer getCard() {
    return this.card;
    }

    public void setCard(Integer v) {
    this.card = v;
    }

    /**
    * Тип участника отношения между типами объектов
    */
    public Long getMemberType() {
    return this.memberType;
    }

    public void setMemberType(Long v) {
    this.memberType = v;
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
    * Классы отношений
    */
    public Long getRelClsMemb() {
    return this.relClsMemb;
    }

    public void setRelClsMemb(Long v) {
    this.relClsMemb = v;
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
