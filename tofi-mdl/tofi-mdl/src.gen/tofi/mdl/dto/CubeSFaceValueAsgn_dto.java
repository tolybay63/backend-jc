/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;


/**
 * Сопоставление участников
* <p>
    * table: CubeSFaceValueAsgn
    */
    public class CubeSFaceValueAsgn_dto {

    private Long id;

    private Long cubeSFaceValue;

    private Long relTypMember;

    private Long relClsMember;

    private Long dimObjItem;

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
    * Отношения между типам объектов
    */
    public Long getCubeSFaceValue() {
    return this.cubeSFaceValue;
    }

    public void setCubeSFaceValue(Long v) {
    this.cubeSFaceValue = v;
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
    public Long getDimObjItem() {
    return this.dimObjItem;
    }

    public void setDimObjItem(Long v) {
    this.dimObjItem = v;
    }

    }
