/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;


/**
 * Роли типы объектов
* <p>
    * table: RelTypRole
    */
    public class RelTypRole_dto {

    private Long id;

    private Long relTyp;

    private Long role;

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
    * Отношения между типами объектов
    */
    public Long getRelTyp() {
    return this.relTyp;
    }

    public void setRelTyp(Long v) {
    this.relTyp = v;
    }

    /**
    * Роли типов объектов и отношений между типами объектов
    */
    public Long getRole() {
    return this.role;
    }

    public void setRole(Long v) {
    this.role = v;
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
