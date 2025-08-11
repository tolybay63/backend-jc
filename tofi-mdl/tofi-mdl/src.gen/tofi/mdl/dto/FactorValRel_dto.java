/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;


/**
 * Несовместные значения факторов и зависимые факторы
* <p>
    * table: FactorValRel
    */
    public class FactorValRel_dto {

    private Long id;

    private Long factor1;

    private Long factor2;

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
    * Факторы и значения факторов
    */
    public Long getFactor1() {
    return this.factor1;
    }

    public void setFactor1(Long v) {
    this.factor1 = v;
    }

    /**
    * Факторы и значения факторов
    */
    public Long getFactor2() {
    return this.factor2;
    }

    public void setFactor2(Long v) {
    this.factor2 = v;
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
