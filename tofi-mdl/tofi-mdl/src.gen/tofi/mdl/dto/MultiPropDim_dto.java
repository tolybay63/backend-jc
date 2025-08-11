/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;


/**
 * Упорядоченный набор измерений многомерного свойства
* <p>
    * table: MultiPropDim
    */
    public class MultiPropDim_dto {

    private Long id;

    private Long multiProp;

    private Long dimMultiProp;

    private Integer dimNumber;

    private Long ord;

    private Integer isFilled;

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
    * Многомерные свойства
    */
    public Long getMultiProp() {
    return this.multiProp;
    }

    public void setMultiProp(Long v) {
    this.multiProp = v;
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
    * Номер измерения
    */
    public Integer getDimNumber() {
    return this.dimNumber;
    }

    public void setDimNumber(Integer v) {
    this.dimNumber = v;
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
    * isFilled
    */
    public Integer getIsFilled() {
    return this.isFilled;
    }

    public void setIsFilled(Integer v) {
    this.isFilled = v;
    }

    }
