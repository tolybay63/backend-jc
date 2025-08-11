/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;


/**
 * Факторы мягкого измерителя
* <p>
    * table: MeterFactor
    */
    public class MeterFactor_dto {

    private Long id;

    private Long meter;

    private Long factor;

    private Long ordDim;

    private Long ordFactorInDim;

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
    * Измерители
    */
    public Long getMeter() {
    return this.meter;
    }

    public void setMeter(Long v) {
    this.meter = v;
    }

    /**
    * Факторы и значения факторов
    */
    public Long getFactor() {
    return this.factor;
    }

    public void setFactor(Long v) {
    this.factor = v;
    }

    /**
    * ordDim
    */
    public Long getOrdDim() {
    return this.ordDim;
    }

    public void setOrdDim(Long v) {
    this.ordDim = v;
    }

    /**
    * ordFactorInDim
    */
    public Long getOrdFactorInDim() {
    return this.ordFactorInDim;
    }

    public void setOrdFactorInDim(Long v) {
    this.ordFactorInDim = v;
    }

    }
