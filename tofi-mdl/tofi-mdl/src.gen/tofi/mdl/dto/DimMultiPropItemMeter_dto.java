/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;


/**
 * Свойства значения элемента измерения многомерного свойства типа число
* <p>
    * table: DimMultiPropItemMeter
    */
    public class DimMultiPropItemMeter_dto {

    private Long id;

    private Long dimMultiPropItem;

    private Long measure;

    private Double minVal;

    private Double maxVal;

    private Integer digit;

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
    * Элементы измерения многомерного свойства
    */
    public Long getDimMultiPropItem() {
    return this.dimMultiPropItem;
    }

    public void setDimMultiPropItem(Long v) {
    this.dimMultiPropItem = v;
    }

    /**
    * Единицы измерения
    */
    public Long getMeasure() {
    return this.measure;
    }

    public void setMeasure(Long v) {
    this.measure = v;
    }

    /**
    * minVal
    */
    public Double getMinVal() {
    return this.minVal;
    }

    public void setMinVal(Double v) {
    this.minVal = v;
    }

    /**
    * maxVal
    */
    public Double getMaxVal() {
    return this.maxVal;
    }

    public void setMaxVal(Double v) {
    this.maxVal = v;
    }

    /**
    * digit
    */
    public Integer getDigit() {
    return this.digit;
    }

    public void setDigit(Integer v) {
    this.digit = v;
    }

    }
