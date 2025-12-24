/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Внешний код в системе кодирования sysCoding
* <p>
    * table: SysCodingCod
    */
    public class SysCodingCod_dto {

    private Long id;

    private Long sysCod;

    private Long sysCoding;

    @FieldProps(size = 30)
    private String cod;

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
    * Встроенные коды
    */
    public Long getSysCod() {
    return this.sysCod;
    }

    public void setSysCod(Long v) {
    this.sysCod = v;
    }

    /**
    * sysCoding
    */
    public Long getSysCoding() {
    return this.sysCoding;
    }

    public void setSysCoding(Long v) {
    this.sysCoding = v;
    }

    /**
    * Код
    */
    public String getCod() {
    return this.cod;
    }

    public void setCod(String v) {
    this.cod = v;
    }

    }
