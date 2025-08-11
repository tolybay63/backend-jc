/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Встроенные коды
* <p>
    * table: SysCod
    */
    public class SysCod_dto {

    private Long id;

    @FieldProps(size = 30)
    private String cod;

    private Long linkType;

    private Long linkId;

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
    * Код
    */
    public String getCod() {
    return this.cod;
    }

    public void setCod(String v) {
    this.cod = v;
    }

    /**
    * linkType
    */
    public Long getLinkType() {
    return this.linkType;
    }

    public void setLinkType(Long v) {
    this.linkType = v;
    }

    /**
    * linkId
    */
    public Long getLinkId() {
    return this.linkId;
    }

    public void setLinkId(Long v) {
    this.linkId = v;
    }

    }
