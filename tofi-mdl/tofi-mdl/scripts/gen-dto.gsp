<%@ page import="jandcode.core.dbm.dbstruct.*; jandcode.jc.*;jandcode.commons.*;jandcode.core.dbm.domain.*" %>
<%
    GspScript th = this

    String pakName = th.args.pakName
    DomainDbUtils dbUtils = th.args.dbUtils

    def javaType = { Field field ->
        String t = field.storeDataType.name
        if (t == "blob") return "byte[]"
        else if (t == "boolean") return "Boolean"
        else if (t == "date") return "XDate"
        else if (t == "datetime") return "XDateTime"
        else if (t == "double") return "Double"
        else if (t == "int") return "Integer"
        else if (t == "long") return "Long"
        else if (t == "string") return "String"
        else if (t == "rawstring") return "String"
        else "Object"
    }

    for (domain in dbUtils.domains) {
        def className = UtString.capFirst(UtString.camelCase(domain.dbTableName)) + "_dto"
        th.changeFile("${className}.java")
        def hasDate = domain.fields.any { it.storeDataType.name.startsWith('date') }
        def hasFP = domain.fields.any { it.size > 0 || it.dict }
%>
/* THIS FILE GENERATED! NOT MODIFY! */
package ${pakName};

<% if (hasDate) { %>
import jandcode.commons.datetime.*;
<% } %>
<% if (hasFP) { %>
import jandcode.commons.reflect.*;
<% } %>

/**
 * ${domain.title ?: domain.name}
* <p>
    * table: ${domain.dbTableName}
    */
    public class ${className} {
    <%
            for (field in domain.fields) {
                def fp = []
                if (field.dict) fp.add("dict = \"${field.dict}\"")
                if (field.size > 0) fp.add("size = ${field.size}")
    %>

    <% if (fp.size() > 0) { %>
    @FieldProps(${fp.join(', ')})
    <% } %>
    private ${javaType(field)} ${field.name};
    <%
            }
    %>

    <% for (field in domain.fields) { %>
    /**
    * ${field.title ?: field.name}
    */
    public ${javaType(field)} get${UtString.capFirst(field.name)}() {
    return this.${field.name};
    }

    public void set${UtString.capFirst(field.name)}(${javaType(field)} v) {
    this.${field.name} = v;
    }

    <% } %>
    }
<% } %>