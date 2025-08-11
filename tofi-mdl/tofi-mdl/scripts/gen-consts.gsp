<%@ page import="jandcode.core.store.*; jandcode.core.dbm.dbstruct.*; jandcode.jc.*;jandcode.commons.*;jandcode.core.dbm.domain.*" %>
<%
    GspScript th = this

    String pakName = th.args.pakName
    DomainDbUtils dbUtils = th.args.dbUtils
    List<Store> stores = th.args.stores

    // java
    for (store in stores) {
        def className = store.name + "_consts"
        th.changeFile("${className}.java")
        def domain = dbUtils.domains.get(store.name)
%>
/* THIS FILE GENERATED! NOT MODIFY! */
package ${pakName};

/**
 * ${domain.title ?: domain.name}
* <p>
    * table: ${domain.dbTableName}
    */
    public class ${className} {
    <%
            for (rec in store) {
                String title = rec.getString('text')
                long id = rec.getLong('id')
                String code = rec.getString('code')
                if (!code) {
                    code = 'code_' + id
                }
    %>

    /**
    * ${title} (id=${id})
    */
    public static final long ${code} = ${id};
    <% } %>
    }
    <% } %>
    <%
        // javascript
        th.changeFile("all-consts.js")
    %>
    export default {
    <%
        for (store in stores) {
            def domain = dbUtils.domains.get(store.name)
    %>
    /**
    * ${domain.title ?: domain.name}
    *

<p>
    * table: ${domain.dbTableName}
    */
    ${domain.dbTableName}: {
    <%
            for (rec in store) {
                String title = rec.getString('text')
                long id = rec.getLong('id')
                String code = rec.getString('code')
                if (!code) {
                    code = 'code_' + id
                }
    %>
    /**
    * ${title} (id=${id})
    */
    ${code}: ${id},

    <% } %>
    },
    <% } %>
    }