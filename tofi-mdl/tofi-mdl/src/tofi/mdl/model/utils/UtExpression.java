package tofi.mdl.model.utils;

import jandcode.commons.UtString;
import net.objecthunter.exp4j.ExpressionBuilder;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UtExpression {
    String exprersion;
    Set<String> varsName;

    public Set<String> getVarsName() {
        return varsName;
    }

    public UtExpression(String exprersion) {
        this.exprersion = exprersion;
        this.varsName = getVariablesName();
    }

    //
    private Set<String> getVariablesName() {
        Set<String> delims = new HashSet<>(Set.of(
                "\\+", "\\-", "\\*", "\\/", "\\(", "\\)", "sin", "cos", "sqrt", "pow", "log", "ceil", "abs"
        ));

        final String[] f1 = {exprersion.replaceAll(" ", "")};

        delims.forEach(it -> {
            f1[0] = f1[0].replaceAll(it, "~");
        });

        String[] arr = f1[0].split("~");

        Set<String> vars = new HashSet<>();
        for (String it : arr) {
            if (!it.isEmpty() && !UtString.isNumber(it)) {
                vars.add(it);
            }
        }
        return vars;
    }

    public double getResult(Map<String, Double> varValues) {
        double r = 0;
        try {
            r = new ExpressionBuilder(exprersion)
                    .variables(varsName)
                    .build()
                    .setVariables(varValues)
                    .evaluate();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }

        return r;

    }


}
