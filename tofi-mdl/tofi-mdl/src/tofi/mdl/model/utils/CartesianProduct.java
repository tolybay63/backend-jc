package tofi.mdl.model.utils;

import java.util.ArrayList;
import java.util.List;

public class CartesianProduct {
    /**
     * Векартово произведение списка списков типа <T>
     *
     * @lists - список списков типа <T> : lists
     */
    static public <T> List<List<T>> result(List<List<T>> lists) {
        List<List<T>> resultLists = new ArrayList<>();
        if (lists.isEmpty()) {
            resultLists.add(new ArrayList<T>());
            return resultLists;
        } else {
            List<T> firstList = lists.get(0);
            List<List<T>> remainingLists = result(lists.subList(1, lists.size()));
            for (T condition : firstList) {
                for (List<T> remainingList : remainingLists) {
                    ArrayList<T> resultList = new ArrayList<T>();
                    resultList.add(condition);
                    resultList.addAll(remainingList);
                    resultLists.add(resultList);
                }
            }
        }
        return resultLists;
    }


}
