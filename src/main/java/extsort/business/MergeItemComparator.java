package extsort.business;

import java.util.Comparator;

public class MergeItemComparator implements Comparator<MergeItem> {

    @Override
    public int compare(MergeItem o1, MergeItem o2) {
        return o1.getValue().compareTo(o2.getValue());
    }
}
