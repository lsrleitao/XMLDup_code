package BayesianNetwork;

import java.util.List;

public class ChildSetNode extends
        ObjBayes {

    private String _formula;
    private List<ObjBayes> _next;

    public ChildSetNode(List<ObjBayes> next, String formula) {
        super(next, formula);
        this._next = next;
        this._formula = formula;
    }

    public String getFormula() {
        return this._formula;
    }

    public List<ObjBayes> getNext() {
        return this._next;
    }
}
