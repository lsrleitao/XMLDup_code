package BayesianNetwork;

import java.util.List;

public class ValueNode extends
        ObjBayes {

    private String _formula;
    private String _nodeName;
    private List<ObjBayes> _next;
    public static final int Hidden = 1;
    public static final int MultipleChildren = 2;
    public static final int Generic = 0;
    private int _nodeType;

    public ValueNode(String nodeName, List<ObjBayes> next, String formula, int nodeType) {
        super(next, formula);
        this._nodeName = nodeName;
        this._next = next;
        this._formula = formula;
        this._nodeType = nodeType;
    }

    public String getFormula() {

        return this._formula;
    }

    public List<ObjBayes> getNext() {
        return this._next;
    }

    public String getNodeName() {
        return this._nodeName;
    }

    public boolean isHidden() {
        if (this._nodeType == Hidden)
            return true;
        return false;
    }

    public boolean isMultipleChildren() {
        if (this._nodeType == MultipleChildren)
            return true;
        return false;
    }

    public boolean isGenericNode() {
        if (this._nodeType == Generic)
            return true;
        return false;
    }

}
