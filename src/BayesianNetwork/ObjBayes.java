package BayesianNetwork;

import java.util.List;

public class ObjBayes {

    private List<ObjBayes> _childNodes;
    private String _formula;
    private double _pruningFactor;

    public ObjBayes(List<ObjBayes> childNodes, String formula) {
        this._childNodes = childNodes;
        this._formula = formula;
    }

    public List<ObjBayes> getChildNodes() {
        return this._childNodes;
    }

    public void setChildNodes(List<ObjBayes> cn) {
        this._childNodes = cn;
    }

    public String getFormula() {
        return this._formula;
    }

    public boolean isFinalNode() {
       return this.getFormula().equals("FINAL");
    }
    
    public void setPruningFactor(double pf){
        this._pruningFactor = pf;
    }
    
    public double getPruningFactor(){
        return this._pruningFactor;
    }

}
