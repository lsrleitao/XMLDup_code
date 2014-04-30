package BayesianNetwork;

import java.util.List;

public class ValueLeafNode extends
        ObjBayes {

    private float _simCut;
    private String _simMeasure;
    private String _nodeContent1;
    private String _nodeContent2;
    private double _pruningFactor;

    public ValueLeafNode(List<ObjBayes> next,
                         float simCut,
                         String simMeasure,
                         String nodeContent1,
                         String nodeContent2,
                         String formula,
                         double pruningFactor) {
        super(next, formula);
        this._simCut = simCut;
        this._simMeasure = simMeasure;
        this._nodeContent1 = nodeContent1;
        this._nodeContent2 = nodeContent2;
        this._pruningFactor = pruningFactor;
    }

    public float getSimCut() {
        return this._simCut;
    }

    public String getSimMeasure() {
        return this._simMeasure;
    }

    public String getContent1() {
        return this._nodeContent1;
    }

    public String getContent2() {
        return this._nodeContent2;
    }
    
    public double getPruningFactor() {
        return this._pruningFactor;
    }

}
