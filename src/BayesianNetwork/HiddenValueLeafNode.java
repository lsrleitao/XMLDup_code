package BayesianNetwork;

import java.util.List;

public class HiddenValueLeafNode extends
        ObjBayes {

    private float _simCut;
    private double _defaultProb;

    public HiddenValueLeafNode(List<ObjBayes> next, float simCut, double defaultProb, String formula) {
        super(next, formula);
        this._simCut = simCut;
        this._defaultProb = defaultProb;
    }

    public float getSimCut() {
        return this._simCut;
    }

    public double getDefaultProb() {
        return this._defaultProb;
    }

}
