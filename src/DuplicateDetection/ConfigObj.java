package DuplicateDetection;

import java.util.List;
import java.util.ArrayList;

public class ConfigObj {

    private String _nodeName = null;
    private List<ConfigObj> _childrenList = new ArrayList<ConfigObj>();
    private int _useFlag = -1; // indicates if that node is to be considered in
    // the evaluation of the object
    private float _defaultProb = -1;
    private String _simMeasure = null;
    private float _simCut = -1;
    private String[] _attributes;
    private boolean _onlyAttributes;
    private int _nodeType = -1; // 0 se for folha, 1 se for no interno.
    private double _pruningFactor = -1;
    
    public ConfigObj(String nodeName, int useFlag, float defaultProb, float simCut) {

        _nodeName = nodeName;
        _useFlag = useFlag;
        _defaultProb = defaultProb;
    }

    public ConfigObj() {

    }

    public String getNodeName() {

        return _nodeName;
    }

    public List<ConfigObj> getChildrenList() {

        return _childrenList;
    }

    public boolean isLeaf() {

        return _nodeType == 0;
    }

    public boolean isInnerNode() {

        return _nodeType == 1;
    }

    public boolean hasAttributes() {

        if (_attributes.length == 0)
            return false;
        else
            return true;
    }

    // no folha em que os filhos nao foram marcados para serem avaliados
    public boolean isDeadEndNode() {
        if (_nodeType == 1 && _childrenList.size() == 0)
            return true;
        else
            return false;
    }

    public int getUseFlag() {

        return _useFlag;
    }

    public double getDefaultProb() {

        return _defaultProb;
    }

    public String getSimMeasure() {

        return _simMeasure;
    }

    public float getSimCut() {

        return _simCut;
    }

    public String[] getAttributes() {

        return _attributes;
    }
    
    public double getPruningFactor() {

        return _pruningFactor;
    }

    public boolean considerOnlyAttributes() {

        return _onlyAttributes;
    }

    //
    public void setNodeName(String nodeName) {

        _nodeName = nodeName;
    }

    public void setChildrenList(List<ConfigObj> childrenList) {

        _childrenList = childrenList;
    }

    public void setUseFlag(int useFlag) {

        _useFlag = useFlag;
    }

    public void setDefaultProb(float defaultProb) {

        _defaultProb = defaultProb;
    }

    public void setSimMeasure(String simMeasure) {

        _simMeasure = simMeasure;
    }

    public void setSimCut(float simCut) {

        _simCut = simCut;
    }

    public void setAttributes(String[] attr) {

        _attributes = attr;
    }

    public void setOnlyAttributes(boolean onlyAttributes) {

        _onlyAttributes = onlyAttributes;
    }

    public void setNodeType(int nt) {
        _nodeType = nt;
    }
    
    public void setPruningFactor(double pf) {

        _pruningFactor = pf;
    }
}
