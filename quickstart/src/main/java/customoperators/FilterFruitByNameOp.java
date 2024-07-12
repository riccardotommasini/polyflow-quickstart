package customoperators;

import customdatatypes.FruitBasket;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;

import java.util.List;

public class FilterFruitByNameOp implements RelationToRelationOperator<FruitBasket> {
    @Override
    public FruitBasket eval(List<FruitBasket> datasets) {
        return null;
    }

    @Override
    public List<String> getTvgNames() {
        return null;
    }

    @Override
    public String getResName() {
        return null;
    }
}
