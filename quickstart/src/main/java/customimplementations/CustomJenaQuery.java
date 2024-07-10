package customimplementations;

import graph.jena.datatypes.JenaGraphOrBindings;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.*;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetImpl;
import org.apache.jena.sparql.core.ResultBinding;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemory;
import org.apache.jena.sparql.engine.binding.Binding;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;

import java.util.ArrayList;
import java.util.List;

/*
 * This class shows how to create a custom Relation To Relation operator.
 * Such operator takes as input an element of type R and applies the desired operations to return a new element of the same type.
 *
 * In order to properly implement this operator, you should provide at least two fields:
 * 1- A name for the result of this operator (All the intermediate results of R2R computations must have a name)
 * 2- A list of names (max 2) on which this operand should be applied. Basically, the names of the results of the operations applied before this one.
 *    To clarify the last point, assume you have a chain of R2R operators (a Directed Acyclic Graph): each node of the DAG must know the name
 *    of its operands, which are the results of the previous DAG node. If we have an R2R operator that performs a filter and another one
 *    that performs a projection, the latter should know the name of the partial result that comes from the filter
 *    (e.g. filter_partial_result), in order to be properly linked.
 *    Right now, an operator must either be unary or binary (this one is binary), so the maximum number of operands supported is two.
 *
 *
 * The eval method contains the logic of the operator, it takes as input one (or two) operands and performs the specified operations,
 * returning a single element of type R (JenaGraphOrBinding in this specific case)
 *
 * In this particular implementation, we take as input a query in String format, and use the API that Jena provides to parse it
 * and perform the query. It is of course possible to not rely on any external APIs and implement custom algorithms to perform the queries.
 */

public class CustomJenaQuery implements RelationToRelationOperator<JenaGraphOrBindings> {


    private String query;

    private List<String> tvgNames;

    private String resName;

    public CustomJenaQuery(String query, List<String> tvgNames, String resName) {
        this.query = query;
        this.tvgNames = tvgNames;
        this.resName = resName;

    }

    @Override
    public JenaGraphOrBindings eval(List<JenaGraphOrBindings> datasets) {

        JenaGraphOrBindings dataset = datasets.get(0);
        Query q = QueryFactory.create(query);
        q.getProjectVars();
        Node aDefault = NodeFactory.createURI("default");
        DatasetGraph dg = new DatasetGraphInMemory();
        dg.addGraph(aDefault, dataset.getContent());

        QueryExecution queryExecution = QueryExecutionFactory.create(q, DatasetImpl.wrap(dg));
        ResultSet resultSet = queryExecution.execSelect();

        List<Binding> res = new ArrayList<>();

        while (resultSet.hasNext()) {

            ResultBinding rb = (ResultBinding) resultSet.next();
            res.add(rb.getBinding());

        }

        dataset.setResult(res);
        return dataset;

    }


    @Override
    public List<String> getTvgNames() {
        return tvgNames;
    }

    @Override
    public String getResName() {
        return resName;
    }
}
