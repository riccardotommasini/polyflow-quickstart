package graph.neo.sds;

import graph.jena.datatypes.JenaGraphOrBindings;
import graph.neo.stream.data.PGraph;
import graph.neo.stream.data.PGraphOrTable;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVaryingFactory;

public class TimeVaryingFactoryPGraph implements TimeVaryingFactory<PGraphOrTable> {

    @Override
    public TimeVarying<PGraphOrTable> create(StreamToRelationOperator<?, ?, PGraphOrTable> s2r, String name) {
        return new TimeVaryingObject<>(s2r, RDFUtils.createIRI(name));
    }
}
