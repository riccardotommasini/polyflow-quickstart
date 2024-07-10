package examples;

import customimplementations.CustomTumblingWindow;
import graph.jena.datatypes.JenaGraphOrBindings;
import graph.jena.operatorsimpl.r2r.jena.FullQueryUnaryJena;
import graph.jena.operatorsimpl.r2s.RelationToStreamOpImpl;
import graph.jena.sds.SDSJena;
import graph.jena.stream.JenaBindingStream;
import graph.jena.stream.JenaStreamGenerator;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.compose.Union;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.graph.GraphFactory;
import org.streamreasoning.rsp4j.api.coordinators.ContinuousProgram;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.rsp4j.api.querying.Task;
import org.streamreasoning.rsp4j.api.querying.TaskImpl;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.report.ReportImpl;
import org.streamreasoning.rsp4j.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.api.secret.time.TimeImpl;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import shared.contentimpl.factories.AccumulatorContentFactory;
import shared.operatorsimpl.r2r.DAG.DAGImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * This class contains an example with a single input stream and a custom Stream To Relation Operator (S2R).
 * An S2R operator controls the windowing logic (Width, Sliding, Eviction, Reporting).
 *
 */
public class CustomS2ROperator {

        public static void main(String[] args) throws InterruptedException {

            /*------------Input and Output Stream definitions------------*/

            // Define a generator to create input graphs
            JenaStreamGenerator generator = new JenaStreamGenerator();
            // Define a single input stream
            DataStream<Graph> inputStreamColors = generator.getStream("http://test/stream1");
            // define an output stream
            JenaBindingStream outStream = new JenaBindingStream("out");


            /*------------Window Properties------------*/

            // Window properties (report, tick)
            Report report = new ReportImpl();
            report.add(new OnWindowClose());

            //Time object used to represent the time in our application
            Time instance = new TimeImpl(0);


            /*------------Window Content------------*/

            //Entity that represents the window content. In particular, we create an instance that represents an empty content
            JenaGraphOrBindings emptyContent = new JenaGraphOrBindings(GraphFactory.createGraphMem());

        /*
         * The customization of the Content will be explained in another example, right now assume it's just an object that accumulates
         * what enters a window.
        */
            ContentFactory<Graph, Graph, JenaGraphOrBindings> accumulatorContentFactory = new AccumulatorContentFactory<>(
                    (g) -> g,
                    (g) -> new JenaGraphOrBindings(g),
                    (r1, r2) -> new JenaGraphOrBindings(new Union(r1.getContent(), r2.getContent())),
                    emptyContent
            );


            /*------------S2R, R2R and R2S Operators------------*/

            /*
             * Use a custom implementation of the Stream To Relation Operator.
             * The source file can be found in the 'customimplementations' directory, along with the details of each parameter.
             */

            StreamToRelationOperator<Graph, Graph, JenaGraphOrBindings> s2rOp_one =
                    new CustomTumblingWindow<>(
                            instance,
                            "TumblingWindow",
                            accumulatorContentFactory,
                            report,
                            1000);

            //Define a simple Relation To Relation operator to extract every graph in the window
            RelationToRelationOperator<JenaGraphOrBindings> r2rOp1 = new FullQueryUnaryJena("SELECT * WHERE {GRAPH ?g {?s ?p ?o }}", Collections.singletonList(s2rOp_one.getName()), "partial_1");

            //Relation to Stream operator, used to transform the result of a query (type R) to a stream of output objects (type O)
            RelationToStreamOperator<JenaGraphOrBindings, Binding> r2sOp = new RelationToStreamOpImpl();


            /*------------Task definition------------*/

            //Define the Tasks, each of which represent a query
            Task<Graph, Graph, JenaGraphOrBindings, Binding> task = new TaskImpl<>();
            task = task.addS2ROperator(s2rOp_one, inputStreamColors)
                    .addR2ROperator(r2rOp1)
                    .addR2SOperator(r2sOp)
                    .addDAG(new DAGImpl<>())
                    .addSDS(new SDSJena())
                    .addTime(instance);
            task.initialize();


            List<DataStream<Graph>> inputStreams = new ArrayList<>();
            inputStreams.add(inputStreamColors);

            List<DataStream<Binding>> outputStreams = new ArrayList<>();
            outputStreams.add(outStream);

            /*------------Continuous Program definition------------*/

            //Define the Continuous Program, which acts as the coordinator of the whole system
            ContinuousProgram<Graph, Graph, JenaGraphOrBindings, Binding> cp = new ContinuousProgram<>();
            cp.buildTask(task, inputStreams, outputStreams);


            /*------------Output Stream consumer------------*/

            outStream.addConsumer((out, el, ts) -> System.out.println(el + " @ " + ts));

            generator.startStreaming();
            Thread.sleep(20_000);
            generator.stopStreaming();
        }
}
