/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import RegTest.Utility;
import basicConnector.Connector;
import gammaJoin.HSplit;
import gammaJoin.Print;
import gammaJoin.PrintMap;
import gammaJoin.ReadRelation;
import gammaJoin.Sink;
import gammaJoin.SplitM;
import gammaSupport.Relation;
import gammaSupport.ThreadList;
import org.junit.Test;

/**
 *
 * @author terriBoose
 */
public class SplitMTest {
    
    private String correctDir = "src/correctOutput/";
    private String outDir = "src/testOutput/";
    private String inDir = "src/tables/";

    public void SplitMRunner (int i) throws Exception {
        Utility.redirectStdOut(outDir+"splitMOut"+i+".txt");
        ThreadList.init();

        String bmap = "tttttttffffffftftftftttffttf";
        // ttttttt
        // fffffff
        // tftftft
        // ttffttf

        Connector in = new Connector("input");
        in.setRelation(Relation.dummy);
        Connector[] outs = new Connector[4];
        outs[0] = new Connector("out0");
        outs[1] = new Connector("out1");
        outs[2] = new Connector("out2");
        outs[3] = new Connector("out3");
        
        SplitM hsplit = new SplitM(in, outs);
        
        Sink s0 = new Sink(outs[0]);
        Sink s1 = new Sink(outs[1]);
        Sink s2 = new Sink(outs[2]);
        Sink s3 = new Sink(outs[3]);
        
        PrintMap p = new PrintMap(outs[i]);
        
        in.getWriteEnd().putNextString(bmap);
        
        ThreadList.run(p);

        Utility.validate(outDir+"splitMOut"+i+".txt", correctDir+"splitMOut"+i+".txt",false);
    }
    
    @Test
    public void SplitMTest() throws Exception{
        SplitMRunner(0);
        SplitMRunner(1);
        SplitMRunner(2);
        SplitMRunner(3);
    }
}
