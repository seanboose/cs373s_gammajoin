/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import RegTest.Utility;
import basicConnector.Connector;
import gammaJoin.HSplit;
import gammaJoin.Merge;
import gammaJoin.Print;
import gammaJoin.ReadRelation;
import gammaJoin.Sink;
import gammaSupport.ThreadList;
import org.junit.Test;

/**
 *
 * @author terriBoose
 */
public class MergeTest {
    
    private String correctDir = "src/correctOutput/";
    private String outDir = "src/testOutput/";
    private String inDir = "src/tables/";
    
    @Test
    public void MergeTest () throws Exception {
        Utility.redirectStdOut(outDir+"mergeOut.txt");
        ThreadList.init();
        
        Connector[] ins = new Connector[4];
        ins[0] = new Connector("input0");
        ins[1] = new Connector("input1");
        ins[2] = new Connector("input2");
        ins[3] = new Connector("input3");
        ReadRelation r0 = new ReadRelation(inDir+"clientSplit0.txt", ins[0]); 
        ReadRelation r1 = new ReadRelation(inDir+"clientSplit1.txt", ins[1]); 
        ReadRelation r2 = new ReadRelation(inDir+"clientSplit2.txt", ins[2]); 
        ReadRelation r3 = new ReadRelation(inDir+"clientSplit3.txt", ins[3]); 
        Connector out = new Connector("out");
        
        Merge merge = new Merge(ins, out);
        Print p = new Print(out);
        ThreadList.run(p);
        
        Utility.validate(outDir+"mergeOut.txt", correctDir+"mergeOut.txt",false);
    }
}
