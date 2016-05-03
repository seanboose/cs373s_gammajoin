/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import RegTest.Utility;
import basicConnector.Connector;
import gammaJoin.Merge;
import gammaJoin.MergeM;
import gammaJoin.Print;
import gammaJoin.PrintMap;
import gammaJoin.ReadRelation;
import gammaSupport.Relation;
import gammaSupport.ThreadList;
import org.junit.Test;

/**
 *
 * @author terriBoose
 */
public class MergeMTest {
    
    private String correctDir = "src/correctOutput/";
    private String outDir = "src/testOutput/";
    private String inDir = "src/tables/";

    @Test
    public void MergeMTest1 () throws Exception {
        Utility.redirectStdOut(outDir+"mergeMOut.txt");
        
        String bmap0 = "ffftffffffftffffffffffffffft";
        String bmap1 = "tfffftffffffffftffffffffffff";
        String bmap2 = "ftffffftffffffffffftffffffff";
        String bmap3 = "fftfffffftffffffffffffftffff";

        ThreadList.init();
        
        Connector[] ins = new Connector[4];
        ins[0] = new Connector("input0");
        ins[0].setRelation(Relation.dummy);
        ins[1] = new Connector("input1");
        ins[1].setRelation(Relation.dummy);
        ins[2] = new Connector("input2");
        ins[2].setRelation(Relation.dummy);
        ins[3] = new Connector("input3");
        ins[3].setRelation(Relation.dummy);
        Connector out = new Connector("out");
        
        MergeM merge = new MergeM(ins, out);
        PrintMap p = new PrintMap(out);
        
        ins[0].getWriteEnd().putNextString(bmap0);
        ins[1].getWriteEnd().putNextString(bmap1);
        ins[2].getWriteEnd().putNextString(bmap2);
        ins[3].getWriteEnd().putNextString(bmap3);
        
        ThreadList.run(p);
        
        Utility.validate(outDir+"mergeMOut.txt", correctDir+"mergeMOut.txt",false);
    }
}
