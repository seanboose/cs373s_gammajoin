/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import RegTest.Utility;
import basicConnector.*;
import gammaJoin.*;
import gammaSupport.*;
import org.junit.Test;

/**
 *
 * @author terriBoose
 */
public class HSplitTest {

    private String correctDir = "src/correctOutput/";
    private String outDir = "src/testOutput/";
    private String inDir = "src/tables/";

    @Test
    public void HSplitTest0 () throws Exception {
        Utility.redirectStdOut(outDir+"hSplit0Out.txt");
        ThreadList.init();

        Connector in = new Connector("input");
        ReadRelation r1 = new ReadRelation(inDir+"client.txt", in); 
        Connector[] outs = new Connector[4];
        outs[0] = new Connector("out0");
        outs[1] = new Connector("out1");
        outs[2] = new Connector("out2");
        outs[3] = new Connector("out3");
        
        HSplit hsplit = new HSplit(in, outs, 0);
        
        Print p0 = new Print(outs[0]);
        Sink s1 = new Sink(outs[1]);
        Sink s2 = new Sink(outs[2]);
        Sink s3 = new Sink(outs[3]);
        
        ThreadList.run(p0);

        Utility.validate(outDir+"hSplit0Out.txt", correctDir+"hSplit0Out.txt",false);
    }

    @Test
    public void HSplitTest1 () throws Exception {
        Utility.redirectStdOut(outDir+"hSplit1Out.txt");
        ThreadList.init();

        Connector in = new Connector("input");
        ReadRelation r1 = new ReadRelation(inDir+"client.txt", in); 
        Connector[] outs = new Connector[4];
        outs[0] = new Connector("out0");
        outs[1] = new Connector("out1");
        outs[2] = new Connector("out2");
        outs[3] = new Connector("out3");
        
        HSplit hsplit = new HSplit(in, outs, 0);
        
        Sink s0 = new Sink(outs[0]);
        Print p1 = new Print(outs[1]);
        Sink s2 = new Sink(outs[2]);
        Sink s3 = new Sink(outs[3]);
        
        ThreadList.run(p1);

        Utility.validate(outDir+"hSplit1Out.txt", correctDir+"hSplit1Out.txt",false);
    }

    @Test
    public void HSplitTest2 () throws Exception {
        Utility.redirectStdOut(outDir+"hSplit2Out.txt");
        ThreadList.init();

        Connector in = new Connector("input");
        ReadRelation r1 = new ReadRelation(inDir+"client.txt", in); 
        Connector[] outs = new Connector[4];
        outs[0] = new Connector("out0");
        outs[1] = new Connector("out1");
        outs[2] = new Connector("out2");
        outs[3] = new Connector("out3");
        
        HSplit hsplit = new HSplit(in, outs, 0);
        
        Sink s0 = new Sink(outs[0]);
        Sink s1 = new Sink(outs[1]);
        Print p2 = new Print(outs[2]);
        Sink s3 = new Sink(outs[3]);
        
        ThreadList.run(p2);

        Utility.validate(outDir+"hSplit2Out.txt", correctDir+"hSplit2Out.txt",false);
    }

    @Test
    public void HSplitTest3 () throws Exception {
        Utility.redirectStdOut(outDir+"hSplit3Out.txt");
        ThreadList.init();

        Connector in = new Connector("input");
        ReadRelation r1 = new ReadRelation(inDir+"client.txt", in); 
        Connector[] outs = new Connector[4];
        outs[0] = new Connector("out0");
        outs[1] = new Connector("out1");
        outs[2] = new Connector("out2");
        outs[3] = new Connector("out3");
        
        HSplit hsplit = new HSplit(in, outs, 0);
        
        Sink s0 = new Sink(outs[0]);
        Sink s1 = new Sink(outs[1]);
        Sink s2 = new Sink(outs[2]);
        Print p3 = new Print(outs[3]);
        
        ThreadList.run(p3);

        Utility.validate(outDir+"hSplit3Out.txt", correctDir+"hSplit3Out.txt",false);
    }
}
