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
public class BFilterTest {

    private String correctDir = "src/correctOutput/";
    private String outDir = "src/testOutput/";
    private String inDir = "src/tables/";

    @Test
    public void BFilterTestFull () throws Exception{
        Utility.redirectStdOut(outDir+"bFilterTestFullOut.txt");

        ThreadList.init();
        
        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(inDir+"client.txt", c1);
        Connector a_sink = new Connector("a_sink");
        Connector m_filter = new Connector("m_filter");
        Bloom b = new Bloom(c1, 0, a_sink, m_filter);
        Sink sink_a = new Sink(a_sink);

        Connector c2 = new Connector("input2");
        ReadRelation r2 = new ReadRelation(inDir+"client.txt", c2);
        Connector b_out = new Connector("b_out");
        BFilter bfilter = new BFilter(m_filter, c2, 0, b_out);
        
        Print p = new Print(b_out);

        ThreadList.run(p);

        Utility.validate(outDir+"bFilterTestFullOut.txt", correctDir+"bFilterTestFullOut.txt",false);
    }

    @Test
    public void BFilterTestHalf () throws Exception{
        Utility.redirectStdOut(outDir+"bFilterTestHalfOut.txt");

        ThreadList.init();
        
        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(inDir+"client.txt", c1);
        Connector a_sink = new Connector("a_sink");
        Connector m_filter = new Connector("m_filter");
        Bloom b = new Bloom(c1, 0, a_sink, m_filter);
        Sink sink_a = new Sink(a_sink);

        Connector c2 = new Connector("input2");
        ReadRelation r2 = new ReadRelation(inDir+"client2.txt", c2);
        Connector b_out = new Connector("b_out");
        BFilter bfilter = new BFilter(m_filter, c2, 0, b_out);
        
        Print p = new Print(b_out);

        ThreadList.run(p);

        Utility.validate(outDir+"bFilterTestHalfOut.txt", correctDir+"bFilterTestHalfOut.txt",false);
    }

    @Test
    public void BFilterTestNone () throws Exception{
        Utility.redirectStdOut(outDir+"bFilterTestNoneOut.txt");

        ThreadList.init();
        
        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(inDir+"client.txt", c1);
        Connector a_sink = new Connector("a_sink");
        Connector m_filter = new Connector("m_filter");
        Bloom b = new Bloom(c1, 0, a_sink, m_filter);
        Sink sink_a = new Sink(a_sink);

        Connector c2 = new Connector("input2");
        ReadRelation r2 = new ReadRelation(inDir+"client3.txt", c2);
        Connector b_out = new Connector("b_out");
        BFilter bfilter = new BFilter(m_filter, c2, 0, b_out);
        
        Print p = new Print(b_out);

        ThreadList.run(p);

        Utility.validate(outDir+"bFilterTestNoneOut.txt", correctDir+"bFilterTestNoneOut.txt",false);
    }
}
