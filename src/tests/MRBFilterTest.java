/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import RegTest.Utility;
import basicConnector.Connector;
import gammaJoin.BFilter;
import gammaJoin.Bloom;
import gammaJoin.Print;
import gammaJoin.ReadRelation;
import gammaJoin.Sink;
import gammaSupport.ThreadList;
import org.junit.Test;
import pipeFilter.MRBFilter;

/**
 *
 * @author terriBoose
 */
public class MRBFilterTest {

    private String correctDir = "src/correctOutput/";
    private String outDir = "src/testOutput/";
    private String inDir = "src/tables/";

    @Test
    public void MRBFilterTestFull () throws Exception{
        Utility.redirectStdOut(outDir+"mRBFilterTestFullOut.txt");

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
        MRBFilter bfilter = new MRBFilter(m_filter, c2, 0, b_out);
        
        Print p = new Print(b_out);

        ThreadList.run(p);

        Utility.validate(outDir+"mRBFilterTestFullOut.txt", correctDir+"mRBFilterTestFullOut.txt",false);
    }

    @Test
    public void MRBFilterTestHalf () throws Exception{
        Utility.redirectStdOut(outDir+"mRBFilterTestHalfOut.txt");

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
        MRBFilter bfilter = new MRBFilter(m_filter, c2, 0, b_out);
        
        Print p = new Print(b_out);

        ThreadList.run(p);

        Utility.validate(outDir+"mRBFilterTestHalfOut.txt", correctDir+"mRBFilterTestHalfOut.txt",false);
    }

    @Test
    public void MRBFilterTestNone () throws Exception{
        Utility.redirectStdOut(outDir+"mRBFilterTestNoneOut.txt");

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
        MRBFilter bfilter = new MRBFilter(m_filter, c2, 0, b_out);
        
        Print p = new Print(b_out);

        ThreadList.run(p);

        Utility.validate(outDir+"mRBFilterTestNoneOut.txt", correctDir+"mRBFilterTestNoneOut.txt",false);
    }
}
