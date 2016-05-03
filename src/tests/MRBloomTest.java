/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests;

import RegTest.Utility;
import basicConnector.Connector;
import gammaJoin.Bloom;
import gammaJoin.Print;
import gammaJoin.PrintMap;
import gammaJoin.ReadRelation;
import gammaJoin.Sink;
import gammaSupport.ThreadList;
import org.junit.Test;
import pipeFilter.MRBloom;


/**
 *
 * @author terriBoose
 */
public class MRBloomTest {
    
    private String correctDir = "src/correctOutput/";
    private String outDir = "src/testOutput/";
    private String inDir = "src/tables/";

    @Test
    public void MRBloomTestM () throws Exception{
        Utility.redirectStdOut(outDir+"mRBloomOutM.txt");
        
        ThreadList.init();
        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(inDir+"client.txt", c1);
        Connector a_print = new Connector("a_print");
        Connector m_print = new Connector("m_print");
        MRBloom b = new MRBloom(c1, 0, a_print, m_print);
        Sink sink_tuples = new Sink(a_print);
        PrintMap print_map = new PrintMap(m_print);
        
        ThreadList.run(print_map);
        
        Utility.validate(outDir+"mRBloomOutM.txt", correctDir+"mRBloomOutM.txt",false);
    }

    @Test
    public void MRBloomTestA () throws Exception{
        Utility.redirectStdOut(outDir+"mRBloomOutA.txt");
        
        ThreadList.init();
        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(inDir+"client.txt", c1);
        Connector a_print = new Connector("a_print");
        Connector m_print = new Connector("m_print");
        MRBloom b = new MRBloom(c1, 0, a_print, m_print);
        Print print_tuples = new Print(a_print);
        Sink sink_map = new Sink(m_print);
        
        ThreadList.run(print_tuples);

        Utility.validate(outDir+"mRBloomOutA.txt", correctDir+"mRBloomOutA.txt",false);
    }
}
