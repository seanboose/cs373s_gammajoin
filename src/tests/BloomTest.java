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
public class BloomTest {
    
    private String correctDir = "src/correctOutput/";
    private String outDir = "src/testOutput/";
    private String inDir = "src/tables/";

    @Test
    public void BloomTestM () throws Exception{
        Utility.redirectStdOut(outDir+"bloomOutM.txt");
        
        ThreadList.init();
        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(inDir+"client.txt", c1);
        Connector a_print = new Connector("a_print");
        Connector m_print = new Connector("m_print");
        Bloom b = new Bloom(c1, 0, a_print, m_print);
        Sink sink_tuples = new Sink(a_print);
        PrintMap print_map = new PrintMap(m_print);
        
        ThreadList.run(print_map);
        
        Utility.validate(outDir+"bloomOutM.txt", correctDir+"bloomOutM.txt",false);
    }

    @Test
    public void BloomTestA () throws Exception{
        Utility.redirectStdOut(outDir+"bloomOutA.txt");
        
        ThreadList.init();
        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(inDir+"client.txt", c1);
        Connector a_print = new Connector("a_print");
        Connector m_print = new Connector("m_print");
        Bloom b = new Bloom(c1, 0, a_print, m_print);
        Print print_tuples = new Print(a_print);
        Sink sink_map = new Sink(m_print);
        
        ThreadList.run(print_tuples);

        Utility.validate(outDir+"bloomOutA.txt", correctDir+"bloomOutA.txt",false);
    }
}
