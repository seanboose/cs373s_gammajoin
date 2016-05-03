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
import gammaJoin.HJoin;
import gammaJoin.Print;
import gammaJoin.ReadRelation;
import gammaJoin.Sink;
import gammaSupport.ThreadList;
import org.junit.Test;

/**
 *
 * @author terriBoose
 */
public class HJoinRefineTest {

    private String correctDir = "src/correctOutput/";
    private String outDir = "src/testOutput/";
    private String inDir = "src/tables/";
    
    public void join(String r1name, String r2name, int jk1, int jk2) throws Exception {
        System.out.println( "Joining " + r1name + " with " + r2name );  
        
        ThreadList.init();
        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(r1name, c1); 
        Connector c2 = new Connector("input2");
        ReadRelation r2 = new ReadRelation(r2name, c2);
        
        Connector a_out = new Connector("a_out");
        Connector m_out = new Connector("m_out");
        Connector b_out = new Connector("b_out");
        Connector joined_out = new Connector("joined_out");
        
        Bloom bloom = new Bloom(c1, jk1, a_out, m_out);
        BFilter bfilter = new BFilter(m_out, c2, jk2, b_out);
        
        HJoin hjoin = new HJoin(a_out, b_out, jk1, jk2, joined_out);
        
        Print p = new Print(joined_out);
        
        ThreadList.run(p);
    }
    
    @Test
    public void HJoinRefinedTest() throws Exception{
        Utility.redirectStdOut(outDir+"hJoinRefinedOut.txt");
        join(inDir+"parts.txt", inDir+"odetails.txt", 0, 1);
        join(inDir+"client.txt", inDir+"viewing.txt", 0, 0);
        join(inDir+"orders.txt", inDir+"odetails.txt", 0, 0);
        Utility.validate(outDir+"hJoinRefinedOut.txt", correctDir+"hJoinOut.txt",false);
    }
}
