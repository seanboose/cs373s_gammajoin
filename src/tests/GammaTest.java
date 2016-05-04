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
import pipeFilter.Gamma;

/**
 *
 * @author terriBoose
 */
public class GammaTest {
    
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
        Connector o = new Connector("output");
        Gamma hj = new Gamma(c1, c2, jk1, jk2, o);
        Print p = new Print(o);
        ThreadList.run(p);
    }
    
    @Test
    public void GammaTest () throws Exception{
        Utility.redirectStdOut(outDir+"gammaOut.txt");
        join(inDir+"parts.txt", inDir+"odetails.txt", 0, 1);
        join(inDir+"client.txt", inDir+"viewing.txt", 0, 0);
        join(inDir+"orders.txt", inDir+"odetails.txt", 0, 0);
        Utility.validate(outDir+"gammaOut.txt", correctDir+"gammaOut.txt",false);
    }
}
