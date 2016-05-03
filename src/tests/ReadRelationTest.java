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
public class ReadRelationTest {
    
    private String correctDir = "src/correctOutput/";
    private String outDir = "src/testOutput/";
    private String inDir = "src/tables/";
    
    @Test
    public void ReadRelationTest1 () throws Exception{
        
        String in = inDir+"client.txt";
        String out = outDir+"out.txt";
        String correct = correctDir+"readRelationTest1.txt";
        
        Utility.redirectStdOut(out);
        ThreadList.init();

        Connector read_print = new Connector("read_print");
        ReadRelation read = new ReadRelation(in, read_print);
        Print print = new Print(read_print);
        ThreadList.run(print);
        Utility.validate(out, correct, false);
    }
}
