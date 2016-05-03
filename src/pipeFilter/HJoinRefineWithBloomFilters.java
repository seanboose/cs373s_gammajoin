/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pipeFilter;

import basicConnector.*;
import gammaJoin.*;
import gammaSupport.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author terriBoose
 */
public class HJoinRefineWithBloomFilters extends Thread {
    
    private ReadEnd in1;
    private ReadEnd in2;
    private WriteEnd write1;
    private WriteEnd write2;

    public HJoinRefineWithBloomFilters(Connector c1, Connector c2, int jk1, int jk2, Connector o) throws Exception{
        
        in1 = c1.getReadEnd();
        in2 = c2.getReadEnd();
        
        Connector a_bloom = new Connector("a_bloom");
        a_bloom.setRelation(c1.getRelation());
        Connector bloom_hjoin = new Connector("bloom_hjoin");
        Connector bloom_filter = new Connector("bloom_filter");
        Connector b_filter = new Connector("b_filter");
        b_filter.setRelation(c2.getRelation());
        Connector filter_hjoin = new Connector("filter_hjoin");
        
        Bloom bloom = new Bloom(a_bloom, jk1, bloom_hjoin, bloom_filter);
        BFilter bfilter = new BFilter(bloom_filter, b_filter, jk2, filter_hjoin);
        HJoin hjoin = new HJoin(bloom_hjoin, filter_hjoin, jk1, jk2, o);
        
        write1 = a_bloom.getWriteEnd();
        write2 = b_filter.getWriteEnd();

        ThreadList.add(this);
    }
    
    public void run () {
        try {
            Tuple t1 = in1.getNextTuple();
            while(t1 != null) {
                write1.putNextTuple(t1);
                t1 = in1.getNextTuple();
            }
            write1.close();
            
            Tuple t2 = in2.getNextTuple();
            while(t2 != null) {
                write2.putNextTuple(t2);
                t2 = in2.getNextTuple();
            }
            write2.close();
            
        } catch (Exception ex) {
            Logger.getLogger(HJoinRefineWithBloomFilters.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
