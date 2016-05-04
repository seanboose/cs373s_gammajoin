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
public class Gamma extends Thread{
    
    private ReadEnd in_a;
    private ReadEnd in_b;
    private WriteEnd out_a;
    private WriteEnd out_b;
    
    public Gamma (Connector c1, Connector c2, int jk1, int jk2, Connector o) throws Exception {
        
        in_a = c1.getReadEnd();
        in_b = c2.getReadEnd();
        
        Connector a_hsplit = new Connector("a_hsplit");
        a_hsplit.setRelation(c1.getRelation());
        out_a = a_hsplit.getWriteEnd();
        
        Connector b_hsplit = new Connector("b_hsplit");
        b_hsplit.setRelation(c2.getRelation());
        out_b = b_hsplit.getWriteEnd();
        
        Connector[] hsplit_a_bloom   = ArrayConnectors.initConnectorArray("hsplit_a_bloom");
        Connector[] bloom_hjoin      = ArrayConnectors.initConnectorArray("bloom_hjoin");
        Connector[] bloom_bfilter    = ArrayConnectors.initConnectorArray("bloom_bfilter");
        Connector[] hsplit_b_bfilter = ArrayConnectors.initConnectorArray("hsplit_b_bfilter");
        Connector[] bfilter_hjoin    = ArrayConnectors.initConnectorArray("bfilter_hjoin");
        Connector[] hjoin_merge      = ArrayConnectors.initConnectorArray("hjoin_merge");
        
        int splits = hjoin_merge.length;
        
        HSplit hsplit_a = new HSplit(a_hsplit, hsplit_a_bloom, jk1);
        HSplit hsplit_b = new HSplit(b_hsplit, hsplit_b_bfilter, jk2);
        
        Bloom[]   blooms   = new Bloom[splits];
        BFilter[] bfilters = new BFilter[splits];
        HJoin[]   hjoins   = new HJoin[splits];
        
        for(int i=0; i<splits; ++i){
            blooms[i] = new Bloom(hsplit_a_bloom[i], jk1, bloom_hjoin[i], bloom_bfilter[i]);
            bfilters[i] = new BFilter(bloom_bfilter[i], hsplit_b_bfilter[i], jk2, bfilter_hjoin[i]);
            hjoins[i] = new HJoin(bloom_hjoin[i], bfilter_hjoin[i], jk1, jk2, hjoin_merge[i]);
        }
        
        Merge merge = new Merge(hjoin_merge, o);
        
        ThreadList.add(this);
    }
    
    
    public void run () {
        try  {
            Tuple ta = in_a.getNextTuple();
            while(ta != null){
                out_a.putNextTuple(ta);
                ta = in_a.getNextTuple();
            }
            out_a.close();
            
            Tuple tb = in_b.getNextTuple();
            while(tb != null){
                out_b.putNextTuple(tb);
                tb = in_b.getNextTuple();
            }
            out_b.close();
            
        } 
        catch (Exception ex) {
            Logger.getLogger(Gamma.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
