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
public class MRBloom extends Thread {
    
    private ReadEnd in;
    private WriteEnd out;
    
    public MRBloom(Connector c_in, int jk, Connector c_out_a, Connector c_out_m){
        
        Connector in_hsplit = new Connector("in_hsplit");
        in_hsplit.setRelation(c_in.getRelation());
        
        Connector[] hsplit_bloom = ArrayConnectors.initConnectorArray("hsplit_bloom");
        Connector[] bloom_a_merge = ArrayConnectors.initConnectorArray("bloom_a_merge");
        Connector[] bloom_m_merge = ArrayConnectors.initConnectorArray("bloom_m_merge");
        int splits = hsplit_bloom.length;
        
        in = c_in.getReadEnd();
        out = in_hsplit.getWriteEnd();
        
        HSplit hsplit = new HSplit(in_hsplit, hsplit_bloom);
        Bloom[] bloom = new Bloom[splits];
        for(int i=0; i<splits; ++i){
            bloom[i] = new Bloom(hsplit_bloom[i], jk, bloom_a_merge[i], bloom_m_merge[i]);
        }
        
        Merge merge = new Merge(bloom_a_merge, c_out_a);
        MergeM mergem = new MergeM(bloom_m_merge, c_out_m);

        ThreadList.add(this);
    }
    
    public void run () {
        try {
            Tuple t = in.getNextTuple();
            while(t != null){
                out.putNextTuple(t);
                t = in.getNextTuple();
            }
            out.close();
        } 
        catch (Exception ex) {
            Logger.getLogger(MRBloom.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
