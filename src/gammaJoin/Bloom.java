/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gammaJoin;

import basicConnector.*;
import gammaSupport.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author terriBoose
 */
public class Bloom extends Thread {
    
    private ReadEnd in;
    private WriteEnd a_out;
    private WriteEnd m_out;
    private BMap bmap;
    private int key;
    
    public Bloom(Connector c_in, int jk, Connector c_out_a, Connector c_out_m){
        
        this.in = c_in.getReadEnd();
        this.key = jk;
        this.a_out = c_out_a.getWriteEnd();
        this.a_out.setRelation(c_in.getRelation());
        this.m_out = c_out_m.getWriteEnd();
        this.m_out.setRelation(Relation.dummy);
        
        this.bmap = BMap.makeBMap();

        ThreadList.add(this);
    }
    
    public void run () {
        try {
            
            // Set all input tuple bits to true, pass on tuple
            Tuple t = in.getNextTuple();
            while(t != null){
                bmap.setValue(t.get(key), true);
                a_out.putNextTuple(t);
                t = in.getNextTuple();
            }
            
            // Output M bitmap
            m_out.putNextString(bmap.getBloomFilter());
            
            a_out.close();
            m_out.close();
        }
        catch (Exception e){
            Logger.getLogger(HJoin.class.getName()).log(Level.SEVERE, null, e);
        }
    }
    
}
