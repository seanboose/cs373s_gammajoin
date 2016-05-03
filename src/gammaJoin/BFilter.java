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
public class BFilter extends Thread {
    private ReadEnd b_in;
    private ReadEnd m_in;
    private WriteEnd out;
    private BMap bmap;
    private int key;
    
    public BFilter (Connector c_m, Connector c_b, int jk, Connector c_out) throws Exception {
        this.m_in = c_m.getReadEnd();
        this.b_in = c_b.getReadEnd();
        this.key = jk;
        this.out = c_out.getWriteEnd();
        
        c_out.setRelation(c_b.getRelation());
        
        ThreadList.add(this);
    }
    
    public void run () {
        try {
            bmap = BMap.makeBMap(m_in.getNextString());
            Tuple t = b_in.getNextTuple();
            while(t != null){
                if(bmap.getValue(t.get(key))){
                    out.putNextTuple(t);
                }
                t = b_in.getNextTuple();
            }
            out.close();
        }
        catch (Exception e) {
            Logger.getLogger(HJoin.class.getName()).log(Level.SEVERE, null, e);
        }
    }
}
