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
public class HSplit extends Thread{
    
    private ReadEnd in;
    private WriteEnd[] outs;
    private int index;
    
    public HSplit (Connector c_in, Connector[] c_outs, int key) {
        
        in = c_in.getReadEnd();
        
        outs = new WriteEnd[c_outs.length];
        for(int i = 0; i< c_outs.length; ++i){
            outs[i] = c_outs[i].getWriteEnd();
            c_outs[i].setRelation(c_in.getRelation());
        }
        
        index = key;
        
        ThreadList.add(this);
    }
    
    public void run () {
        try {
            Tuple t = in.getNextTuple();
            while(t != null){
                int i = BMap.myhash(t.get(index));
                outs[i].putNextTuple(t);
                t = in.getNextTuple();
            }
            
            for(int i=0; i<outs.length; ++i){
                outs[i].close();
            }
        } catch (Exception ex) {
            Logger.getLogger(HSplit.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
