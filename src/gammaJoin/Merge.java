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
public class Merge extends Thread {
    
    private ReadEnd[] ins;
    private WriteEnd out;

    public Merge (Connector[] c_ins, Connector c_out){

        ins = new ReadEnd[c_ins.length];
        for(int i=0; i<c_ins.length; ++i){
            ins[i] = c_ins[i].getReadEnd();
        }
        
        out = c_out.getWriteEnd();
        c_out.setRelation(c_ins[0].getRelation());
        
        ThreadList.add(this);
    }
    
    public void run () {
        try {
            int i=0;
            Tuple t;
            while(i<ins.length){
                t = ins[i].getNextTuple();
                while(t != null){
                    out.putNextTuple(t);
                    t = ins[i].getNextTuple();
                }
                ++i;
            }
            
            out.close();
        } 
        catch (Exception ex) {
            Logger.getLogger(HSplit.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
