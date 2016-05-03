/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gammaJoin;

import basicConnector.Connector;
import basicConnector.ReadEnd;
import basicConnector.WriteEnd;
import gammaSupport.BMap;
import gammaSupport.Relation;
import gammaSupport.ThreadList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author terriBoose
 */
public class SplitM extends Thread {
    
    private ReadEnd in;
    private WriteEnd[] outs;
    
    
    public SplitM (Connector c_in, Connector[] c_outs) {
        
        in = c_in.getReadEnd();
        
        outs = new WriteEnd[c_outs.length];
        for(int i = 0; i< c_outs.length; ++i){
            outs[i] = c_outs[i].getWriteEnd();
            c_outs[i].setRelation(Relation.dummy);
        }
        
        ThreadList.add(this);
    }
    
    public void run () {
        try {
            String s = in.getNextString();
            for(int i=0; i<outs.length; ++i){
                outs[i].putNextString(BMap.mask(s, i));
                outs[i].close();
            }
        } 
        catch (Exception ex) {
            Logger.getLogger(SplitM.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}
