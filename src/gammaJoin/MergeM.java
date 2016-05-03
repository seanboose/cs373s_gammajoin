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
public class MergeM extends Thread {
    
    private ReadEnd[] ins;
    private WriteEnd out;
    private BMap bmap;

    public MergeM (Connector[] c_ins, Connector c_out){

        ins = new ReadEnd[c_ins.length];
        for(int i=0; i<c_ins.length; ++i){
            ins[i] = c_ins[i].getReadEnd();
        }
        
        out = c_out.getWriteEnd();
        c_out.setRelation(Relation.dummy);
        
        bmap = BMap.makeBMap();
        
        ThreadList.add(this);
    }
    
    public void run () {
        try {
            int i = 0;
            String s = ins[i].getNextString();
            while(s != null && i < ins.length-1){
                BMap temp = BMap.makeBMap(s);
                bmap = BMap.or(bmap, temp);
                s = ins[++i].getNextString();
            }
            BMap temp = BMap.makeBMap(s);
            bmap = BMap.or(bmap, temp);
            
            out.putNextString(bmap.getBloomFilter());
            out.close();
        } 
        catch (Exception ex) {
            Logger.getLogger(MergeM.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
