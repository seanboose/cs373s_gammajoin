/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gammaJoin;

import basicConnector.*;
import gammaSupport.*;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author terriBoose
 */
public class HJoin extends Thread{
    
    private ReadEnd in1;
    private ReadEnd in2;
    private WriteEnd out;
    private int key1;
    private int key2;
    private Relation r;
    
    private Hashtable<String, Tuple> table;
    
    public HJoin(Connector c1, Connector c2, int jk1, int jk2, Connector o) {
        this.in1 = c1.getReadEnd();
        this.in2 = c2.getReadEnd();
        this.key1 = jk1;
        this.key2 = jk2;
        this.out = o.getWriteEnd();
        
        this.r = Relation.join(c1.getRelation(), c2.getRelation(), this.key1, this.key2);
        out.setRelation(r);
        
        table = new Hashtable<String, Tuple>();
        
        ThreadList.add(this);
    }
    
    public void run () {
        try {
            
            // Read all of input 1 into table
            Tuple t1 = in1.getNextTuple();
            while(t1 != null){
                table.put(t1.get(key1), t1);
                t1 = in1.getNextTuple();
            }
            
            // Read all of input 2
            Tuple t2 = in2.getNextTuple();
            while(t2 != null){
                t1 = table.get(t2.get(key2));
                
                // If there's a match between join keys, output joined
                if(t1 != null){
                    Tuple t_out = Tuple.join(t1, t2, key1, key2);
                    out.putNextTuple(t_out);
                }
                t2  = in2.getNextTuple();
            }
            out.close();
        }
        catch (Exception ex) {
            Logger.getLogger(HJoin.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
}
