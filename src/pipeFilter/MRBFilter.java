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
public class MRBFilter extends Thread{
    
    private ReadEnd b_in;
    private ReadEnd m_in;
    private WriteEnd b_out;
    private WriteEnd m_out;
    
    public MRBFilter (Connector c_m, Connector c_b, int jk, Connector c_out) throws Exception {
        
        b_in = c_b.getReadEnd();
        m_in = c_m.getReadEnd();
        
        Connector m_splitm = new Connector("m_splitm");
        m_splitm.setRelation(Relation.dummy);
        m_out = m_splitm.getWriteEnd();
        
        Connector b_hsplit = new Connector("b_hsplit");
        b_hsplit.setRelation(c_b.getRelation());
        b_out = b_hsplit.getWriteEnd();
        
        Connector[] splitm_bfilter = ArrayConnectors.initConnectorArray("splitm_bfilter");
        Connector[] hsplit_bfilter = ArrayConnectors.initConnectorArray("hsplit_bfilter");
        Connector[] bfilter_merge = ArrayConnectors.initConnectorArray("bfilter_merge");
        
        int splits = bfilter_merge.length;
        
        SplitM splitm = new SplitM(m_splitm, splitm_bfilter);
        HSplit hsplit = new HSplit(b_hsplit, hsplit_bfilter);
        
        BFilter[] bfilters = new BFilter[splits];
        for(int i=0; i<splits; ++i){
            bfilters[i] = new BFilter(splitm_bfilter[i], hsplit_bfilter[i], jk, bfilter_merge[i]);
        }
        
        Merge merge = new Merge(bfilter_merge, c_out);
        
        ThreadList.add(this);
    }
    
    public void run () {
        try {
            String s = m_in.getNextString();
            m_out.putNextString(s);
            m_out.close();
            
            Tuple t = b_in.getNextTuple();
            while(t != null){
                b_out.putNextTuple(t);
                t = b_in.getNextTuple();
            }
            b_out.close();
        } 
        catch (Exception ex) {
            Logger.getLogger(MRBFilter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
