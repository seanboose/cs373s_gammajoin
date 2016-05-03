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
public class Sink {
    
    private ReadEnd sink;
    
    public Sink (Connector in) {
        sink = in.getReadEnd();
    }
    
    public void run () {
        try {
            Tuple t = sink.getNextTuple();
            while(t != null){
                t = sink.getNextTuple();
            }
        } catch (Exception ex) {
            Logger.getLogger(Sink.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
