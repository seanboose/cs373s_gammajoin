/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gammaJoin;

import basicConnector.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import gammaSupport.*;
import static gammaSupport.Tuple.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author terriBoose
 */
public class ReadRelation extends Thread {
    private BufferedReader in;
    private WriteEnd out;
    private Relation r;
    
    ArrayList<String> columnNames = new ArrayList<String>();

    public ReadRelation(String fileName, Connector c) {
        try {
            // Set write end
            out = c.getWriteEnd();
            
            // Create Relation
            in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
            String relationLine = in.readLine();
            StringTokenizer st = new StringTokenizer(relationLine);
            r = new Relation(fileName, st.countTokens());
            while(st.hasMoreTokens()){
                r.addField(st.nextToken());
            }
            
            // Set Connector's relation
            c.setRelation(r);
            
            // Remove blank line before Tuples
            in.readLine();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.out.println("Working dir: " + System.getProperty("user.dir"));
        }
        this.out = out;
        
        // Add this to the list of running threads
        ThreadList.add(this);
    }
    
    public void run() {
        try {
            String input;
            Tuple t;
            // Parse rows into records
            while(true) {
                input = in.readLine();
                if(input == null){
                    // No more data to consume
                    break;
                }
                t = makeTupleFromFileData(r, input);
                out.putNextTuple(t);
            }
            out.close();
        }
        catch (IOException e){
            ReportError.msg(this.getClass().getName() + e);
        } catch (Exception ex) {
            Logger.getLogger(ReadRelation.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
