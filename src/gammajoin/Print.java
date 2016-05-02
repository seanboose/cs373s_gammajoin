package gammaJoin;

import basicConnector.*;
import gammaSupport.*;

import java.util.StringTokenizer;

public class Print extends Thread {
    private ReadEnd rTuple;

    public Print(Connector connReadTuple) {
        this.rTuple = connReadTuple.getReadEnd();
        ThreadList.add(this);
    }

    public void run() {
        try {
            String input;

            // Print the column names
            Relation rel = rTuple.getRelation();
            for(int i = 0; i < rel.getSize(); i++) {
                System.out.print(rel.getField(i));
                // Add spaces
                for(int j = 0; j < 3 * rel.getField(i).length(); j++) {
                    System.out.print(" ");
                }
                System.out.print(" ");
            }

            // Print the lines under
            System.out.println();
            for(int i = 0; i < rel.getSize(); i++) {
                for(int j = 0; j < 4 * rel.getField(i).length(); j++) {
                    System.out.print("-");
                }
                System.out.print(" ");
            }

            // Handle each incoming tuple
            while (true) {
                System.out.println();
                input = rTuple.getNextString();

                if (input == null) {
                    break;
                }

                // Handle each value in the tuple
                StringTokenizer st = new StringTokenizer(input);
                int fieldNum = 0; // Know which field is associated with the value
                boolean gotNumber = false; // Still need to throw the number away
                while(st.hasMoreTokens()) {
                    String nextFieldValue = st.nextToken("#");
                    if (!gotNumber) {
                        // Throwing away the size
                        gotNumber = true;
                        continue;
                    }
                    System.out.print(nextFieldValue);
                    for (int i = nextFieldValue.length(); i < 4 * rel.getField(fieldNum).length() + 1; i++) {
                        System.out.print(" ");
                    }
                    fieldNum++;
                }
            }
            System.out.flush();
        }
        catch (Exception e) {
            ReportError.msg("Print: " + this.getClass().getName() + e);
        }
    }
}
