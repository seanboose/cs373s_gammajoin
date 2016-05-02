package gammaJoin;

import basicConnector.Connector;
import basicConnector.ReadEnd;
import gammaSupport.ReportError;
import gammaSupport.GammaConstants;
import gammaSupport.ThreadList;

public class PrintMap extends Thread {
    private ReadEnd rBMap;

    public PrintMap(Connector connReadBMap) {
        this.rBMap = connReadBMap.getReadEnd();

        ThreadList.add(this);
    }

    public void run() {
        try {
            String bitmapString = rBMap.getNextString();
            for(int i = 0; i < GammaConstants.splitLen; i++) {
                for(int j = 0; j < GammaConstants.mapSize; j++) {
                    System.out.print(bitmapString.charAt(i * GammaConstants.mapSize + j));
                }
                System.out.println();
            }
        }
        catch(Exception e) {
            ReportError.msg("PrintMap: " + this.getClass().getName() + e);
        }
    }
}
