package org.vimalkeshu.replication.master;

public class MasterMain {


    public static void main(String[] args) throws Exception {
        // args
        String masterHost = args[0];
        int masterPort = Integer.parseInt(args[1]);

        // start service
        MasterService masterService = new MasterService(masterHost, masterPort);
        masterService.start();
        masterService.blockUntilShutdown();
    }
}
