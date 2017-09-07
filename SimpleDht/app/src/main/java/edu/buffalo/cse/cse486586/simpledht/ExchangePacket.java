package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;

/**
 * Created by yuzeliu on 4/13/17.
 */

public class ExchangePacket implements Serializable {


        int packetType;
        String originalSenderAvd = null;
        String receiverAvd = null;
        String asignSuccessor = null;
        String assignPredecessor = null;
        String exchangeKey = null;
        //String keyFiledVal = null;
        String contentValue = null;

        public ExchangePacket(){

        }


        public String ToString(){
                return packetType + "," + originalSenderAvd + "," + receiverAvd + "," + asignSuccessor + "," + assignPredecessor + "," +
                        exchangeKey + "," + contentValue;
        }

}
