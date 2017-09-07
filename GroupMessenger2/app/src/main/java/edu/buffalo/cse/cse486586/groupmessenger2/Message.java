package edu.buffalo.cse.cse486586.groupmessenger2;

import android.util.Log;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by yuzeliu on 3/24/17.
 */

public class Message implements Comparable<Message>{
    int messageType;             // 0 - newMessage; 1 - proposedMessage; 2 - AgreedMessage
    int messageId;               // Counter from the processor who first send this msg
    int processorID;             // "11108", "11112", "11116", "11120", "11124"
    int sequence;                // counter that the sequence that this processor receives from another processor
    int anotherProcessorID;      // B-deliver from processor; "11108", "11112", "11116", "11120", "11124"
    String msg;                  // Message content
    int deliverableStatus;       // 1 - undeliverable; 2 - deliverable
    int[] proposedArray = {0,0,0,0,0};
    //int[] proposedArray = {0,0,1,1,1};


    public Message (int messageType, int messageId, int processorID, int sequence, int anotherProcessorID, int deliverableStatus, String msg ){
        this.messageType = messageType;
        this.messageId = messageId;
        this.processorID = processorID;
        this.sequence = sequence;
        this.anotherProcessorID = anotherProcessorID;
        this.deliverableStatus = deliverableStatus;
        this.msg = msg;

    }

    public  Message(String inputMessage){
        this.msg = inputMessage;
    }


    @Override //Natural order is smallest sequence number and smallest proposed ID
    public int compareTo(Message another) {
//        if(this.sequence == another.sequence){
//            if(this.deliverableStatus == another.deliverableStatus){
//                if(this.anotherProcessorID < another.anotherProcessorID){
//                    return -1;
//                } else {
//                    return 1;
//                }
//            } else if(this.deliverableStatus < another.deliverableStatus){
//                return -1;
//            } else if(this.deliverableStatus > another.deliverableStatus){
//                return 1;
//            }
//        } else if(this.sequence < another.sequence){
//            return -1;
//        } else {
//            return 1;
//        }

//        if(o == null || !(o instanceof Message)){
//            return 1;
//        }
//        Message other = (Message) o;
//        if(this.sequenceNumber == other.sequenceNumber){
//            return Integer.compare(this.proposerId,other.proposerId);
//        }
//        return Integer.compare(this.sequenceNumber,other.sequenceNumber);
//    }
//}
//        if(sequence < another.sequence){
//            return -1;
//        } else if(sequence > another.sequence){
//            return 1;
//        } else if(sequence == another.sequence){
//            if(deliverableStatus < another.deliverableStatus){
//                return -1;
//            } else if(deliverableStatus > another.deliverableStatus){
//                return 1;
//            } else if(deliverableStatus == another.deliverableStatus){
//                if(anotherProcessorID < another.anotherProcessorID){
//                    return -1;
//                } else if (anotherProcessorID > another.anotherProcessorID){
//                    return 1;
//                }
//            }
//        }
        if(sequence < another.sequence){
            return -1;
        }
        if(sequence > another.sequence){
            return 1;
        }

        if(deliverableStatus < another.deliverableStatus){
            return -1;
        }

        if(deliverableStatus > another.deliverableStatus){
            return 1;
        }


        if(anotherProcessorID < another.anotherProcessorID){
            return -1;
        }

        if (anotherProcessorID > another.anotherProcessorID){
            return 1;
        }

        return 0;
    }

    public String MessageToStringMsg(){
        String messageToString = this.messageType + "," + this.messageId + "," + this.processorID + "," + this.sequence + "," + this.anotherProcessorID + ","
                + this.deliverableStatus + "," + this.msg + "," + this.proposedArray[0]+ "," + this.proposedArray[1]+ "," + this.proposedArray[2]
                + "," + this.proposedArray[3]+ "," + this.proposedArray[4];

//        String messageToString = this.messageId + "," + this.processorID + "," + this.sequence + "," + this.anotherProcessorID + ","
//                + this.deliverableStatus + "," + this.msg + "," + this.proposedArray[0]+ "," + this.proposedArray[1]+ "," + this.proposedArray[2]
//                + "," + this.proposedArray[3]+ "," + this.proposedArray[4];
        return messageToString;
    }

    //@Override
    public boolean equals(Message o){
        if(o.messageId == this.messageId && o.processorID == this.processorID){
            return true;
        }

        return false;
    }

    public void replace(Message o){
        this.sequence = o.sequence;
        this.anotherProcessorID = o.anotherProcessorID;
    }

//    public void update(Message o){
//        this.sequence = o.sequence;
//        this.anotherProcessorID = o.anotherProcessorID;
//        this.proposedArray = o.proposedArray;
//        this.deliverableStatus = o.deliverableStatus;
//    }

    public boolean getHighestSeq(Message o){
        if(o.sequence > this.sequence){
            return true;
        } else if(o.sequence == this.sequence){
            if(o.anotherProcessorID < this.anotherProcessorID){
                return true;
            }
        }
        return false;
    }

    public void getOriginalSender(){
        if(this.processorID == 11108){
            this.proposedArray[0] = 1;
        } else if(this.processorID == 11112){
            this.proposedArray[1] = 1;
        } else if(this.processorID == 11116){
            this.proposedArray[2] = 1;
        } else if(this.processorID == 11120){
            this.proposedArray[3] = 1;
        }else if(this.processorID == 11124){
            this.proposedArray[4] = 1;
        }
    }

    public void getProposedSender(Message msg){
        if(msg.anotherProcessorID == 11108){
            this.proposedArray[0] = 1;
        } else if(msg.anotherProcessorID == 11112){
            this.proposedArray[1] = 1;
        } else if(msg.anotherProcessorID == 11116){
            this.proposedArray[2] = 1;
        } else if(msg.anotherProcessorID == 11120){
            this.proposedArray[3] = 1;
        }else if(msg.anotherProcessorID == 11124){
            this.proposedArray[4] = 1;
        }
    }

    public boolean checkArray(){
        for(int i : this.proposedArray){
            if(i == 1){
                continue;
            } else {
                return false;
            }
        }
        return true;
    }

    public int indexInPArray(){
        if(this.anotherProcessorID == 11108){
            return 0;
        } else if(this.anotherProcessorID == 11112){
            return 1;
        } else if(this.anotherProcessorID == 11116){
            return 2;
        } else if(this.anotherProcessorID == 11120){
            return 3;
        }else{
            return 4;
        }
    }

}
