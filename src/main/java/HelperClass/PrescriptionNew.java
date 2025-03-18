package HelperClass;

import java.sql.Timestamp;

public class PrescriptionNew {

        public Timestamp timestamp;
        public String sendingApplication;
        public String receivingApplication;
        public String messageId;
        public String hmisCode;
        public String regimenCode;
        public int regimenDuration;
        public int prescriptionsCount;


        public PrescriptionNew (Timestamp timestamp, String sendingApplication, String receivingApplication, String messageId, String hmisCode, String regimenCode, int regimenDuration, int prescriptionsCount, String prescriptionUuid) {
            this.timestamp = timestamp;
            this.sendingApplication = sendingApplication;
            this.receivingApplication = receivingApplication;
            this.messageId = messageId;
            this.hmisCode = hmisCode;
            this.regimenCode = regimenCode;
            this.regimenDuration = regimenDuration;
            this.prescriptionsCount = prescriptionsCount;
        }

}
