package zm.gov.moh.hie.elmis;
import BusinessLogic.PrescriptionAckProcess;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PrescriptionAckMain {
    public static void main(String[] args) throws Exception {
        System.out.println("Process Started");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        PrescriptionAckProcess.processAcks(env);
        env.execute("Prescription-Ack Process Started!!!!!");
    }

}
