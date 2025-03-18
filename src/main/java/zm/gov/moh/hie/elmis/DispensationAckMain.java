package zm.gov.moh.hie.elmis;
import BusinessLogic.DispensationAckProcess;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DispensationAckMain {
    public static void main(String[] args) throws Exception {

        System.out.println("Process Started.....");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DispensationAckProcess.Entry_point(env);
        env.execute("Dispensation-Ack Process Started!!!!!");

    }
}
