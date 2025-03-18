package zm.gov.moh.hie.elmis;

import BusinessLogic.DispensationNewProcess;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DispensationNewMain {
    public static void main(String[] args) throws Exception {
        System.out.println("Process Started.......");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         DispensationNewProcess.processDispensationPayloads(env);
        env.execute("Dispensation Payload Processing Job");
    }
}
