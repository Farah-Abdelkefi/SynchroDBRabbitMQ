import java.util.*;


public class BranchOffice {
 
    public static void main(String[] args) {
        
        BranchOfficeSync bo1 = new BranchOfficeSync("bo1");
        BranchOfficeSync bo2 = new BranchOfficeSync("bo2");
        
        bo2.Sync();

        // Timer
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                bo1.Sync();
                
               }
        }, 0, 60000); // Run every minute



    }
}
