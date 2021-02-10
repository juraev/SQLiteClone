import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

public class Test {
    public static void main(String[] args) {
        Process p;

        try{
            p = new ProcessBuilder().command("/Users/user/CLionProjects/SQLClone/cmake-build-debug/SQLCloneExp").start();
            InputStream is = p.getInputStream();
            InputStream er = p.getErrorStream();
            OutputStream os = p.getOutputStream();

            byte[] buffer = new byte[4096*100];
            List<String> commands = new ArrayList<>();
            int k = 3;

//             for(int i = 0; i < k; i ++){
//             }

            String s = "insert " + -1 + " " +
                                   String.join("", Collections.nCopies(33, "a")) + " " +
                                   String.join("", Collections.nCopies(256, "a"))+ "\n";
            commands.add(s);
            commands.add("select\n");
            commands.add(".exit\n");
            for(String command : commands) {
                if(!p.isAlive())
                    break;
                os.write(command.getBytes());
                os.flush();
            }
            os.close();
            try{
                Thread.sleep(1000);
            } catch(Exception e){}

            int num = is.available();
            while(num == 0){
                num = is.available();
            }
            is.read(buffer, 0, num);

            int last = 0;
            int cnt = 0;
            for (int i = 0; i < num && cnt <= k; i ++){
                if(buffer[i] == '\n'){
                    System.out.println(new String(buffer, last, i - last));
                    cnt ++;
                    last = i + 1;
                }
            }

            is.close();
            er.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
