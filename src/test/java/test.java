import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.io.File;

/**
 * @Author TianCheng
 * @Date 2020/3/6 10:45
 */
public class test {

    @Test
    public void strip(){
        String a = "._sfv.* ";
        System.out.println(StringUtils.strip(a, " ._*"));
    }

    @Test
    public void fileURL(){
        File file = new File("src/main/resources/file");
    }

}
