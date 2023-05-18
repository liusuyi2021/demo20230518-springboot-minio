package com.example.minio;

/**
 * @Description:
 * @ClassName: MinioClientSingleton
 * @Author: 刘苏义
 * @Date: 2023年05月18日9:32
 * @Version: 1.0
 **/
import io.minio.MinioClient;
import lombok.Data;
import java.io.IOException;
import java.util.Properties;

@Data
public class MinioClientSingleton {

    private static String domainUrl;
    private static String accessKey;
    private static String secretKey;

    private volatile static MinioClient minioClient;
    static {
        // 1.加载配置文件获取配置信息
        Properties properties = new Properties();
        try {
            properties.load(MinioClientSingleton.class.getClassLoader().getResourceAsStream("application.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        domainUrl= properties.getProperty("minio.endpoint");
        accessKey=properties.getProperty("minio.accessKey");
        secretKey= properties.getProperty("minio.secretKey");

        System.out.println("minio信息：" + domainUrl + "(" + accessKey+"/"+secretKey+")");
    }


    /**
     * 获取minio客户端实例
     *
     * @return {@link MinioClient}
     */
    public static MinioClient getMinioClient() {
        if (minioClient == null) {
            synchronized (MinioClientSingleton.class) {
                if (minioClient == null) {
                    minioClient = MinioClient.builder()
                            .endpoint(domainUrl)
                            .credentials(accessKey, secretKey)
                            .build();
                }
            }
        }
        return minioClient;
    }
}