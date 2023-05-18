package com.example.minio;

import io.minio.*;
import io.minio.http.Method;
import io.minio.messages.Bucket;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @Description: Minio客户端工具类
 * @ClassName: MinioUtils
 * @Author: 刘苏义
 * @Date: 2023年05月18日9:34
 * @Version: 1.0
 **/
@SuppressWarnings("ALL")
@Slf4j
public class MinioUtils {

    /**
     * 判断桶是否存在
     */
    public static boolean exitsBucket(String bucketName) {
        boolean found = false;
        try {
            BucketExistsArgs bucketExistsArgs = BucketExistsArgs.builder().bucket(bucketName).build();
            found = MinioClientSingleton.getMinioClient().bucketExists(bucketExistsArgs);
        } catch (Exception ex) {
            log.error("minio判断桶存在异常：", ex.getMessage());
        }
        return found;
    }

    /**
     * 创建桶,并设置桶策略为公共
     */
    public static boolean createBucket(String bucketName) {
        try {
            /*创建桶*/
            MakeBucketArgs makeBucketArgs = MakeBucketArgs.builder().bucket(bucketName).build();
            MinioClientSingleton.getMinioClient().makeBucket(makeBucketArgs);
            /*设置策略*/
            String sb = "{\"Version\":\"2012-10-17\"," +
                    "\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":" +
                    "{\"AWS\":[\"*\"]},\"Action\":[\"s3:ListBucket\",\"s3:ListBucketMultipartUploads\"," +
                    "\"s3:GetBucketLocation\"],\"Resource\":[\"arn:aws:s3:::" + bucketName +
                    "\"]},{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"*\"]},\"Action\":[\"s3:PutObject\",\"s3:AbortMultipartUpload\",\"s3:DeleteObject\",\"s3:GetObject\",\"s3:ListMultipartUploadParts\"],\"Resource\":[\"arn:aws:s3:::" +
                    bucketName + "/*\"]}]}";
            SetBucketPolicyArgs setBucketPolicyArgs = SetBucketPolicyArgs.builder()
                    .bucket(bucketName)
                    .config(sb)
                    .build();
            MinioClientSingleton.getMinioClient().setBucketPolicy(setBucketPolicyArgs);
            return true;
        } catch (Exception ex) {
            log.error("minio创建桶异常：", ex.getMessage());
            return false;
        }
    }

    /**
     * 删除一个桶
     *
     * @param bucket 桶名称
     */
    public static boolean removeBucket(String bucket) {
        try {
            boolean found = exitsBucket(bucket);
            if (found) {
                Iterable<Result<Item>> myObjects = MinioClientSingleton.getMinioClient().listObjects(ListObjectsArgs.builder().bucket(bucket).build());
                for (Result<Item> result : myObjects) {
                    Item item = result.get();
                    //有对象文件，则删除失败
                    if (item.size() > 0) {
                        return false;
                    }
                }
                // 删除`bucketName`存储桶，注意，只有存储桶为空时才能删除成功。
                MinioClientSingleton.getMinioClient().removeBucket(RemoveBucketArgs.builder().bucket(bucket).build());
                found = exitsBucket(bucket);
                return !found;
            }
        } catch (Exception ex) {
            log.error("删除桶异常：" + ex.getMessage());
        }
        return false;
    }

    /**
     * 查询所有桶文件
     *
     * @return
     */
    public static List<Bucket> getListBuckets() {
        try {
            return MinioClientSingleton.getMinioClient().listBuckets();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    /**
     * 生成一个GET请求的带有失效时间的分享链接。
     * 失效时间默认是7天。
     *
     * @param bucketName 存储桶名称
     * @param objectName 存储桶里的对象名称
     * @param expires    失效时间（以秒为单位），默认是7天，不得大于七天
     * @return
     */
    public static String getObjectWithExpired(String bucketName, String objectName, Integer expires, TimeUnit timeUnit) {
        String url = "";
        if (exitsBucket(bucketName)) {
            try {
                GetPresignedObjectUrlArgs getPresignedObjectUrlArgs = GetPresignedObjectUrlArgs.builder()
                        .method(Method.GET)
                        .bucket(bucketName)
                        .object(objectName)
                        .expiry(expires, timeUnit)
                        .build();
                url = MinioClientSingleton.getMinioClient().getPresignedObjectUrl(getPresignedObjectUrlArgs);
            } catch (Exception ex) {
                log.error("minio生成失效url异常", ex.getMessage());
            }
        }
        return url;
    }

    /**
     * @描述 上传MultipartFile文件返回url
     * @参数 [bucketName, file]
     * @返回值 java.lang.String
     * @创建人 刘苏义
     * @创建时间 2023/5/18 12:16
     * @修改人和其它信息
     */
    public static String putObjectAndGetUrl(String bucketName, MultipartFile file) {
        //判断文件是否为空
        if (null == file || 0 == file.getSize()) {
            log.error("上传minio文件服务器错误，上传文件为空");
        }
        boolean exsit = exitsBucket(bucketName);
        if (!exsit) {
            log.error(bucketName + "-桶不存在");
        }
        //文件名
        String originalFilename = file.getOriginalFilename();
        //新的文件名
        String fileName = UUID.randomUUID().toString().replace("-", "") + originalFilename;
        try {
            InputStream inputStream = file.getInputStream();
            /*上传对象*/
            PutObjectArgs putObjectArgs = PutObjectArgs
                    .builder()
                    .bucket(bucketName)
                    .object(fileName)
                    .stream(inputStream, file.getSize(), -1)
                    .contentType(file.getContentType())
                    .build();
            MinioClientSingleton.getMinioClient().putObject(putObjectArgs);
            inputStream.close();
            /*获取url*/
            GetPresignedObjectUrlArgs getPresignedObjectUrlArgs = GetPresignedObjectUrlArgs
                    .builder()
                    .bucket(bucketName)
                    .object(fileName)
                    .build();
            return MinioClientSingleton.getMinioClient().getPresignedObjectUrl(getPresignedObjectUrlArgs);
        } catch (Exception ex) {
            log.error("上传对象返回url异常：" + ex.getMessage());
        }
        return "";
    }

    /**
     * 删除文件
     *
     * @param bucket     桶名称
     * @param objectName 对象名称
     * @return boolean
     */
    public static boolean removeObject(String bucket, String objectName) {
        try {
            boolean exsit = exitsBucket(bucket);
            if (exsit) {
                RemoveObjectArgs removeObjectArgs = RemoveObjectArgs.builder().bucket(bucket).object(objectName).build();
                MinioClientSingleton.getMinioClient().removeObject(removeObjectArgs);
                return true;
            }
        } catch (Exception e) {
            log.error("removeObject", e);
        }
        return false;
    }

    /**
     * 批量删除文件
     *
     * @param objectNames 对象名称
     * @return boolean
     */
    public static boolean removeObjects(String bucket, List<String> objectNames) {
        if (exitsBucket(bucket)) {
            try {
                List<DeleteObject> objects = new LinkedList<>();
                for (String str : objectNames) {
                    objects.add(new DeleteObject(str));
                }
                RemoveObjectsArgs removeObjectsArgs = RemoveObjectsArgs.builder().bucket(bucket).objects(objects).build();
                Iterable<Result<DeleteError>> results = MinioClientSingleton.getMinioClient().removeObjects(removeObjectsArgs);
                /*删除完遍历结果，否则删不掉*/
                for (Result<DeleteError> result : results) {
                    DeleteError error = result.get();
                    log.error("Error in deleting object " + error.objectName() + "; " + error.message());
                }

                return true;
            } catch (Exception ex) {
                log.error("minio批量删除文件异常", ex.getMessage());
            }
        }
        return false;
    }

    /**
     * 获取单个桶中的所有文件对象名称
     *
     * @param bucket 桶名称
     * @return {@link List}<{@link String}>
     */
    public static List<String> getBucketObjectName(String bucket) {
        boolean exsit = exitsBucket(bucket);
        if (exsit) {
            List<String> listObjetcName = new ArrayList<>();
            try {
                ListObjectsArgs listObjectsArgs = ListObjectsArgs.builder().bucket(bucket).build();
                Iterable<Result<Item>> myObjects = MinioClientSingleton.getMinioClient().listObjects(listObjectsArgs);
                for (Result<Item> result : myObjects) {
                    Item item = result.get();
                    listObjetcName.add(item.objectName());
                }
                return listObjetcName;
            } catch (Exception ex) {
                log.error("minio获取桶下对象异常：" + ex.getMessage());
            }
        }
        return null;
    }

    public static List<String> getBucketObjectName(String bucketName,String folder)
    {
        boolean exsit = exitsBucket(bucketName);
        if (exsit) {
            List<String> listObjetcName = new ArrayList<>();
            try {
                ListObjectsArgs listObjectsArgs = ListObjectsArgs.builder().prefix(folder+"/").bucket(bucketName).build();
                Iterable<Result<Item>> myObjects = MinioClientSingleton.getMinioClient().listObjects(listObjectsArgs);
                for (Result<Item> result : myObjects) {
                    Item item = result.get();
                    listObjetcName.add(item.objectName());
                }
                return listObjetcName;
            } catch (Exception ex) {
                log.error("minio获取桶下对象异常：" + ex.getMessage());
            }
        }
        return null;
    }
    /**
     * 获取某个桶下某个对象的URL
     *
     * @param bucket     桶名称
     * @param objectName 对象名 (文件夹名 + 文件名)
     * @return
     */
    public static String getBucketObjectUrl(String bucketName, String objectName) {
        try {
            if (!exitsBucket(bucketName)) {
                return "";
            }
            GetPresignedObjectUrlArgs getPresignedObjectUrlArgs = GetPresignedObjectUrlArgs
                    .builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .build();
            return MinioClientSingleton.getMinioClient().getPresignedObjectUrl(getPresignedObjectUrlArgs);
        } catch (Exception ex) {
            log.error("minio获取对象URL异常" + ex.getMessage());
        }
        return "";
    }

    /**
     * 上传对象-stream
     *
     * @param bucketName  bucket名称
     * @param objectName  ⽂件名称
     * @param stream      ⽂件流
     * @param size        ⼤⼩
     * @param contextType 类型 Image/jpeg 浏览器可以直接打开，否则下载
     */
    public static boolean uploadObject(String bucketName, String objectName, InputStream stream, long size, String contextType) {
        try {
            PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(stream, size, -1)
                    .contentType(contextType)
                    .build();
            ObjectWriteResponse objectWriteResponse = MinioClientSingleton.getMinioClient().putObject(putObjectArgs);
            return true;
        } catch (Exception ex) {
            log.error("minio上传文件(通过stream)异常" + ex.getMessage());
            return false;
        }
    }

    /**
     * 上传对象-File
     *
     * @param bucketName  bucket名称
     * @param objectName  ⽂件名称
     * @param file        ⽂件
     * @param contextType 类型 Image/jpeg 浏览器可以直接打开，否则下载
     */
    public static boolean uploadObject(String bucketName, String objectName, File file, String contextType) {
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(fileInputStream, file.length(), -1)
                    .contentType(contextType)
                    .build();
            ObjectWriteResponse objectWriteResponse = MinioClientSingleton.getMinioClient().putObject(putObjectArgs);
            return true;
        } catch (Exception ex) {
            log.error("minio上传文件(通过File)异常" + ex.getMessage());
            return false;
        }
    }

    /**
     * 上传对象-MultipartFile
     *
     * @param bucketName    bucket名称
     * @param objectName    ⽂件名称
     * @param MultipartFile ⽂件
     * @param contextType   类型 Image/jpeg 浏览器可以直接打开，否则下载
     */
    public static boolean uploadObject(String bucketName, String objectName, MultipartFile multipartFile, String contextType) {
        try {
            if (bucketName.isEmpty()) {
                log.error("bucket名称为空");
                return false;
            }
            if (objectName.isEmpty()) {
                log.error("对象名称为空");
                return false;
            }
            InputStream inputStream = multipartFile.getInputStream();
            PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(inputStream, multipartFile.getSize(), -1)
                    .contentType(contextType)
                    .build();
            ObjectWriteResponse objectWriteResponse = MinioClientSingleton.getMinioClient().putObject(putObjectArgs);
            return true;
        } catch (Exception ex) {
            log.error("minio上传文件(通过File)异常" + ex.getMessage());
            return false;
        }
    }

    /**
     * 上传对象,用multipartFile名称作为对象名
     *
     * @param bucketName    bucket名称
     * @param objectName    ⽂件名称
     * @param MultipartFile ⽂件
     * @param contextType   类型 Image/jpeg 浏览器可以直接打开，否则下载
     */
    public static boolean uploadObject(String bucketName, MultipartFile multipartFile, String contextType) {
        try {
            if (multipartFile == null) {
                log.error("上传文件为空");
                return false;
            }
            String objectName = multipartFile.getOriginalFilename();
            InputStream inputStream = multipartFile.getInputStream();
            PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(inputStream, multipartFile.getSize(), -1)
                    .contentType(contextType)
                    .build();
            MinioClientSingleton.getMinioClient().putObject(putObjectArgs);
            return true;
        } catch (Exception ex) {
            log.error("minio上传文件(通过File)异常" + ex.getMessage());
            return false;
        }
    }

    /**
     * 上传对象-通过本地路径
     *
     * @param bulkName
     * @param objectName
     * @param localFilePathName
     * @return
     */
    public static boolean uploadObject(String bulkName, String objectName, String localFilePathName, String contextType) {
        try {
            if (!exitsBucket(bulkName)) {
                log.debug(bulkName + "不存在");
                return false;
            }
            File file = new File(localFilePathName);
            if (!file.exists()) {
                log.debug("文件不存在");
                return false;
            }
            UploadObjectArgs uploadObjectArgs = UploadObjectArgs.builder()
                    .bucket(bulkName)
                    .object(objectName)
                    .filename(localFilePathName)
                    .contentType(contextType)
                    .build();
            ObjectWriteResponse objectWriteResponse = MinioClientSingleton.getMinioClient().uploadObject(uploadObjectArgs);
            return true;
        } catch (Exception e) {
            log.error("minio upload object file error " + e.getMessage());
            return false;
        }
    }

    public static void main(String[] args) {
//        /*删除桶*/
//        boolean b = removeBucket("lsy");
//        log.info(String.valueOf(b));
//        /*创建桶*/
//        boolean lsy = createBucket("lsy");
//        log.info(String.valueOf(lsy));
//        /*判断桶是否存在*/
//        boolean pic = exitsBucket("lsy");
//        log.info(String.valueOf(pic));
        /*查询所有桶*/
//        List<Bucket> listBuckets = getListBuckets();
//        for (Bucket bucket : listBuckets) {
//            log.info(bucket.name());
//        }

        String bucket = "lsy";
        String filename ="20230518/"+UUID.randomUUID().toString().replace("-", "") + "pic.jpeg";
        String fileFullPath = "C:\\Users\\Administrator\\Desktop\\微信截图_20230518102605.png";
//        uploadObject(bucket, filename, fileFullPath, "Image/jpeg");
//        String url = getObjectWithExpired(bucket, filename, 10, SECONDS);
//        System.out.println(url);
        boolean b = uploadObject(bucket,filename,fileFullPath,"Image/jpeg");
        System.out.println(b);

        List<String> bucketObjectName = getBucketObjectName(bucket, "20230518");
        System.out.println(bucketObjectName);
        for (String objectName:bucketObjectName)
        {
            removeObject(bucket,objectName);
        }
//        boolean b = uploadObjectBylocalPath(bucket, filename, fileFullPath);
//        System.out.println(b);
        //String url = getObjectWithExpired(bucket, filename, 10000);
//
        //String url = getBucketObject(bucket, filename);
        //      System.out.println(url);
//        List<String> bucketObjectNames = getBucketObjectName(bucket);
//        System.out.println(bucketObjectNames);
//        boolean b = removeObjects(bucket, bucketObjectNames);
//        System.out.println(b);
//        for (String obj:bucketObjectNames) {
//            boolean b = removeObject(bucket, obj);
//            System.out.println(b);
//        }
//        boolean b = removeObjects(bucket,bucketObjectNames);
//        System.out.println(b);
    }

}