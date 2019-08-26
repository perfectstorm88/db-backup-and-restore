import os
from oss2 import SizedFileAdapter, determine_part_size
from oss2.models import PartInfo
import oss2
import sys

class OssHelper:

    def __init__(self,keyid,keysecret,url,bucket):
        self.auth = oss2.Auth(keyid, keysecret)
        self.bucket = oss2.Bucket(self.auth, url, bucket)

    def get_file_list(self,dir = ''):
        result = []
        ossObjects = oss2.ObjectIterator(self.bucket,prefix=dir)
        for ossObject in ossObjects:
            result.append({
                "name":os.path.basename(ossObject.key),
                "size":ossObject.size,
                "type":"oss"
            })
        return result
        # return [ for b in oss2.ObjectIterator(self.bucket,prefix=dir)]

    def upload(self,upload_path,filepath):
        """
        upload_path 文件上传后的完整路径包括本身
        filepath 本地文件路径
        """
        key = upload_path
        filename = filepath

        total_size = os.path.getsize(filename)
        # determine_part_size方法用来确定分片大小。
        part_size = determine_part_size(total_size, preferred_size=10 * 1024 * 1024)

        # 初始化分片。
        upload_id = self.bucket.init_multipart_upload(key).upload_id
        parts = []

        # 逐个上传分片。
        with open(filename, 'rb') as fileobj:
            part_number = 1
            offset = 0
            while offset < total_size:
                num_to_upload = min(part_size, total_size - offset)
                # SizedFileAdapter(fileobj, size)方法会生成一个新的文件对象，重新计算起始追加位置。
                result = self.bucket.upload_part(key, upload_id, part_number,
                                            SizedFileAdapter(fileobj, num_to_upload))
                parts.append(PartInfo(part_number, result.etag))

                offset += num_to_upload
                part_number += 1

        # 完成分片上传。
        self.bucket.complete_multipart_upload(key, upload_id, parts)

        ## 验证分片上传。
        #with open(filename, 'rb') as fileobj:
        #    if not self.bucket.get_object(key).read() == fileobj.read():
        #        msg='上传' + filename + '出错，验证分片失败'
        #        print(msg)
        #        LogHelper.info(msg)


    def delete(self,obj_name):
        self.bucket.delete_object(obj_name)

    def percentage(self, consumed_bytes, total_bytes):
        """进度条回调函数，计算当前完成的百分比

        :param consumed_bytes: 已经上传/下载的数据量
        :param total_bytes: 总数据量
        """
        if total_bytes:
            rate = int(100 * (float(consumed_bytes) / float(total_bytes)))
            print('\r{0}% '.format(rate))
            sys.stdout.flush()

    def download(self, ossObject, loaclFile):
        oss2.resumable_download(self.bucket, ossObject, loaclFile,
                                store=oss2.ResumableDownloadStore(root=os.path.dirname(loaclFile)),
                                multiget_threshold=1 * 1024,
                                part_size=10 * 1024 * 1024,
                                num_threads=3,
                                progress_callback= self.percentage
                                )

if __name__ == '__main__':
    oss = OssHelper('LTAI9ziVIDTcSbW0','fsITkWUPxjfljcS3lEsdZSlGBlGhcN','http://oss-cn-hangzhou.aliyuncs.com','jfjun4test')
    print(oss.get_file_list('backup/'))
