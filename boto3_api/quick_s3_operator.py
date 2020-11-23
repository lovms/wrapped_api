from boto3.session import Session
import boto3
from botocore.exceptions import ClientError
import os

# http://man.hubwiz.com/docset/Boto3.docset/Contents/Resources/Documents/reference/services/s3.html#S3.Client.get_object -- Documents for Python boto3

class QuickS3Operator(object):
    def __init__(self):
        # client initialization
        self.access_key=""
        self.secret_key=""
        self.url="http://xxxxxx" #EndPoint
        self.s3_client = None
        self.session = None

    def startSession(self):
        if not self.session:
            self.session = Session(self.access_key, self.secret_key)

        if not self.s3_client:
            self.s3_client = self.session.client('s3', endpoint_url=self.url)

    def listBuckets(self):
        # go on operating
        print('\n'.join([bucket['Name'] for bucket in self.s3_client.list_buckets()['Buckets']]))

    def deleteEmptyBucket(self, bucketName):
        ''' Bucket containing no objects can be deleted directly'''
        if not self.s3_client:
            print('Error: s3 client is None')
            return
        try:
            resp = self.s3_client.delete_bucket(Bucket=bucketName)
        except ClientError as e:
            print(e.response['Error']['Code'])

    def deleteBucketWithObject(self, bucketName):
        ''' Delete objects before the bucket can be deleted'''
        self.deleteObjectsInBucket(bucketName)
        
        self.deleteEmptyBucket(bucketName)

    def listObjectsForBucket(self, bucketName, prefix=None):
        if not self.s3_client:
            print('Error: session or client is None')
            return
        resp = self.s3_client.list_objects(Bucket=bucketName, Prefix=prefix)
        objNameList = []
        for obj in resp['Contents']:
            #print(obj['Key'])
            objNameList.append(obj['Key'])
        return objNameList

    def deleteCertainObjectsInBucket(self, bucketName, prefix=None):
		'''
	    [reference]: https://www.itranslater.com/qa/details/2582543392097960960
		[notice] Both list_objects() and delete_objects() limit maximum number
		         of handled objects to 1000. This function should be operated 
			     several times until all objects with `prefix` are deleted.
		         Or this loop can be done in this function[<- TODO].
		'''
        if not self.s3_client:
            print('Error: session or client is None')
            return
        if prefix:
            resp = self.s3_client.list_objects(Bucket=bucketName, Prefix=prefix)
        else: 
            resp = self.s3_client.list_objects(Bucket=bucketName)
        delete_keys = {'Objects' : []}
        for obj in resp['Contents']:
            print(obj['Key'])
            delete_keys['Objects'].append({'Key': obj['Key']})
        self.s3_client.delete_objects(Bucket=bucketName, Delete=delete_keys)

    def deleteAllObjectsInBucket(self, bucketName):
        if not self.session:
            print('Error: session or client is None')
            return
        s3 = self.session.resource('s3', endpoint_url=self.url)
        b = s3.Bucket(bucketName)
        b.objects.all().delete()

    def traverseBucketObjects(self, bucketName):
        if not self.session:
            print('Error: session or client is None')
            return
        s3 = self.session.resource('s3', endpoint_url=self.url)
        b = s3.Bucket(bucketName)

        for obj in b.objects.all():
            body = obj.get()['Body']
            yield body

    def readFromBucketObject(self, bucketName, objectName):
        if not self.session:
            print('Error: session or client is None')
            return
        s3 = self.session.resource('s3', endpoint_url=self.url)
        obj = s3.Object(bucketName, objectName)
        for line in obj.get()['Body'].read().splitlines():
            #print(line.decode('utf-8'))
            yield line.decode('utf-8')

    def writeToBucketObject(self, bucketName, objectName, content):
        if not self.session:
            print('Error: session or client is None')
            return
        s3 = self.session.resource('s3', endpoint_url=self.url)
        obj = s3.Object(bucketName, objectName)
        obj.put(Body=content)
        #obj.put(Body="the first line\nthe second line\n")

        # Method 2
        #self.s3_client.put_object(Body="the first line\n", Bucket=bucketName, Key=objectName)
    def uploadFileToBucket(self, bucketName, fromName, toName):
        if not self.s3_client:
            print('Error: session or client is None')
            return False
        if os.path.exists(fromName) and os.path.isfile(fromName):
            resp = self.s3_client.put_object(Bucket=bucketName, Key=toName, Body=open(fromName, 'rb').read())
            print(resp)
            return True
        else:
            print("file {0} doesn't exist".format(fromName))
            return False
        return False
        


if __name__ == '__main__':
    qs = QuickS3Operator()

    qs.startSession()
    #qs.uploadFileToBucket('model.doc2vec.demo1', './tmp_d2v.model', '202006181805/dpp_d2v.model')
    #qs.uploadFileToBucket('model.doc2vec.demo1', './tmp_d2v.model.docvecs.vectors_docs.npy', 
    #                      '202006181805/dpp_d2v.model.docvecs.vectors_docs.npy')
    #qs.listBuckets()

    #qs.deleteBucketWithObject('corpus')
    #for key in qs.listObjectsForBucket('corpus.withseg'):
    #for key in qs.listObjectsForBucket('model', 'numerous_v2/default/billy_rerank_base0'):
    #    print(key)
    #    key.delete()
    #qs.deleteAllObjectsInBucket('corpus.origin')
    #count = 0
    #for content in qs.traverseBucketObjects('corpus.origin'):
        #print(count)
        #for i in content.read().splitlines():
        #    print(i.decode('utf-8'))

    # Test writing to object
    #qs.writeToBucketObject("tmp", "test_file_2", "this is a test")
    
    # Test reading from object
    #qs.readFromBucketObject('corpus.withseg', '202006181805/part-00000')
#    count = 0
#    for line in qs.readFromBucketObject('corpus.withseg', '202006181805/part-00350'):
#        count += 1
#        print(line)
#        if count > 10000:
#            break
#    print(count)
   
    qs.deleteCertainObjectsInBucket("model", 'numerous_v2/default/mix_sort_test1')
