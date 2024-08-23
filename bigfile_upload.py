#!/usr/bin/python3

import requests
import uuid
import math
import re
import os
import sys
from urllib.parse import quote
from queue import Queue
import concurrent.futures


mypath='/path/to/folder/in/nextcloud'
file='my_file_to_upload'

block_size=50000000

user_name='' #Put your username here
password=''  #Put your password here
host=''      #Put your Nextcloud host here

url='https://'+host+'/remote.php/dav'
concurrent_uploads=7

url=url.rstrip('/')
mypath=mypath.strip('/')

uploads=url+'/uploads/'+user_name
path_url=url+'/files/'+user_name

# This is the maximum defined by Nextcloud
# Do not change!!!
max_chunks=10000

tmpdir='bigfileupload-'+str(uuid.uuid4())

def create_dir(url,dirlist):

    if len(dirlist)==0: return

    d=dirlist.pop()
    tmpurl=url+'/'+d

    r=requests.request('PROPFIND',tmpurl,auth=(user_name,password))
    if r.status_code==404:
        r=requests.request('MKCOL',tmpurl,auth=(user_name,password))
        if not ( r.status_code>=200 and r.status_code<300 ):
            print("Unable to create "+tmpurl,file=sys.stderr)
            print("Error code: "+str(r.status_code),file=sys.stderr)
            sys.exit(2)
  
    create_dir(tmpurl,dirlist)
    
def mkdir(url,dir):

    dirlist=dir.split('/')
    dirlist.reverse()

    create_dir(url,dirlist)

def splitfile(file,size):
    list=[]
    offset=0
    cnt=1
    while size>0:
        chunk=min(block_size,size)
        counter="{:05d}".format(cnt)
        list.append([counter,offset,chunk])
        offset+=chunk
        size-=chunk
        cnt+=1
    return list

def read_in_chunk(f,offset,chunk):
    f.seek(offset,0)
    data=f.read(chunk)
    yield data

def upload(filename,q,sz,upl,dst):
    headers={'OC-Total-Length':str(sz),'Destination':dst}
    auth=(user_name,password)
    f=open(filename,'rb')
    while not q.empty():
        filepart=q.get()
        number=filepart[0]
        offset=filepart[1]
        chunk=filepart[2]
        print("Uploading chunk "+number+" of "+str(chunk)+" bytes")
        r=requests.put(upl+'/'+number,auth=auth,headers=headers,data=read_in_chunk(f,offset,chunk))
        if not ( r.status_code>=200 and r.status_code<300 ):
            print("Unable to upload "+filename,file=sys.stderr)
            print("Error code: "+str(r.status_code),file=sys.stderr)
            q.task_done()
            sys.exit(2)
        else:
            print("Chunk "+number+" successfully uploaded")
        q.task_done()

    f.close()

def move_to_destination(sz,upl,dst):
    headers={'OC-Total-Length':str(sz),'Destination':dst}
    auth=(user_name,password)
    r=requests.request('MOVE',upl+'/.file',auth=(user_name,password), headers=headers,timeout=7200)
    if not ( r.status_code>=200 and r.status_code<300 ):
        print("Unable to move "+upl,file=sys.stderr)
        print("Error code: "+str(r.status_code),file=sys.stderr)
        print(r.content)
        sys.exit(2)

if __name__ == '__main__':

    mkdir(path_url,mypath)
    mkdir(uploads,tmpdir)

    filesize=os.path.getsize(file)
    if math.ceil(filesize/block_size)>max_chunks:
        print("Maximum number of chunks is exceeded.", file=sys.stderr)
        print("Choose a block size of at least "+str(math.ceil(filesize/max_chunks)), file=sys.stderr)
        sys.exit(3)

    partlist=splitfile(file,filesize)
    number_of_chunks=len(partlist)

    concurrent_uploads=min(concurrent_uploads,number_of_chunks)

    destination_url=path_url+'/'+quote(mypath+'/'+os.path.basename(file))
    upload_url=uploads+'/'+tmpdir

    q = Queue(maxsize = number_of_chunks)
    for f in partlist:
        q.put(f)

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_uploads) as executor:
        for i in range(concurrent_uploads):
            executor.submit(upload,file,q,filesize,upload_url,destination_url)

    q.join()
    print("Assembling the chunks and copy the data to :"+destination_url)
    move_to_destination(filesize,upload_url,destination_url)
