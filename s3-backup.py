#!/usr/bin/env python
## s3-backup.py -- Backup a file or dir to S3 -*- Python -*-
## Time-stamp: "2008-09-25 16:56:00 ghoseb"

## Copyright (c) 2008, Baishampayan Ghose
## All rights reserved.

## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met:
##     * Redistributions of source code must retain the above copyright
##       notice, this list of conditions and the following disclaimer.
##     * Redistributions in binary form must reproduce the above copyright
##       notice, this list of conditions and the following disclaimer in the
##       documentation and/or other materials provided with the distribution.
##     * Neither the name of the <organization> nor the
##       names of its contributors may be used to endorse or promote products
##       derived from this software without specific prior written permission.

## THIS SOFTWARE IS PROVIDED BY BAISHAMPAYAN GHOSE ''AS IS'' AND ANY
## EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
## WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
## DISCLAIMED. IN NO EVENT SHALL BAISHAMPAYAN GHOSE BE LIABLE FOR ANY
## DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
## (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
## LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
## ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
## (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
## SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import os
import boto
from optparse import OptionParser, make_option

option_list = [
    make_option("-a", "--access-key", default=None, dest="AWS_ACCESS_KEY_ID", help="Your AWS Access key", metavar="AWS-ACCESS-KEY"),
    make_option("-s", "--secret-key", default=None, dest="AWS_SECRET_ACCESS_KEY", help="Your AWS Secret key", metavar="AWS-SECRET-KEY"),
    make_option("-b", "--bucket", default=None, dest="BUCKET", help="Destination bucket", metavar="BUCKET"),
    make_option("-c", "--create-bucket", action='store_true', default=False, dest="CREATE_BUCKET", help="Create the bucket if it doesn't exist"),
    make_option("-u", "--bucket-acl", default='private', dest="BUCKET_ACL", help="Access control for buckets", metavar="ACCESS-CONTROL"),
    make_option("-k", "--key-acl", default='public-read', dest="KEY_ACL", help="Access control for keys", metavar="ACCESS-CONTROL"),
    make_option("-f", "--file", default=None, dest="FILE", help="File to upload", metavar="FILE"),
    make_option("-n", "--key-name", default=None, dest="KEY_NAME", help="File name to use", metavar="KEY-NAME"),
    ]

usage = "Usage: %prog -a AWS_ACCESS_KEY -s AWS_SECRET_KEY -b BUCKET -f FILE"
parser = OptionParser(option_list=option_list, usage=usage)

def create_connection(access_key, secret_key):
    """Create a connection with Amazon AWS
    @param access_key - Amazon AWS access key
    @param secret_key - Amazon AWS secret key
    """
    return boto.connect_s3(access_key, secret_key)


def get_bucket(connection, bucket_name, create_bucket, acl='public-read'):
    """Get a bucket object by name
    @param connection - Connection object
    @param bucket_name - Bucket name to use
    @param create_bucket - If we should create a bucket if not found
    @param acl - Access control for bucket
    """
    buckets = conn.get_all_buckets()
    for bucket in buckets:
        if bucket.name == bucket_name:
            return bucket

    # We've been told to create a missing bucket
    if create_bucket:
        bucket = conn.create_bucket(bucket_name)
        bucket.set_acl(acl)
        return bucket

    # Nothing to do
    return False
    

def store_file(bucket, file, key=None, acl='public-read'):
    """Store a file in Amazon S3
    @param bucket - Bucket object
    @param file - File name
    @param key - Key name
    @param acl - Access control for key
    """
    # Sanity checks
    if not os.path.exists(file):
        raise TypeError('Please supply an existing file name to upload')
    elif not os.path.isfile(file):
        if os.path.isdir(file):
            raise TypeError('I can only upload files at the moment')
        raise TypeError('I can\'t upload the given file %s', file)

    # Set the file name as the key
    if not key:
        key = os.path.split(file)[1]
        
    keyobj = boto.s3.key.Key(bucket)
    keyobj.key = key
    keyobj.set_contents_from_filename(file)
    keyobj.set_acl(acl)    


def store_dir(bucket, dir, key=None, acl='public-read'):
    """Recursively store all files in a directory in Amazon S3
    @param bucket - Bucket object
    @param dir - Directory name
    @param key - Key name
    @param acl - Access control for key
    """
    if not os.path.exists(dir):
        raise TypeError('Please supply an existing directory name to upload')    
    listdir = os.listdir(dir)
    for file in listdir:
        if os.path.isfile(file):
            store_file(bucket, file, key, acl)
        elif os.path.isdir(file):
            store_dir(bucket, dir, key, acl)


def store(bucket, thunk, key=None, acl='public-read'):
    """Store a file or directory in Amazon S3
    @param bucket - Bucket object
    @param thunk - File or directory
    @param key - Key name
    @param acl - Access control for key
    """
    if os.path.isfile(thunk):
        store_file(bucket, thunk, key, acl)
    elif os.path.isdir(thunk):
        store_dir(bucket, thunk, key, acl)


if __name__ == '__main__':
    (options, args) = parser.parse_args()

    if (options.AWS_ACCESS_KEY_ID and options.AWS_SECRET_ACCESS_KEY and options.BUCKET and options.FILE):
        conn = create_connection(options.AWS_ACCESS_KEY_ID, options.AWS_SECRET_ACCESS_KEY)
        bucket = get_bucket(conn, options.BUCKET, options.CREATE_BUCKET, options.BUCKET_ACL)
        if bucket:
            store_file(bucket, options.FILE, options.KEY_NAME, options.KEY_ACL)
        else:
            print "Bucket %s doesn't exist and you didn't ask me to create one. Exiting..." % options.BUCKET
    else:
        parser.print_help()
