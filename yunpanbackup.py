#!/usr/bin/env python
import hashlib
import argparse
from pymongo import MongoClient
from pymongo import ReturnDocument
from os.path import getsize
import time
import progressbar
Client = MongoClient()
db = Client['yunpanbackup']


backupstatus = {'initial': 0, 'inprogress': 1, 'completed': 2, 'failure': 3}
valuetostatus = {v: k for k, v in backupstatus.items()}

# progress bar
bar = progressbar.ProgressBar(maxval=100)


def read_in_chunks(filePath, chunk_size=64*1024):
    """
    Lazy function (generator) to read a file piece by piece.
    Default chunk size: 64K
    You can set your own chunk size
    """
    file_object = open(filePath, 'rb')
    while True:
        chunk_data = file_object.read(chunk_size)
        if not chunk_data:
            break
        yield chunk_data


def backupimage(file, backupname=None, datafile='backuprepository', chunksize=64*1024):
    try:
        handlecount = 0
        size = getsize(file)
        chunkcount = size//chunksize + 1

        if backupname is None:
            backupname = time.strftime('%Y%m%d%H%M%S')
        # write_file = open("D:\\test.bak", 'wb+')
        db[backupname].update_one({'_id': 0},  {'$setOnInsert': {'totalcount': chunkcount, 'backupfilesize': size,
                                                                 'writecount': 0, 'realWriteSize': 0, 'status':
                                                                     backupstatus['inprogress']}}, True)
        for chunk in read_in_chunks(file, chunksize):
            key = hashlib.sha1(chunk).hexdigest()
            backuptodatabase(backupname, handlecount+1, datafile, key, chunk)
            # write_file.write(chunk)
            handlecount += 1
            bar.update(int(handlecount/chunkcount*100))
        # print("handlecount={}, totalcount={}".format(handlecount, chunkcount))
        db[backupname].update_one({'_id': 0},  {'$set': {'status': backupstatus['completed']}}, True)
    except Exception as e:
        db[backupname].update_one({'_id': 0},  {'$set': {'status': backupstatus['failure']}}, True)
        print(e)


def __handleOneChunkOnRepository(backupcollection, index, datacollection):
    metadataitem = backupcollection.find_one({'_id': index})
    if metadataitem is None:
        raise RuntimeError("the metadataItem{} can't be found.".format(index))
    pass

    key = metadataitem['key']
    chunkitem = datacollection.find_one_and_update({'_id': key},
                                                   {'$inc': {'seq': -1}},
                         return_document=ReturnDocument.AFTER)
    if chunkitem is None:
        print("Warning, the {} can't be found at backuprepository".format(key))
        return
    if chunkitem['seq'] == 0:
        datacollection.delete_one({'_id': key})


def removeBackup(backupName, dataCollectionName='backuprepository'):
    # remove one backup
    try:
        if backupName not in db.list_collection_names():
            print("The input backup {} is not exist. Please check.")
            return
        backupCollection = db[backupName]
        datacollection = db[dataCollectionName]
        backupsummaryItem = backupCollection.find_one({'_id': 0})
        if backupsummaryItem is None:
            print("The backup summary item can't be fetched.")
        totalCount = backupsummaryItem['totalcount']

        for index in range(totalCount):
            __handleOneChunkOnRepository(backupCollection, index+1, datacollection)
            bar.update(int(index/totalCount*100))

        backupCollection.drop()

    except Exception as e:
        print("remove Backup failure! Exception is {}".format(e))


def fetchOneChunk(backupCollection, index, dataCollection):
    metadataitem = backupCollection.find_one({'_id':index})
    if metadataitem is None:
        raise RuntimeError("the metadataItem{} can't be found.".format(index))

    key = metadataitem['key']

    chunkitem = dataCollection.find_one({'_id': key})
    if chunkitem is None:
        raise RuntimeError("The {}th chunk item can't be found, key is {}".format(index, key))

    return chunkitem['content']



def restorefromdatabase(backupCollectionName, restoreFile, datacollectionName='backuprepository'):
    # restore the backuped image to the restoreFile.
    try:
        if backupCollectionName not in db.list_collection_names():
            print("The input backup {} is not exist. Please check.")
            return
        write_file = open(restoreFile, 'wb+')
        backupCollection = db[backupCollectionName]
        datacollection = db[datacollectionName]
        backupsummaryItem = backupCollection.find_one({'_id': 0})
        if backupsummaryItem is None:
            print("The backup summary item can't be fetched.")
        totalCount = backupsummaryItem['totalcount']
        chunk = None
        for i in range(totalCount):
            chunk = fetchOneChunk(backupCollection, i+1, datacollection)
            if chunk is None:
                raise RuntimeError("Chunk is None")
            write_file.write(chunk)
            bar.update(int((i+1)/totalCount*100))
        write_file.close()

    except Exception as e:
        print("Restore meet error {}".format(e))


def backuptodatabase(backupCollectionName, sequence, datacollectionName, key, chunk):
    backupCollection = db[backupCollectionName]
    datacollection = db[datacollectionName]
    # first store key/value to datacollection
    # if non-exist, insert and set refcount to 1.
    # if exist, update the refcount +1.
    updateditem = datacollection.find_one_and_update({'_id':key},
                                    {'$inc': {'seq': 1}},
                                    # projection={'seq': True, '_id': False, 'content': False},
                                    return_document=ReturnDocument.AFTER)

    if updateditem is None:
        item = {'_id': key, 'content': chunk, 'seq': 1}
        datacollection.insert_one(item)



    # add the sequnce number and key to backupCollection.
    # The sequence is from 1 start.
    item = {'_id': sequence, 'key': key}
    if sequence == 0:
        raise RuntimeError("the input sequence is 0")
    backupCollection.update_one({'_id': sequence}, {'$set': {'key': key}}, True)
    # update the 0th document which
    # is used to store the backup summary.
    if updateditem is None:
        updated0item = backupCollection.find_one_and_update({'_id': 0},
                                                     {'$inc': {'writecount': 1, 'realWriteSize': len(chunk)}},
                                                     return_document=False)


def __printhead():
    print("Name".center(14), "|", "status".center(10), "|", "size".center(32),
          "|", "totalcount".center(32), "|", "realwrite".center(32))
    print('-'*124)



def __printbackup(backupname):
    backupcollection = db[backupname]
    backupsummaryItem = backupcollection.find_one({'_id': 0})
    if backupsummaryItem is None:
        print(backupname.center(14), "|", "Broken!")
        return
    status = backupsummaryItem['status']
    print(backupname.center(14), "|", valuetostatus[status].center(10), "|",
          str(backupsummaryItem['backupfilesize']).center(32), "|",
          str(backupsummaryItem['totalcount']).center(32), "|",
          str(backupsummaryItem['realWriteSize']).center(32))


def listbackups():
    backupnames = [i for i in db.list_collection_names() if i != "backuprepository"]
    if len(backupnames) == 0:
        print("No backups found.")
        return
    __printhead()
    backupnames.sort()
    for backup in backupnames:
        __printbackup(backup)


def backupcommand(args):
    bar.start()
    backupimage(args.filepath, chunksize=args.chunksize)
    bar.finish()


def listcommand(args):
    listbackups()


def restorecommand(args):
    backup = args.backupname
    restorefile = args.filepath
    bar.start()
    restorefromdatabase(backup, restorefile)
    bar.finish()


def removecommand(args):
    bar.start()
    removeBackup(args.backupname)
    bar.finish()


def exportcommand(args):
    print("The metadata export function is not implemented.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='PROG')
    subparsers = parser.add_subparsers(help='sub-command help')
    # backup sub command
    parser_backup = subparsers.add_parser('backup', help='backup --help')
    parser_backup.add_argument("-f", "--filepath", type=str, help="The file path")
    parser_backup.add_argument("-c", "--chunksize", type=int, default=65536,
                               help="The input file will be seperated by this chunksize")
    parser_backup.set_defaults(func=backupcommand)

    # list sub command
    parser_list = subparsers.add_parser('list', help='list --help')
    parser_list.set_defaults(func=listcommand)

    # restore sub command
    parser_restore = subparsers.add_parser('restore', help='restore --help')
    parser_restore.add_argument("-b", "--backupname", type=str, help="The backup name")
    parser_restore.add_argument("-f", "--filepath", type=str, help="The restored full file path name")
    parser_restore.set_defaults(func=restorecommand)

    # remove sub command
    parser_remove = subparsers.add_parser('remove', help='remove --help')
    parser_remove.add_argument("-b", "--backupname", type=str, help="The backup needs be removed")
    parser_remove.set_defaults(func=removecommand)

    # export sub command
    parser_export = subparsers.add_parser('export', help='export --help')
    parser_export.add_argument("-b", "--backupname", type=str, help="The backup's metadata will be exported")
    parser_export.add_argument("-f", "--filename", type=str, help="The backup's metadata will be exported")
    parser_export.set_defaults(func=exportcommand)
    args = parser.parse_args()
    # execute the set function.
    args.func(args)
