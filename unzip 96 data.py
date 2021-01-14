#!/usr/bin/env python
# coding: utf-8


import tarfile
import os
import numpy as np
import glob
import shutil
def un_gz(file_name, dirs):
    t = tarfile.open(file_name)
    dirs = dirs + '/' + os.path.basename(file_name)
    os.mkdir(dirs)
    try:
        t.extractall(path=dirs)
    except:
        print('something wrong during decompress')
    da_te = os.path.basename(file_name).split('_')[1]
    readPath = dirs + '/md***' + da_te + '***'
    dataPathLs = np.array(glob.glob(readPath))
    readPath = dirs + '/***'
    dataPathLs1 = np.array(glob.glob(readPath))
    for i in list(set(dataPathLs1) - set(dataPathLs)):
        if os.path.exists(i):
            if os.path.isfile(i):
                os.remove(i)
            if os.path.isdir(i):
                shutil.rmtree(i) 
    print('finish ' + da_te)


readPath = '/mnt/DataTeamShared/for_Zhenyu/***'
dataPathLs = np.array(glob.glob(readPath))
for path in np.sort(dataPathLs)[81:92]:
    un_gz(path, '/mnt/e/96')

