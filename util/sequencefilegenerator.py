import os
import codecs
import snappy
import logging
import sys
import concurrent.futures

class SequenceFileGenerator():

    def __init__(self, inId=10000000, inLimit=100000):
        self.mHandle = None
        self.mID = inId
        self.mLimit = inLimit

    def resize(self, inData, inSize = 5):
        if inSize - len(inData) > 0 :
            while len(inData) != inSize:
                inData = inData + " "
        elif inSize - len(inData) < 0:
            inData = inData[5:]
        return inData

    def addTrailing(self, inData, inChar, inSize=10):
        if inSize - len(inData) > 0 :
            while len(inData) != inSize:
                inData = inChar + inData
        return inData
    
    def stripTrailingSpaces(self, inData):
        index = len(inData) - 1
        while inData[index] == " ":
            inData = inData.rstrip(" ")
            index = index - 1
        if len(inData) == 0:
            inData=""
        return inData
    
    def compress(self, inData):
        try:
            data = snappy.compress(inData)
            return data
        except:
            return None


    def decompress(self, inData):
        try:
            outData = snappy.decompress(inData)
            return outData
        except:
            return None
        

    def processFile(self, inFile, outFile):
        with open(inFile, 'rb') as handle:
            buffer = None
            try:
                data = handle.read()
                buffer = self.compress(data).encode('hex')
                # buffer = binascii.a2b_hex(handle.read())
            except Exception as ex:
                logging.error("Failed to sequence: " + str(ex) + ":" + inFile)\
            #buffer = base64.encodestring(handle.read())
        return inFile, buffer


    def processDir(self, inFolder, outFolder, isReverse=False):
        if isReverse:
            for root, dirs, fileList in os.walk(inFolder):
                for cfile in fileList:
                    with open(os.path.join(inFolder,cfile), 'rb') as handle:
                        for line in handle:
                            id = line[0:8]
                            ext = self.stripTrailingSpaces(line[8:13])
                            data = line[13:].rstrip("\n")
                            deData = self.decompress(data.decode('hex'))
                            self.mHandle = open(os.path.join(outFolder,id+ext),"wb")
                            self.mHandle.write(deData.rstrip("\n"))
                            self.mHandle.close()
        else:
            index = 0
            self.mHandle = open(os.path.join(outFolder, self.addTrailing(str(index),"0")),"wb")
            self.mIDhandle = open("IDMap.txt","w")
            for root, dirs, fileList in os.walk(inFolder):
                with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executer:
                    requests = {
                        executer.submit(self.processFile, os.path.join(inFolder,cfile),
                        os.path.join(outFolder,cfile)) :
                        cfile for cfile in fileList
                    }
                
                for futures in concurrent.futures.as_completed(requests):
                    pFile = ""
                    try:
                        if self.mID != 10000000 and self.mID % self.mLimit == 0 :
                            index = index + 1
                            self.mHandle.close()
                            self.mHandle = open(os.path.join(outFolder,self.addTrailing(str(index),"0")),"wb")
                            logging.info("Processed " + str(self.mID))
                        pFile, buffer = futures.result()
                        if buffer:
                            id = os.path.basename(pFile)
                            name, ext = os.path.splitext(id)
                            self.mIDhandle.write(pFile + "\t" + str(self.mID) + "\n")
                            self.mHandle.write(str(self.mID))
                            self.mHandle.write(self.resize(ext))
                            self.mHandle.write(buffer)
                            self.mHandle.write("\n")
                            self.mID = self.mID + 1
                    except Exception as ex:
                        logging.error("Failed to sequence: " + str(ex) + ":" + pFile)
                
                self.mIDhandle.close()
                self.mHandle.close()
