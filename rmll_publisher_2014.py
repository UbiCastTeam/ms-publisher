#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import errno
import ftplib
import logging
import metacast
import os
import re
import requests
import sys
import zipfile

logger = logging.getLogger("rmll_publisher")

# FTP info
HOST = "videos-cdn.rmll.info"
PATH = "org/rmll/videos2014/orga/Amphi07/"
HTTP_PATTERN = "http://videos-cdn.rmll.info/videos2014/orga/Amphi07/%(media_id)s/%(filename)s"
DIRECTORY_PATTERN = re.compile(r"(?P<date>\d+)[-_](?P<spip_id>\d+)[-_](?P<title>.*)")

# Data
HIGH_QUALITY_PREFIX = "hd_ready"
LOW_QUALITY_PREFIX = "low"

# MediaServer info
MS_URL = "http://video.rmll.info/"
MS_API_KEY = "pCk1v-GJFV6-BAj3N-c5arz-QhRRM"
MS_CHANNEL = "2014 Montpellier"

# Settings
TMP_DIR = "/tmp/rmllpublisher"
CLEAN = True


class RmllPublisher(object):
    
    def __init__(self):
        object.__init__(self)
        # Connect to FTP
        self.ftp = ReconnectableFTP(HOST)
        # Anonymous login
        self.ftp.login()
    
    def publish(self):
        # Create temporary directory if needed
        try:
            os.makedirs(TMP_DIR)
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise e
        # Retrieve all the directories
        directories = self.ftp.nlst(PATH)
        for directory in directories:
            media_path = os.path.join(PATH, directory)
            self.process_media(media_path)
    
    def process_media(self, path):
        # Retrieve all the files
        files = self.ftp.nlst(path + "/")
        if not files:
            return
        # Get metadata if present
        match = DIRECTORY_PATTERN.match(os.path.basename(path))
        if not match:
            print >>sys.stderr, "Could not retrieve metadata of media %s" % os.path.basename(path)
            return
        metadata = match.groupdict()
        metadata["date"] = datetime.datetime.strptime(metadata["date"], "%Y%m%d").ctime()
        metadata["title"] = metadata["title"].replace("-", " ").replace("_", " ")
        # Retrieve all the existing resources
        low, high, others = self.get_video_resources(files)
        if not low and not high:
            print >>sys.stderr, "Media %s has no resources!" % os.path.basename(path)
            return # Skip this media
        # Build metacast object
        metacast_obj = self.build_metacast(os.path.basename(path), metadata, low, high, others)
        # Dump metacast object
        metacast_path = "%s.xml" % os.path.join(TMP_DIR, os.path.basename(path))
        metacast.xmlview.dump(metacast_obj, metacast_path)
        # Create ZIP file
        zipfile_path = "%s.zip" % os.path.join(TMP_DIR, os.path.basename(path))
        zipfile_obj = zipfile.ZipFile(zipfile_path, "w")
        try:
            zipfile_obj.write(metacast_path, "metadata.xml")
        finally:
            zipfile_obj.close()
        # Upload ZIP file to MediaServer
        result = self.upload_zip(zipfile_path)
        if not result:
            print >>sys.stderr, "Upload of media %s failed!" % os.path.basename(path)
        # Clean files
        if CLEAN:
            os.remove(metacast_path)
            os.remove(zipfile_path)
    
    def get_video_resources(self, files):
        low = list()
        high = list()
        others = list()
        for filename in files:
            basename, extension = os.path.splitext(filename)
            if basename.startswith(LOW_QUALITY_PREFIX):
                low.append(filename)
            elif basename.startswith(HIGH_QUALITY_PREFIX):
                high.append(filename)
            else:
                others.append(filename)
        return low, high, others
    
    def build_metacast(self, media_id, metadata, low, high, others):
        resources = list()
        for filename in low:
            resources.append(metacast.model.Resource(filename=HTTP_PATTERN % dict(media_id=media_id, filename=filename), quality='low', downloadable=True, displayable=True))
        for filename in high:
            resources.append(metacast.model.Resource(filename=HTTP_PATTERN % dict(media_id=media_id, filename=filename), quality='high', downloadable=True, displayable=True))
        for filename in others:
            resources.append(metacast.model.Resource(filename=HTTP_PATTERN % dict(media_id=media_id, filename=filename), quality='high', downloadable=True, displayable=not filename.startswith("original")))
        if metadata.get("speaker"):
            speakers = metacast.model.Speaker(", ".join([speaker.strip() for speaker in metadata["speaker"].split("|") if speaker.strip()]))
        else:
            speakers = None
        if metadata.get("license"):
            license = metacast.model.License(metadata["license"])
        else:
            license = None
        return metacast.model.MetaCast(type='dual', language=metadata.get("language", "fr"), title=metadata["title"], speaker=speakers, license=license, category=MS_CHANNEL, creation=metadata["date"], resources=resources)
    
    def upload_zip(self, zipfile_path):
        url = "%s/api/v2/medias/add/" % MS_URL.rstrip("/")
        zipfile_obj = open(zipfile_path, "r")
        req = requests.post(url, data=dict(api_key=MS_API_KEY), files=dict(file=zipfile_obj))
        zipfile_obj.close()
        return req.ok


class ReconnectableFTP(ftplib.FTP):
    
    def __init__(self, *args, **kwargs):
        self._last_cmd = None
        ftplib.FTP.__init__(self, *args, **kwargs)

    def login(self, user='', passwd='', acct=''):
        self.user = user
        self.passwd = passwd
        self.acct = acct
        return ftplib.FTP.login(self, user, passwd, acct)
    
    def reconnect(self):
        self.connect(self.host, self.port)
        self.login(self.user, self.passwd, self.acct)
    
    def sendcmd(self, cmd):
        cmd_log = cmd
        if cmd.startswith("PASS "):
            cmd_log = "PASS ******"
        logger.debug("sendcmd %s", cmd_log)
        last_cmd = self._last_cmd
        try:
            res = ftplib.FTP.sendcmd(self, cmd)
            logger.debug("%s succeeded", cmd_log)
            self._last_cmd = None
            return res
        except ftplib.error_temp, e:
            logger.debug("%s failed with error %s", cmd_log, e)
            self._last_cmd = cmd
            if cmd == last_cmd:
                raise e
            else:
                logger.warning("Command %s failed with error %s, try to reconnect", cmd_log, e)
                self.reconnect()
                return ftplib.FTP.sendcmd(self, cmd)

    def voidcmd(self, cmd):
        cmd_log = cmd
        if cmd.startswith("PASS "):
            cmd_log = "PASS ******"
        logger.debug("voidcmd %s", cmd)
        last_cmd = self._last_cmd
        try:
            res = ftplib.FTP.sendcmd(self, cmd)
            logger.debug("%s succeeded", cmd_log)
            self._last_cmd = None
            return res
        except ftplib.error_temp, e:
            logger.debug("%s failed with error %s", cmd_log, e)
            self._last_cmd = cmd
            if cmd == last_cmd:
                raise e
            else:
                logger.warning("Command %s failed with error %s, try to reconnect", cmd_log, e)
                self.reconnect()
                return ftplib.FTP.voidcmd(self, cmd)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    RmllPublisher().publish()

