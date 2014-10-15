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
import tempfile
import zipfile

logger = logging.getLogger("rmll_publisher")

# FTP info
HOST = "videos-cdn.rmll.info"
PATH = "org/rmll/videos2010/videos/"
HTTP_PATTERN = "http://videos-cdn.rmll.info/videos2010/videos/%(media_id)s/%(filename)s"

# Data
METADATA_FILE = "titre.sh"
THUMBNAIL_FILE = "titre.jpg"
HIGH_QUALITY_SUFFIX = "_big"
LOW_QUALITY_SUFFIX = "_small"

# Metadata
METADATA = dict(
    spip_id = ("CID", int),
    title = ("TIT", unicode),
    speaker = ("AUT", unicode), # separated by | if there are several ones
    date = ("DAT", int), # the day number in July 2010
    format = ("FMT", unicode), # ?
    # Optional
    start = ("START", int), # Number of seconds to skip at the beginning of the video
    duration = ("DURATION", int),
    language = ("LNG", unicode),
    license = ("LIC", unicode)
)

# MediaServer info
MS_URL = "http://video.rmll.info/"
MS_API_KEY = "pCk1v-GJFV6-BAj3N-c5arz-QhRRM"
MS_CHANNEL = "2010 Bordeaux"

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
        files = self.ftp.nlst(path)
        # Get metadata if present
        metadata = None
        if METADATA_FILE in files:
            metadata = self.get_metadata(os.path.join(path, METADATA_FILE))
        else:
            print >>sys.stderr, "Media %s has no metadata file!" % os.path.basename(path)
            return # Skip this media
        # Check if thumbnail is present
        thumbnail_path = None
        if THUMBNAIL_FILE in files:
            # Download thumbnail
            thumbnail_path = "%s.jpg" % os.path.join(TMP_DIR, os.path.basename(path))
            with open(thumbnail_path, "wb") as thumbnail_file:
                remote_path = os.path.join(path, THUMBNAIL_FILE)
                self.ftp.retrbinary('RETR %s' % remote_path, thumbnail_file.write)
        # Retrieve all the existing resources
        low, high = self.get_video_resources(files)
        if not low and not high:
            print >>sys.stderr, "Media %s has no resources!" % os.path.basename(path)
            return # Skip this media
        # Build metacast object
        metacast_obj = self.build_metacast(os.path.basename(path), metadata, low, high)
        # Dump metacast object
        metacast_path = "%s.xml" % os.path.join(TMP_DIR, os.path.basename(path))
        metacast.xmlview.dump(metacast_obj, metacast_path)
        # Create ZIP file
        zipfile_path = "%s.zip" % os.path.join(TMP_DIR, os.path.basename(path))
        zipfile_obj = zipfile.ZipFile(zipfile_path, "w")
        try:
            zipfile_obj.write(metacast_path, "metadata.xml")
            if thumbnail_path:
                zipfile_obj.write(thumbnail_path, "thumb.jpg")
        finally:
            zipfile_obj.close()
        # Upload ZIP file to MediaServer
        result = self.upload_zip(zipfile_path)
        if not result:
            print >>sys.stderr, "Upload of media %s failed!" % os.path.basename(path)
        # Clean files
        if CLEAN:
            os.remove(metacast_path)
            if thumbnail_path:
                os.remove(thumbnail_path)
            os.remove(zipfile_path)
        
    def get_metadata(self, file_path):
        metadata = dict()
        # Download file to a temporary file
        local_path = tempfile.mktemp()
        with open(local_path, "w+b") as local:
            self.ftp.retrbinary('RETR %s' % file_path, local.write)
            # Read metadata
            local.seek(0) # Go back to the beginning of the file
            content = local.read().decode("latin-1")
            for key, value in METADATA.iteritems():
                metadata[key] = self.read_value_from_file(content, value[0], value[1])
            metadata["date"] = datetime.datetime(2010, 7, metadata["date"]).ctime()
        return metadata
    
    def read_value_from_file(self, content, variable, cast_function):
        # Look for variable in file content
        regexp = re.compile(r'%s="([^"]*)"' % variable)
        match = regexp.search(content)
        if match:
            return cast_function(match.group(1))
        else:
            return None
    
    def get_video_resources(self, files):
        low = list()
        high = list()
        for filename in files:
            basename, extension = os.path.splitext(filename)
            if basename.endswith(LOW_QUALITY_SUFFIX):
                low.append(filename)
            elif basename.endswith(HIGH_QUALITY_SUFFIX):
                high.append(filename)
        return low, high
    
    def build_metacast(self, media_id, metadata, low, high):
        resources = list()
        for filename in low:
            resources.append(metacast.model.Resource(filename=HTTP_PATTERN % dict(media_id=media_id, filename=filename), quality='low', downloadable=True, displayable=True))
        for filename in high:
            resources.append(metacast.model.Resource(filename=HTTP_PATTERN % dict(media_id=media_id, filename=filename), quality='high', downloadable=True, displayable=True))
        speakers = metacast.model.Speaker(", ".join([speaker.strip() for speaker in metadata["speaker"].split("|") if speaker.strip()]))
        license = metacast.model.License(metadata["license"])
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

