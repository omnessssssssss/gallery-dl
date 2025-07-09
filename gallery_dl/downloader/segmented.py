# -*- coding: utf-8 -*-

# Copyright 2014-2025 Mike Fährmann
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.

"""Downloader module for segmented downloading"""

import threading
import os
import signal
from enum import Enum
from .. import util

# Global shutdown event for all segmented downloads
_global_shutdown_event = threading.Event()
_global_signal_handler_setup = False

def _setup_global_signal_handler():
    """Set up global signal handler for graceful shutdown"""
    global _global_signal_handler_setup
    if _global_signal_handler_setup:
        return
        
    def signal_handler(signum, frame):
        print("\nReceived interrupt signal, shutting down gracefully...")
        _global_shutdown_event.set()
    
    # Set up signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, signal_handler)
        except (OSError, ValueError):
            # Signal not available on this platform
            pass
    
    _global_signal_handler_setup = True

class SegmentStatus(Enum):
    PENDING = 1
    DOWNLOADING = 2
    COMPLETED = 3
    FAILED = 4

class Segment:
    """Represents a segment of a file to be downloaded."""
    def __init__(self, start, end, status=SegmentStatus.PENDING):
        self.start = start
        self.end = end
        self.size = end - start + 1
        self.status = status
        self.lock = threading.Lock()

    def __repr__(self):
        return f"<Segment start={self.start} end={self.end} size={self.size} status={self.status.name}>"

class DownloadManager:
    """Manages the segmented download of a file."""
    def __init__(self, http_downloader, url, pathfmt):
        self.http_downloader = http_downloader
        self.job = http_downloader.job
        self.url = url
        self.pathfmt = pathfmt
        self.log = self.job.get_logger("downloader.segmented")
        self.threads = []
        self.segments = []
        self.file_size = 0
        self.lock = threading.Lock()
        self.num_threads = self.job.extractor.config("downloader-threads", 4)
        self.out = self.job.out
        self.downloaded_bytes = 0
        
        _setup_global_signal_handler()

    def download(self):
        try:
            if _global_shutdown_event.is_set():
                return False
                
            # Ensure pathfmt is properly set up
            if not self.pathfmt.temppath:
                if self.http_downloader.part:
                    self.pathfmt.part_enable(self.http_downloader.partdir)
                if not self.pathfmt.temppath:
                    self.pathfmt.build_path()
            
            # Ensure realpath points to the final filename (without .part)
            if self.pathfmt.temppath.endswith('.part') and self.pathfmt.realpath.endswith('.part'):
                self.pathfmt.realpath = self.pathfmt.temppath[:-5]
                self.pathfmt.path = self.pathfmt.realpath
            
            self._get_file_size()
            self._create_initial_segments()

            for _ in range(self.num_threads):
                thread = threading.Thread(target=self._worker)
                self.threads.append(thread)
                thread.start()

            for thread in self.threads:
                thread.join()

            if _global_shutdown_event.is_set():
                self.log.warning("Download was interrupted")
                self._cleanup_partial_files()
                return False

            self._assemble_file()
            result = self.pathfmt.finalize()
            
            return True
        except Exception as e:
            self.log.error(f"Error during segmented download: {e}")
            return False

    def _cleanup_partial_files(self):
        """Clean up partial segment files when interrupted"""
        try:
            for segment in self.segments:
                if segment.status == SegmentStatus.COMPLETED:
                    part_path = f"{self.pathfmt.temppath}.part{segment.start}"
                    util.remove_file(part_path)
        except Exception as e:
            self.log.debug(f"Error during cleanup: {e}")

    def _get_file_size(self):
        response = self.http_downloader.session.head(self.url, timeout=self.http_downloader.timeout)
        response.raise_for_status()
        self.file_size = int(response.headers['Content-Length'])

    def _create_initial_segments(self):
        segment_size = self.file_size // self.num_threads
        for i in range(self.num_threads):
            start = i * segment_size
            end = start + segment_size - 1
            if i == self.num_threads - 1:
                end = self.file_size - 1
            self.segments.append(Segment(start, end))

    def _worker(self):
        while not _global_shutdown_event.is_set():
            segment = self._get_next_segment()
            if not segment:
                segment = self._split_largest_segment()
                if not segment:
                    break

            try:
                self._download_segment(segment)
            except Exception as e:
                if _global_shutdown_event.is_set():
                    break
                self.log.error(f"Failed to download segment {segment}: {e}")
                with segment.lock:
                    segment.status = SegmentStatus.FAILED

    def _download_segment(self, segment):
        with segment.lock:
            segment.status = SegmentStatus.DOWNLOADING

        headers = {'Range': f'bytes={segment.start}-{segment.end}'}
        response = self.http_downloader.session.get(self.url, headers=headers, stream=True, timeout=self.http_downloader.timeout)
        response.raise_for_status()

        os.makedirs(self.pathfmt.realdirectory, exist_ok=True)

        temp_part_path = os.path.join(self.pathfmt.realdirectory, f"{os.path.basename(self.pathfmt.temppath)}.part{segment.start}")
        with open(temp_part_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    with self.lock:
                        self.downloaded_bytes += len(chunk)
                        self.out.progress(self.file_size, self.downloaded_bytes, 0)
                
                if _global_shutdown_event.is_set():
                    response.close()
                    return

        with segment.lock:
            segment.status = SegmentStatus.COMPLETED

    def _get_next_segment(self):
        with self.lock:
            for segment in self.segments:
                with segment.lock:
                    if segment.status == SegmentStatus.PENDING:
                        return segment
            return None

    def _split_largest_segment(self):
        largest_segment = None
        with self.lock:
            for segment in self.segments:
                with segment.lock:
                    if segment.status == SegmentStatus.DOWNLOADING and (not largest_segment or segment.size > largest_segment.size):
                        largest_segment = segment

            if largest_segment and largest_segment.size > 1024 * 1024:
                with largest_segment.lock:
                    old_end = largest_segment.end
                    split_point = largest_segment.start + largest_segment.size // 2
                    largest_segment.end = split_point
                    largest_segment.size = largest_segment.end - largest_segment.start + 1

                    new_segment = Segment(split_point + 1, old_end)
                    self.segments.append(new_segment)
                    return new_segment
        return None

    def _assemble_file(self):
        os.makedirs(self.pathfmt.realdirectory, exist_ok=True)
        
        if not self.pathfmt.temppath:
            self.log.error("No temppath available for assembly")
            return
            
        with open(self.pathfmt.temppath, 'wb') as f:
            for segment in sorted(self.segments, key=lambda s: s.start):
                part_path = f"{self.pathfmt.temppath}.part{segment.start}"
                try:
                    with open(part_path, 'rb') as part_file:
                        data = part_file.read()
                        f.write(data)
                    util.remove_file(part_path)
                except FileNotFoundError:
                    self.log.warning(f"Segment file {part_path} not found during assembly")
                except Exception as e:
                    self.log.error(f"Error reading segment {part_path}: {e}")