# This file is part of the MapProxy project.
# Copyright (C) 2011 Omniscale <http://omniscale.de>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from mapproxy.cache.base import TileCacheBase
from mapproxy.cache.s3 import S3Cache
from mapproxy.cache.file import FileCache

class HybridCache(TileCacheBase):
    def __init__(self, s3_base_path, file_ext, s3_directory_layout='tms',
                 lock_timeout=60.0, s3_bucket_name='mapproxy', s3_profile_name=None,
                 cache_dir='/tmp', directory_layout='tc',
                 link_single_color_images=False):
        self.s3Cache = S3Cache(
            base_path=s3_base_path,
            file_ext=file_ext,
            directory_layout=s3_directory_layout,
            bucket_name=s3_bucket_name,
            profile_name=s3_profile_name,
        )

        self.fileCache = FileCache(cache_dir=cache_dir,
            file_ext=file_ext,
            directory_layout=directory_layout,
            link_single_color_images=link_single_color_images,
            lock_timeout=lock_timeout)

        self.lock_cache_id = self.s3Cache.lock_cache_id


    def is_cached(self, tile):
        # return self.s3Cache.is_cached(tile)
        return self.fileCache.is_cached(tile) or self.s3Cache.is_cached(tile)

    def lock(self, tile):
        return self.s3Cache.lock(tile)

    def load_tile(self, tile, with_metadata=False):
        # return self.s3Cache.load_tile(tile, with_metadata=with_metadata)
        if self.fileCache.is_cached(tile):
            print "Loading from file cache"
            return self.fileCache.load_tile(tile, with_metadata=with_metadata)
        else:
            print "Loading from S3"
            return_val = self.s3Cache.load_tile(tile, with_metadata=with_metadata)
            return return_val

    def store_tile(self, tile, ignore_s3=False):
        if tile.stored:
            return
        self.s3Cache.store_tile(tile)
        tile.stored = False
        self.fileCache.store_tile(tile)


