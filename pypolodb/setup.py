
import tempfile
import os
import urllib.request
from os import path
from distutils.core import setup, Extension

LIB_VERSION = '0.1.0'

def get_download_url():
       return 'https://www.polodb.org/resources/' + LIB_VERSION + '/lib/darwin/x64/libpolodb_clib.a'

def download_lib():
       temp_root = tempfile.gettempdir()
       lib_root = path.join(temp_root, "polodb_lib")
       if not path.exists(lib_root):
              os.mkdir(lib_root)
       file_path = path.join(lib_root, 'libpolodb_clib.a')
       print('download lib to: ' + file_path)
       if path.exists(file_path):
              return None
       g = urllib.request.urlopen(get_download_url())
       with open(file_path, 'b+w') as f:
              f.write(g.read())

download_lib()

module1 = Extension('polodb',
                    sources = ['polodb_ext.c'],
                    extra_objects=['../target/debug/libpolodb_clib.a'])

setup (name = 'polodb',
       version = '0.1.0',
       description = 'This is a demo package',
       author = 'Vincent Chan',
       author_email = 'okcdz@diverse.space',
       license = 'MIT',
       ext_modules = [module1])
