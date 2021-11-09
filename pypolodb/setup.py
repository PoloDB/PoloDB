
import tempfile
import os
import urllib.request
import hashlib
from os import path
from setuptools import setup, Extension

user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
BUF_SIZE = 65536
LIB_VERSION = '0.10.4'

def get_platform_name():
       if os.name == 'nt':
              return 'win32'
       else:
              uname = os.uname()
              if uname.sysname == 'Darwin':
                     return 'darwin'
              else:
                     return 'linux'


def get_lib_name():
       platform = get_platform_name()
       if platform == 'win32':
              return 'polodb_clib.lib'
       else:
              return 'libpolodb_clib.a'

def get_download_url():
       platform_name = get_platform_name()
       lib_name = get_lib_name()
       return 'https://www.polodb.org/resources/' + LIB_VERSION + '/lib/' + platform_name + '/x64/' + lib_name

def gen_checksum_for(path):
       h = hashlib.sha256()
       with open(path, 'rb') as f:
              while True:
                     data = f.read(BUF_SIZE)
                     if not data:
                            break
                     h.update(data)

       return h.hexdigest()


def get_checksum_url(download_url):
       return download_url + '.SHA256'

def download_file(url, path):
       print('download file path: ' + url)
       headers = {'User-Agent':user_agent,}
       request = urllib.request.Request(url, None, headers) #The assembled request
       g = urllib.request.urlopen(request)
       with open(path, 'b+w') as f:
              f.write(g.read())

def get_text_from_url(url):
       headers = {'User-Agent':user_agent,}
       request = urllib.request.Request(url, None, headers) #The assembled request
       g = urllib.request.urlopen(request)
       with urllib.request.urlopen(request) as g:
              return g.read().decode('utf-8')

def download_lib():
       temp_root = tempfile.gettempdir()
       lib_root = path.join(temp_root, "polodb_lib", LIB_VERSION)
       os.makedirs(lib_root, exist_ok=True)
       file_path = path.join(lib_root, get_lib_name())

       lib_url = get_download_url()
       sha256_url = get_checksum_url(lib_url)

       if not path.exists(file_path):
              print('download lib to: ' + file_path)
              download_file(lib_url, file_path)

       remote_checksum_text = get_text_from_url(sha256_url)

       local_checksum_text = gen_checksum_for(file_path)
       return file_path

lib_path = download_lib()
# lib_path = '../target/release/libpolodb_clib.a'

extra_objects = [lib_path]

if get_platform_name() == 'win32':
       extra_objects.append('Userenv.lib')
       extra_objects.append('shell32.lib')
       extra_objects.append('Ws2_32.lib')
       extra_objects.append('Advapi32.lib')

module1 = Extension('polodb',
                    include_dirs=['include'],
                    sources = ['polodb_ext.c'],
                    extra_objects=extra_objects)

long_description = ''

setup (name = 'polodb',
       version = '2.0.0',
       description = 'PoloDB for Python',
       long_description=long_description,
       long_description_content_type="text/markdown",
       author = 'Vincent Chan',
       author_email = 'okcdz@diverse.space',
       license = 'MIT',
       install_requires=[
          'msgpack',
       ])
