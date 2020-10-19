
from distutils.core import setup, Extension

module1 = Extension('polodb',
                    sources = ['polodb_ext.c'],
                    extra_objects=['../target/debug/libpolodb_clib.a'])

setup (name = 'polodb',
       version = '0.1',
       description = 'This is a demo package',
       author = 'Vincent Chan',
       author_email = 'okcdz@diverse.space',
       license = 'MIT',
       ext_modules = [module1])
