
from distutils.core import setup, Extension

module1 = Extension('polodb',
                    sources = ['polodb_ext.c'],
                    extra_objects=['../target/debug/libpolodb_clib.a'])

setup (name = 'polodb',
       version = '1.0',
       description = 'This is a demo package',
       ext_modules = [module1])
