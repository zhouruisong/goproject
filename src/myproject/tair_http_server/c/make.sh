PREFIX=/code/tair_http_server/c
gcc tair_clib.cc tair_cmap.cc tair_cvec.cc -I $PREFIX/include -I $PREFIX/include/tbnet/ -I $PREFIX/include/tbsys/ -L $PREFIX/lib -ltairclientapi -ltbsys -ltbnet -fPIC -shared -olibtair_clib.so
