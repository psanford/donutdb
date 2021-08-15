donutdb.so: donutloadable.c donutloadable.a
	gcc -g -fPIC -shared -o $@ $^

donutloadable.a: donut_loadable.go
	go build -tags SQLITE3VFS_LOADABLE_EXT -o $@ -buildmode=c-archive $^
	rm donutloadable.h

.PHONY: clean
clean:
	rm -f donutdb.so donutloadable.a
